"""
Data quality gate (Great Expectations) for one layer of the lakehouse.

Runs after each CDE stage in the DAG:

  generate -> dq bronze -> silver -> dq silver -> gold -> dq gold -> CAI scoring

Every expectation carries a severity:
  critical  the load is wrong (null keys, future dates, orphans, label leakage...):
            the job exits 1 after recording results, so Airflow stops the DAG and
            yesterday's call list stays in place.
  warning   worth a look (unexpected category, volume swing, roll rate outside its
            usual band): recorded, the pipeline continues.

All results, pass or fail, are appended to <prefix>_ref.dq_results (Iceberg),
one row per expectation, with the Iceberg snapshot id of the table checked, so
Impala / Hue / Cloudera Data Visualization can show quality per load and link
it to the exact data version.

Some checks span tables or compare with history (orphans, dedupe ratio, label
timing, volume change vs the previous load). Those are computed in Spark as a
small derived frame and validated with a standard expectation, so the results
table stays uniform.

Usage:
  spark-submit dq_check.py --layer bronze|silver|gold [--as-of YYYY-MM-DD]
                           [--db-prefix P] [--pipeline-run ID]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
os.environ.setdefault("TQDM_DISABLE", "1")

import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("dq_check")
for noisy in ("great_expectations", "py4j"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

DEFAULT_DB_PREFIX = "rsingh_collections_delinquency_prediction"
LAYERS = ("bronze", "silver", "gold")
CRITICAL, WARNING = "critical", "warning"
HORIZON_DAYS = 30
MAX_ORPHAN_RATE = 0.001
LABEL = "rolled_to_sma1_30d"

RESULT_SCHEMA = (
    "pipeline_run string, run_id string, run_ts timestamp, as_of date, layer string, "
    "table_name string, check_name string, expectation_type string, column_name string, "
    "severity string, success boolean, observed_value string, element_count bigint, "
    "unexpected_count bigint, unexpected_pct double, kwargs string, table_snapshot_id bigint, "
    "gx_version string"
)

# table -> (key columns, event date column)
BRONZE = {
    "loan_master": (["loan_id"], "disbursal_date"),
    "loan_instalments": (["loan_id", "instalment_no"], "due_date"),
    "nach_presentations": (["loan_id", "presentation_date", "presentation_seq"], "presentation_date"),
    "dialler_contacts": (["contact_id"], "contact_ts"),
    "casa_credits": (["customer_id", "credit_date"], "credit_date"),
    "bureau_snapshot": (["customer_id", "pull_date"], "pull_date"),
}
SILVER = {
    "loan": (["loan_id"], "bronze.loan_master"),
    "instalment": (["loan_id", "instalment_no"], "bronze.loan_instalments"),
    "nach_presentation": (["loan_id", "presentation_date", "presentation_seq"], "bronze.nach_presentations"),
    "collection_contact": (["contact_id"], None),
    "salary_credit": (["customer_id", "credit_date"], "bronze.casa_credits"),
    "bureau_score": (["customer_id", "pull_date"], "bronze.bureau_snapshot"),
}
GOLD_FEATURES = ["product_code", "dpd_now", "overdue_amount", "emi_amount", "overdue_to_emi", "months_on_book",
                 "max_dpd_12m", "nach_bounces_6m", "broken_ptp_3m", "contacts_reached_30d",
                 "salary_credit_last_30d", "salary_account_flag", "bureau_score"]


@dataclass
class Check:
    table: str          # table the check is about (as shown in results)
    frame: str          # key of the Spark frame validated (a table or a derived frame)
    name: str           # human-readable check name
    expectation: object
    severity: str


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--layer", choices=LAYERS, required=True)
    p.add_argument("--as-of", default=None, help="load date (default: max batch_as_of in bronze.loan_master)")
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    p.add_argument("--pipeline-run", default=None, help="groups the layers of one DAG run (Airflow run_id)")
    args, _ = p.parse_known_args(argv)
    return args


# ---------------------------------------------------------------- checks

def _days_after(col: str, as_of: date):
    return F.datediff(F.to_date(F.col(col)), F.lit(as_of))


def _ratio_frame(spark: SparkSession, value: float | None, column: str) -> DataFrame:
    return spark.createDataFrame([(float(value) if value is not None else None,)], f"{column} double")


def bronze_checks(spark: SparkSession, db: str, as_of: date, previous: dict) -> tuple[dict, list[Check]]:
    frames, checks = {}, []
    ref = spark.table(f"{db}_ref.product_map")
    products = [r.product_code for r in ref.select("product_code").collect()]
    for name, (keys, date_col) in BRONZE.items():
        df = (spark.table(f"{db}_bronze.{name}")
              .withColumn("days_after_as_of", _days_after(date_col, as_of))
              .withColumn("batch_days_from_as_of", _days_after("batch_as_of", as_of)))
        frames[name] = df
        checks += [
            Check(name, name, "row count", gxe.ExpectTableRowCountToBeBetween(min_value=1), CRITICAL),
            Check(name, name, "latest event not after the load date",
                  gxe.ExpectColumnMaxToBeBetween(column="days_after_as_of", max_value=0), CRITICAL),
            Check(name, name, "batch is for the load date",
                  gxe.ExpectColumnValuesToBeBetween(column="batch_days_from_as_of", min_value=0, max_value=0),
                  CRITICAL),
            Check(name, name, "re-sent duplicates below 1% (removed in silver)",
                  gxe.ExpectCompoundColumnsToBeUnique(column_list=keys, mostly=0.99) if len(keys) > 1
                  else gxe.ExpectColumnValuesToBeUnique(column=keys[0], mostly=0.99), WARNING),
        ]
        checks += [Check(name, name, f"{k} not null", gxe.ExpectColumnValuesToNotBeNull(column=k), CRITICAL)
                   for k in keys]
        prev = previous.get((name, "row count"))
        if prev:
            frames[f"{name}[volume]"] = _ratio_frame(spark, df.count() / prev, "rows_vs_previous_load")
            checks.append(Check(name, f"{name}[volume]", "volume within -20% / +25% of the previous load",
                                gxe.ExpectColumnValuesToBeBetween(column="rows_vs_previous_load",
                                                                  min_value=0.8, max_value=1.25), WARNING))

    master = frames["loan_master"]
    checks += [
        Check("loan_master", "loan_master", "loan_id unique", gxe.ExpectColumnValuesToBeUnique(column="loan_id"),
              CRITICAL),
        Check("loan_master", "loan_master", "product_code in ref.product_map",
              gxe.ExpectColumnValuesToBeInSet(column="product_code", value_set=products), CRITICAL),
        Check("loan_master", "loan_master", "repayment_mode is NACH or SELF_PAY",
              gxe.ExpectColumnValuesToBeInSet(column="repayment_mode", value_set=["NACH", "SELF_PAY"]), WARNING),
        Check("loan_master", "loan_master", "interest rate 6-48%",
              gxe.ExpectColumnValuesToBeBetween(column="interest_rate", min_value=6, max_value=48), WARNING),
        Check("loan_master", "loan_master", "EMI positive",
              gxe.ExpectColumnValuesToBeBetween(column="emi_amount", min_value=0.01), CRITICAL),
    ]
    frames["loan_master[term]"] = master.where(F.col("product_code") != 3)   # 3 = credit card, revolving
    checks.append(Check("loan_master", "loan_master[term]", "term loans: tenor 1-360 months",
                        gxe.ExpectColumnValuesToBeBetween(column="tenor_months", min_value=1, max_value=360),
                        WARNING))

    ids = master.select("loan_id").distinct().withColumn("known", F.lit(1))
    for child in ("loan_instalments", "nach_presentations", "dialler_contacts"):
        frames[f"{child}[orphans]"] = (frames[child].select("loan_id").join(ids, "loan_id", "left")
                                       .select(F.when(F.col("known").isNull(), 1).otherwise(0).alias("orphan")))
        checks.append(Check(child, f"{child}[orphans]", "rows for unknown loans below 0.1%",
                            gxe.ExpectColumnMeanToBeBetween(column="orphan", max_value=MAX_ORPHAN_RATE), CRITICAL))

    inst = frames["loan_instalments"]
    frames["loan_instalments[paid]"] = (inst.where(F.col("paid_date").isNotNull())
                                        .withColumn("paid_minus_due_days", F.datediff("paid_date", "due_date")))
    checks += [
        Check("loan_instalments", "loan_instalments", "EMI positive",
              gxe.ExpectColumnValuesToBeBetween(column="emi_amount", min_value=0.01, mostly=0.999), CRITICAL),
        Check("loan_instalments", "loan_instalments[paid]", "paid at most 5 days before due",
              gxe.ExpectColumnValuesToBeBetween(column="paid_minus_due_days", min_value=-5, mostly=0.999), CRITICAL),
        Check("loan_instalments", "loan_instalments[paid]", "payment channel known",
              gxe.ExpectColumnValuesToBeInSet(column="payment_channel",
                                              value_set=["UPI", "NACH", "NETBANKING", "BRANCH", "CASH"]), WARNING),
    ]
    nach = frames["nach_presentations"]
    frames["nach_presentations[bounced]"] = nach.where(F.col("status") == "BOUNCED")
    frames["nach_presentations[success]"] = nach.where(F.col("status") == "SUCCESS")
    checks += [
        Check("nach_presentations", "nach_presentations", "status is SUCCESS or BOUNCED",
              gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["SUCCESS", "BOUNCED"]), CRITICAL),
        Check("nach_presentations", "nach_presentations[bounced]", "bounces carry a reason",
              gxe.ExpectColumnValuesToNotBeNull(column="bounce_reason"), WARNING),
        Check("nach_presentations", "nach_presentations[success]", "successful presentations carry no reason",
              gxe.ExpectColumnValuesToBeNull(column="bounce_reason"), WARNING),
    ]
    checks += [
        Check("dialler_contacts", "dialler_contacts", "channel known",
              gxe.ExpectColumnValuesToBeInSet(column="channel", value_set=["CALL", "IVR", "FIELD"]), WARNING),
        Check("dialler_contacts", "dialler_contacts", "outcome known",
              gxe.ExpectColumnValuesToBeInSet(column="outcome",
                                              value_set=["REACHED", "NO_ANSWER", "SWITCHED_OFF", "WRONG_NUMBER"]),
              WARNING),
        Check("casa_credits", "casa_credits", "credit amount positive",
              gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0.01), CRITICAL),
        Check("bureau_snapshot", "bureau_snapshot", "bureau score 300-900",
              gxe.ExpectColumnValuesToBeBetween(column="bureau_score", min_value=300, max_value=900, mostly=0.999),
              CRITICAL),
        Check("bureau_snapshot", "bureau_snapshot", "bureau score present",
              gxe.ExpectColumnValuesToNotBeNull(column="bureau_score", mostly=0.999), CRITICAL),
    ]
    return frames, checks


def silver_checks(spark: SparkSession, db: str, as_of: date, previous: dict) -> tuple[dict, list[Check]]:
    frames, checks = {}, []
    for name, (keys, source) in SILVER.items():
        df = spark.table(f"{db}_silver.{name}")
        frames[name] = df
        checks.append(Check(name, name, "row count", gxe.ExpectTableRowCountToBeBetween(min_value=1), CRITICAL))
        checks.append(Check(name, name, "key unique after dedupe",
                            gxe.ExpectCompoundColumnsToBeUnique(column_list=keys) if len(keys) > 1
                            else gxe.ExpectColumnValuesToBeUnique(column=keys[0]), CRITICAL))
        if source:
            ratio = df.count() / max(spark.table(f"{db}_{source}").count(), 1)
            frames[f"{name}[dedupe]"] = _ratio_frame(spark, ratio, "silver_to_bronze_rows")
            checks.append(Check(name, f"{name}[dedupe]", "dedupe removed less than 2% of bronze rows",
                                gxe.ExpectColumnValuesToBeBetween(column="silver_to_bronze_rows",
                                                                  min_value=0.98, max_value=1.0), CRITICAL))

    ids = frames["loan"].select("loan_id").distinct().withColumn("known", F.lit(1))
    for child in ("instalment", "nach_presentation", "collection_contact"):
        frames[f"{child}[orphans]"] = (frames[child].select("loan_id").join(ids, "loan_id", "left")
                                       .select(F.when(F.col("known").isNull(), 1).otherwise(0).alias("orphan")))
        checks.append(Check(child, f"{child}[orphans]", "rows for unknown loans below 0.1%",
                            gxe.ExpectColumnMeanToBeBetween(column="orphan", max_value=MAX_ORPHAN_RATE), CRITICAL))

    contact = frames["collection_contact"]
    frames["collection_contact[no ptp]"] = contact.where(F.col("ptp_date").isNull())
    frames["collection_contact[ptp due]"] = contact.where(F.col("ptp_date") <= F.lit(as_of))
    checks += [
        Check("collection_contact", "collection_contact", "source is DIALLER or COLLECTOR_APP",
              gxe.ExpectColumnValuesToBeInSet(column="source", value_set=["DIALLER", "COLLECTOR_APP"]), CRITICAL),
        Check("collection_contact", "collection_contact[no ptp]", "ptp_kept only set where a PTP exists",
              gxe.ExpectColumnValuesToBeNull(column="ptp_kept"), CRITICAL),
        Check("collection_contact", "collection_contact[ptp due]", "PTP kept/broken known once the PTP date passed",
              gxe.ExpectColumnValuesToNotBeNull(column="ptp_kept", mostly=0.99), WARNING),
        Check("instalment", "instalment", "paid amount not negative",
              gxe.ExpectColumnValuesToBeBetween(column="paid_amount", min_value=0), CRITICAL),
        Check("loan", "loan", "salary_account_flag is 0/1",
              gxe.ExpectColumnValuesToBeInSet(column="salary_account_flag", value_set=[0, 1]), CRITICAL),
    ]
    return frames, checks


def gold_checks(spark: SparkSession, db: str, as_of: date, previous: dict) -> tuple[dict, list[Check]]:
    t = "collections_features"
    df = spark.table(f"{db}_gold.{t}").withColumn("days_after_as_of", _days_after("snapshot_date", as_of))
    matured = F.date_add(F.col("snapshot_date"), HORIZON_DAYS) <= F.lit(as_of)
    frames = {
        t: df,
        f"{t}[today]": df.where(F.col("snapshot_date") == F.lit(as_of)),
        f"{t}[matured]": df.where(matured),
        f"{t}[maturing]": df.where(~matured),
    }
    checks = [
        Check(t, t, "row count", gxe.ExpectTableRowCountToBeBetween(min_value=1), CRITICAL),
        Check(t, t, "one row per loan per snapshot",
              gxe.ExpectCompoundColumnsToBeUnique(column_list=["loan_id", "snapshot_date"]), CRITICAL),
        Check(t, t, "SMA-0 only: DPD 1-30", gxe.ExpectColumnValuesToBeBetween(column="dpd_now", min_value=1,
                                                                             max_value=30), CRITICAL),
        Check(t, t, "label is 0/1", gxe.ExpectColumnValuesToBeInSet(column=LABEL, value_set=[0, 1]), CRITICAL),
        Check(t, t, "overdue within 0-3 EMIs",
              gxe.ExpectColumnValuesToBeBetween(column="overdue_to_emi", min_value=0, max_value=3, mostly=0.999),
              WARNING),
        Check(t, t, "bureau score 300-900",
              gxe.ExpectColumnValuesToBeBetween(column="bureau_score", min_value=300, max_value=900), WARNING),
        Check(t, t, "snapshot not after the load date",
              gxe.ExpectColumnMaxToBeBetween(column="days_after_as_of", max_value=0), CRITICAL),
        Check(t, f"{t}[today]", "today's SMA-0 book present",
              gxe.ExpectTableRowCountToBeBetween(min_value=1), CRITICAL),
        Check(t, f"{t}[matured]", "label known once 30 days have passed",
              gxe.ExpectColumnValuesToNotBeNull(column=LABEL), CRITICAL),
        Check(t, f"{t}[maturing]", "no label before 30 days have passed (leakage)",
              gxe.ExpectColumnValuesToBeNull(column=LABEL), CRITICAL),
    ]
    critical_features = {"product_code", "dpd_now", "overdue_amount", "emi_amount"}
    checks += [Check(t, t, f"{c} not null", gxe.ExpectColumnValuesToNotBeNull(column=c),
                     CRITICAL if c in critical_features else WARNING) for c in GOLD_FEATURES]

    latest = (frames[f"{t}[matured]"].groupBy("snapshot_date").agg(F.avg(LABEL).alias("roll_rate"))
              .orderBy(F.col("snapshot_date").desc()).limit(1).collect())
    if latest:
        frames[f"{t}[roll rate]"] = _ratio_frame(spark, latest[0]["roll_rate"], "latest_labelled_roll_rate")
        checks.append(Check(t, f"{t}[roll rate]", "latest labelled roll rate within 10-40%",
                            gxe.ExpectColumnValuesToBeBetween(column="latest_labelled_roll_rate",
                                                              min_value=0.10, max_value=0.40), WARNING))
    prev = previous.get((t, "row count"))
    if prev:
        frames[f"{t}[volume]"] = _ratio_frame(spark, df.count() / prev, "rows_vs_previous_load")
        checks.append(Check(t, f"{t}[volume]", "rows within -5% / +10% of the previous load",
                            gxe.ExpectColumnValuesToBeBetween(column="rows_vs_previous_load",
                                                              min_value=0.95, max_value=1.10), WARNING))
    return frames, checks


LAYER_CHECKS = {"bronze": bronze_checks, "silver": silver_checks, "gold": gold_checks}


# ---------------------------------------------------------------- running

def _exception(result) -> str | None:
    info = result.exception_info or {}
    if isinstance(info, dict) and "raised_exception" not in info:   # keyed by metric id
        info = next((v for v in info.values() if isinstance(v, dict) and v.get("raised_exception")), {})
    return info.get("exception_message") if isinstance(info, dict) and info.get("raised_exception") else None


def _json(value) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value, default=str)


def run_checks(frames: dict[str, DataFrame], checks: list[Check]) -> list[dict]:
    """Validate each frame with its expectations; one result dict per check."""
    ctx = gx.get_context(mode="ephemeral")
    source = ctx.data_sources.add_spark("spark")
    out = []
    for frame in dict.fromkeys(c.frame for c in checks):
        mine = [c for c in checks if c.frame == frame]
        asset = source.add_dataframe_asset(f"frame_{len(out)}_{abs(hash(frame))}")
        batch = asset.add_batch_definition_whole_dataframe("all").get_batch(
            batch_parameters={"dataframe": frames[frame]})
        for c in mine:
            c.expectation.meta = {"check": c.name, "severity": c.severity, "table": c.table}
        suite = ctx.suites.add(gx.ExpectationSuite(name=f"suite_{len(out)}_{abs(hash(frame))}",
                                                   expectations=[c.expectation for c in mine]))
        res = batch.validate(suite, result_format="SUMMARY")
        by_name = {c.name: c for c in mine}
        for r in res.results:
            cfg = r.expectation_config
            c = by_name[cfg.meta["check"]]
            res_dict = r.result or {}
            error = _exception(r)
            kwargs = {k: v for k, v in cfg.kwargs.items() if k not in ("batch_id",)}
            out.append({
                "table_name": c.table, "check_name": c.name, "expectation_type": cfg.type,
                "column_name": kwargs.get("column") or ",".join(kwargs.get("column_list", []) or []) or None,
                "severity": c.severity, "success": bool(r.success) and error is None,
                "observed_value": f"ERROR: {error}"[:500] if error else _json(res_dict.get("observed_value")),
                "element_count": res_dict.get("element_count"),
                "unexpected_count": res_dict.get("unexpected_count"),
                "unexpected_pct": res_dict.get("unexpected_percent"),
                "kwargs": _json(kwargs),
            })
    return out


def _snapshot_id(spark: SparkSession, table: str) -> int | None:
    try:
        row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        return int(row[0]) if row else None
    except Exception:  # lineage is best effort
        return None


def _previous(spark: SparkSession, results_table: str, layer: str, as_of: date) -> dict:
    """Last recorded row counts per table from earlier loads, for volume checks."""
    if not spark.catalog.tableExists(results_table):
        return {}
    rows = spark.sql(f"""
        SELECT table_name, check_name, observed_value FROM (
          SELECT table_name, check_name, observed_value,
                 ROW_NUMBER() OVER (PARTITION BY table_name, check_name ORDER BY as_of DESC, run_ts DESC) rn
          FROM {results_table}
          WHERE layer = '{layer}' AND check_name = 'row count' AND as_of < DATE '{as_of.isoformat()}'
            AND observed_value IS NOT NULL AND observed_value NOT LIKE 'ERROR%')
        WHERE rn = 1""").collect()
    return {(r.table_name, r.check_name): float(r.observed_value) for r in rows}


def write_results(spark: SparkSession, results_table: str, rows: list[dict]) -> None:
    df = spark.createDataFrame(rows, RESULT_SCHEMA)
    if spark.catalog.tableExists(results_table):
        df.writeTo(results_table).append()
    else:
        (df.writeTo(results_table).using("iceberg").tableProperty("format-version", "2")
         .partitionedBy(F.months("as_of")).create())


def resolve_as_of(spark: SparkSession, db: str, as_of: str | None) -> date:
    if as_of:
        return datetime.strptime(as_of, "%Y-%m-%d").date()
    return spark.table(f"{db}_bronze.loan_master").agg(F.max("batch_as_of")).first()[0]


def run(spark: SparkSession, args: argparse.Namespace) -> list[dict]:
    db, layer = args.db_prefix, args.layer
    as_of = resolve_as_of(spark, db, args.as_of)
    results_table = f"{db}_ref.dq_results"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}_ref")
    frames, checks = LAYER_CHECKS[layer](spark, db, as_of, _previous(spark, results_table, layer, as_of))
    logger.info("%s layer as of %s: %d checks on %d frames", layer, as_of, len(checks), len(frames))

    results = run_checks(frames, checks)
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = f"{layer}-{as_of:%Y%m%d}-{uuid.uuid4().hex[:8]}"
    snapshot = {name: _snapshot_id(spark, f"{db}_{layer}.{name}") for name in {r["table_name"] for r in results}}
    for r in results:
        r.update(pipeline_run=args.pipeline_run or f"manual-{run_ts:%Y%m%dT%H%M%S}", run_id=run_id, run_ts=run_ts,
                 as_of=as_of, layer=layer, table_snapshot_id=snapshot.get(r["table_name"]),
                 gx_version=gx.__version__)
    write_results(spark, results_table, results)

    failed = [r for r in results if not r["success"]]
    for r in results:
        logger.info("%-4s %-8s %-20s %-55s %s", "PASS" if r["success"] else "FAIL", r["severity"], r["table_name"],
                    r["check_name"], r["observed_value"] if r["observed_value"] is not None
                    else f"{r['unexpected_count']} unexpected of {r['element_count']}")
    logger.info("%s: %d checks, %d failed (%d critical); results in %s (run %s)", layer, len(results), len(failed),
                sum(r["severity"] == CRITICAL for r in failed), results_table, run_id)
    return results


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName(f"coll-dq-{args.layer}").getOrCreate()
    try:
        results = run(spark, args)
    finally:
        if own:
            spark.stop()
    critical = [r for r in results if not r["success"] and r["severity"] == CRITICAL]
    if critical:
        for r in critical:
            logger.error("DATA QUALITY GATE FAILED: %s.%s: %s", r["table_name"], r["check_name"], r["observed_value"])
        sys.exit(1)
    logger.info("%s data quality gate passed", args.layer)


if __name__ == "__main__":
    main()
