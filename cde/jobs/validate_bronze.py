"""
Stage 2 - Validate (bronze gate)

Data-quality gate on the loan, NACH, dialler, CASA and bureau extracts. Exits
non-zero on a hard failure so the Airflow DAG stops before anything reaches
silver/gold. Duplicate re-sent records are expected and only logged: silver
removes them.

Hard checks:
  * every extract is non-empty and has no null keys
  * loan_id is unique in loan_master; every product_code is in ref.product_map
  * instalments, NACH presentations and contacts belong to a known loan (< 0.1% orphans)
  * EMI amounts positive; paid dates not more than 5 days before the due date
  * bureau scores within 300-900 (< 0.1% outside)
  * no event dated after the load's as-of date

Reads:
  <prefix>_bronze.{loan_master, loan_instalments, nach_presentations,
                   dialler_contacts, casa_credits, bureau_snapshot}
  <prefix>_ref.product_map

Usage:
  spark-submit validate_bronze.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_collections_delinquency_prediction"
MAX_BAD_RATE = 0.001

# table -> (key columns, event date column)
EXTRACTS = {
    "loan_master": (["loan_id"], "disbursal_date"),
    "loan_instalments": (["loan_id", "instalment_no"], "due_date"),
    "nach_presentations": (["loan_id", "presentation_date", "presentation_seq"], "presentation_date"),
    "dialler_contacts": (["contact_id"], "contact_ts"),
    "casa_credits": (["customer_id", "credit_date"], "credit_date"),
    "bureau_snapshot": (["customer_id", "pull_date"], "pull_date"),
}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def _rate(bad: int, rows: int) -> float:
    return bad / rows if rows else 0.0


def validate(spark: SparkSession, db_prefix: str) -> list[str]:
    bronze = f"{db_prefix}_bronze"
    errors = []
    tables = {name: spark.table(f"{bronze}.{name}") for name in EXTRACTS}

    as_of = tables["loan_master"].agg(F.max("batch_as_of")).first()[0]
    logger.info("load as of %s", as_of)
    for name, (keys, date_col) in EXTRACTS.items():
        df = tables[name]
        null_key = F.lit(False)
        for k in keys:
            null_key = null_key | F.col(k).isNull()
        s = df.agg(F.count("*").alias("rows"),
                   F.sum(F.when(null_key, 1).otherwise(0)).alias("null_keys"),
                   F.sum(F.when(F.to_date(date_col) > F.lit(as_of), 1).otherwise(0)).alias("future"),
                   F.min(date_col).alias("first"), F.max(date_col).alias("last")).first()
        logger.info("%-20s rows=%-10s %s -> %s", name, s["rows"], s["first"], s["last"])
        if not s["rows"]:
            errors.append(f"{name} is empty")
            continue
        if s["null_keys"]:
            errors.append(f"{name}: {s['null_keys']} rows with a null key ({', '.join(keys)})")
        if s["future"]:
            errors.append(f"{name}: {s['future']} rows dated after the as-of date {as_of}")
        dupes = df.groupBy(*keys).count().where("count > 1").count()
        if dupes:
            logger.info("%-20s duplicate keys: %d (removed in silver)", name, dupes)

    master = tables["loan_master"]
    n_loans, n_ids = master.agg(F.count("*"), F.countDistinct("loan_id")).first()
    if n_loans != n_ids:
        errors.append(f"loan_master: {n_loans - n_ids} duplicate loan_id")
    unknown = (master.select("product_code").distinct()
               .join(spark.table(f"{db_prefix}_ref.product_map"), "product_code", "left_anti").collect())
    if unknown:
        errors.append("unknown product_code: " + ", ".join(str(r.product_code) for r in unknown))

    for name in ("loan_instalments", "nach_presentations", "dialler_contacts"):
        df = tables[name]
        orphans = df.join(master.select("loan_id"), "loan_id", "left_anti").count()
        rate = _rate(orphans, df.count())
        if rate > MAX_BAD_RATE:
            errors.append(f"{name}: {rate:.3%} rows for unknown loans (limit {MAX_BAD_RATE:.1%})")

    inst = tables["loan_instalments"]
    s = inst.agg(F.count("*").alias("rows"),
                 F.sum(F.when(F.col("emi_amount") <= 0, 1).otherwise(0)).alias("bad_emi"),
                 F.sum(F.when(F.datediff("paid_date", "due_date") < -5, 1).otherwise(0)).alias("early"),
                 F.sum(F.when(F.col("paid_date").isNull(), 1).otherwise(0)).alias("unpaid")).first()
    logger.info("instalments unpaid as of load: %d of %d", s["unpaid"], s["rows"])
    for col, label in (("bad_emi", "non-positive EMI"), ("early", "paid > 5 days before due")):
        rate = _rate(s[col], s["rows"])
        if rate > MAX_BAD_RATE:
            errors.append(f"loan_instalments: {label} in {rate:.3%} of rows")

    bureau = tables["bureau_snapshot"]
    bad = bureau.where(~F.col("bureau_score").between(300, 900) | F.col("bureau_score").isNull()).count()
    rate = _rate(bad, bureau.count())
    if rate > MAX_BAD_RATE:
        errors.append(f"bureau_snapshot: {rate:.3%} scores outside 300-900")
    return errors


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("coll-validate-bronze").getOrCreate()
    try:
        errors = validate(spark, args.db_prefix)
    finally:
        if own:
            spark.stop()
    if errors:
        for e in errors:
            logger.error("VALIDATION FAILED: %s", e)
        sys.exit(1)
    logger.info("Bronze validation passed")


if __name__ == "__main__":
    main()
