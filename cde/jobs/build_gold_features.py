"""
Stage 4 - Gold (collections features, one row per SMA-0 loan per snapshot)

Snapshot dates are every Friday from FIRST_SNAPSHOT up to the load's as-of
date, plus the as-of date itself (today's book, the rows to score). A loan is
in a snapshot when it is in SMA-0 that day: 1-30 days past due, counted from
its oldest unpaid due date. Everything is encoded as numbers here, so the
model gets a clean numeric table.

Label: rolled_to_sma1_30d = 1 if the loan is 31+ DPD on any day in
(snapshot_date, snapshot_date + 30]. It is NULL until 30 days have passed, so
today's rows and the last four weeks stay unlabelled; each daily load fills in
the labels that have matured.

Writes via MERGE INTO once the table exists: rows whose values changed
(mostly labels maturing) are updated, new snapshots inserted, and rows no
longer produced (a mid-week as-of row once the next load runs) deleted. Each
load is one Iceberg snapshot, which the CAI job records for lineage.

Reads:
  <prefix>_silver.{loan, instalment, nach_presentation, collection_contact,
                   salary_credit, bureau_score}
Writes:
  <prefix>_gold.collections_features

Usage:
  spark-submit build_gold_features.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_collections_delinquency_prediction"
FIRST_SNAPSHOT = date(2025, 4, 4)   # a Friday, 12 months after the extracts start
HORIZON_DAYS = 30
BOUNCE_CHARGE_INR = 590.0           # per bounced presentation in the current arrears
FAR = "2999-12-31"

FEATURES = ["product_code", "dpd_now", "overdue_amount", "emi_amount", "overdue_to_emi", "months_on_book",
            "max_dpd_12m", "nach_bounces_6m", "broken_ptp_3m", "contacts_reached_30d",
            "salary_credit_last_30d", "salary_account_flag", "bureau_score"]
LABEL = "rolled_to_sma1_30d"
COLUMNS = ["loan_id", "snapshot_date", *FEATURES, LABEL]


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def snapshot_dates(as_of: date) -> list[date]:
    out, d = [], FIRST_SNAPSHOT
    while d <= as_of:
        out.append(d)
        d += timedelta(days=7)
    if not out or out[-1] != as_of:
        out.append(as_of)
    return out


def sma0_population(spark: SparkSession, instalment: DataFrame, snaps: list[date]) -> DataFrame:
    """(loan_id, snapshot_date) with dpd 1-30, oldest unpaid due and the unpaid EMIs."""
    snap_df = F.broadcast(spark.createDataFrame([(d,) for d in snaps], "snapshot_date date"))
    unpaid = (instalment.where(F.col("paid_date").isNull() | (F.col("paid_date") > F.col("due_date")))
              .join(snap_df, (F.col("due_date") < F.col("snapshot_date"))
                    & (F.col("paid_date").isNull() | (F.col("paid_date") > F.col("snapshot_date")))))
    return (unpaid.groupBy("loan_id", "snapshot_date")
            .agg(F.min("due_date").alias("oldest_due"), F.sum("emi_amount").alias("unpaid_emi"))
            .withColumn("dpd_now", F.datediff("snapshot_date", "oldest_due"))
            .where(F.col("dpd_now").between(1, 30)))


def build(spark: SparkSession, db_prefix: str) -> tuple[DataFrame, date]:
    s = f"{db_prefix}_silver"
    loan = spark.table(f"{s}.loan")
    instalment = spark.table(f"{s}.instalment")
    as_of = loan.agg(F.max("batch_as_of")).first()[0]
    snaps = snapshot_dates(as_of)
    logger.info("as of %s: %d snapshot dates %s -> %s", as_of, len(snaps), snaps[0], snaps[-1])

    pop = sma0_population(spark, instalment, snaps).cache()
    keys = pop.select("loan_id", "snapshot_date", "oldest_due")
    d = F.col("snapshot_date")

    inst = instalment.select("loan_id", "due_date", "paid_date")
    paid_or_far = F.coalesce(F.col("paid_date"), F.lit(FAR).cast("date"))
    hist = (keys.join(inst, "loan_id")
            .where((F.col("due_date") <= d) & (paid_or_far > F.date_sub(d, 365))))
    max_dpd = (hist.groupBy("loan_id", "snapshot_date")
               .agg(F.max(F.greatest(F.lit(0), F.datediff(F.least(d, F.date_sub(paid_or_far, 1)), "due_date")))
                    .alias("max_dpd_12m")))

    fwd = keys.join(inst, "loan_id")
    reached_31 = (F.greatest(F.date_add(d, 1), F.date_add("due_date", 31))
                  <= F.least(F.date_add(d, HORIZON_DAYS), F.date_sub(paid_or_far, 1)))
    label = (fwd.groupBy("loan_id", "snapshot_date")
             .agg(F.max(reached_31.cast("int")).alias("rolled")))

    nach = spark.table(f"{s}.nach_presentation").where(F.col("status") == "BOUNCED")
    bounces = (keys.join(nach.select("loan_id", "presentation_date"), "loan_id")
               .where(F.col("presentation_date") <= d)
               .groupBy("loan_id", "snapshot_date")
               .agg(F.sum((F.col("presentation_date") > F.date_sub(d, 182)).cast("int")).alias("nach_bounces_6m"),
                    F.sum((F.col("presentation_date") >= F.col("oldest_due")).cast("int")).alias("bounces_now")))

    contact = spark.table(f"{s}.collection_contact")
    contacts = (keys.join(contact.select("loan_id", F.to_date("contact_ts").alias("cd"), "outcome",
                                         "ptp_date", "ptp_kept"), "loan_id")
                .groupBy("loan_id", "snapshot_date")
                .agg(F.sum(((F.col("outcome") == "REACHED") & (F.col("cd") > F.date_sub(d, 30))
                            & (F.col("cd") <= d)).cast("int")).alias("contacts_reached_30d"),
                     F.sum(((F.col("ptp_kept") == F.lit(False)) & (F.col("ptp_date") > F.date_sub(d, 91))
                            & (F.col("ptp_date") <= d)).cast("int")).alias("broken_ptp_3m")))

    cust = loan.select("loan_id", "customer_id")
    salary = (keys.join(cust, "loan_id")
              .join(spark.table(f"{s}.salary_credit").select("customer_id", "credit_date"), "customer_id")
              .where((F.col("credit_date") > F.date_sub(d, 30)) & (F.col("credit_date") <= d))
              .groupBy("loan_id", "snapshot_date").agg(F.lit(1).alias("salary_credit_last_30d")))
    bureau = (keys.join(cust, "loan_id")
              .join(spark.table(f"{s}.bureau_score"), "customer_id")
              .where(F.col("pull_date") <= d)
              .groupBy("loan_id", "snapshot_date")
              .agg(F.max_by("bureau_score", "pull_date").alias("bureau_score")))

    labelled = F.date_add(d, HORIZON_DAYS) <= F.lit(as_of)
    feats = (pop.join(loan.select("loan_id", "product_code", "emi_amount", "disbursal_date",
                                  "salary_account_flag", "bureau_score_at_origination"), "loan_id")
             .join(max_dpd, ["loan_id", "snapshot_date"], "left")
             .join(label, ["loan_id", "snapshot_date"], "left")
             .join(bounces, ["loan_id", "snapshot_date"], "left")
             .join(contacts, ["loan_id", "snapshot_date"], "left")
             .join(salary, ["loan_id", "snapshot_date"], "left")
             .join(bureau, ["loan_id", "snapshot_date"], "left")
             .withColumn("overdue_amount",
                         F.round(F.col("unpaid_emi") + BOUNCE_CHARGE_INR * F.coalesce("bounces_now", F.lit(0)), 2))
             .select(
                 "loan_id", "snapshot_date",
                 F.col("product_code").cast("int"),
                 F.col("dpd_now").cast("int"),
                 "overdue_amount",
                 F.col("emi_amount").cast("double"),
                 F.round(F.col("overdue_amount") / F.col("emi_amount"), 3).alias("overdue_to_emi"),
                 F.floor(F.months_between(d, "disbursal_date")).cast("int").alias("months_on_book"),
                 F.coalesce("max_dpd_12m", "dpd_now").cast("int").alias("max_dpd_12m"),
                 F.coalesce("nach_bounces_6m", F.lit(0)).cast("int").alias("nach_bounces_6m"),
                 F.coalesce("broken_ptp_3m", F.lit(0)).cast("int").alias("broken_ptp_3m"),
                 F.coalesce("contacts_reached_30d", F.lit(0)).cast("int").alias("contacts_reached_30d"),
                 F.coalesce("salary_credit_last_30d", F.lit(0)).cast("int").alias("salary_credit_last_30d"),
                 F.col("salary_account_flag").cast("int"),
                 F.coalesce("bureau_score", "bureau_score_at_origination").cast("int").alias("bureau_score"),
                 F.when(labelled, F.coalesce("rolled", F.lit(0))).cast("int").alias(LABEL)))
    return feats, as_of


def bootstrap_or_merge(spark: SparkSession, target: str, source_view: str) -> None:
    if spark.catalog.tableExists(target):
        changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in COLUMNS[2:])
        spark.sql(f"""
            MERGE INTO {target} t
            USING {source_view} s
            ON t.loan_id = s.loan_id AND t.snapshot_date = s.snapshot_date
            WHEN MATCHED AND ({changed}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)
        logger.info("Merged into %s (new snapshot)", target)
    else:
        spark.sql(f"""
            CREATE TABLE {target}
            USING iceberg
            PARTITIONED BY (months(snapshot_date))
            TBLPROPERTIES ('format-version'='2')
            AS SELECT * FROM {source_view}
        """)
        logger.info("Bootstrapped %s (first run)", target)


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("coll-build-gold-features").getOrCreate()
    try:
        gold_db = f"{args.db_prefix}_gold"
        target = f"{gold_db}.collections_features"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")
        feats, as_of = build(spark, args.db_prefix)
        feats.withColumn("loaded_at", F.current_timestamp()).createOrReplaceTempView("collections_features_src")
        bootstrap_or_merge(spark, target, "collections_features_src")

        summary = (spark.table(target).groupBy("snapshot_date")
                   .agg(F.count("*").alias("sma0_loans"), F.round(F.avg(LABEL), 3).alias("roll_rate"),
                        F.round(F.sum("overdue_amount") / 1e7, 2).alias("overdue_inr_cr"))
                   .orderBy(F.col("snapshot_date").desc()))
        summary.show(10, truncate=False)
        logger.info("%s rows in %s (as of %s)", spark.table(target).count(), target, as_of)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()
