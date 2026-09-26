"""
Stage 3 - Silver (conformed loan, payment and contact history)

Cleans the bronze extracts into one table per business entity: removes re-sent
duplicates (latest ingested record wins), normalises codes, drops unusable
rows, and derives whether each promise-to-pay was kept.

A PTP is kept when the loan has a payment between the contact date and the
promised date. It stays NULL while the promised date is after the load's as-of
date, so the feature build never counts an open promise as broken.

Collector outcomes captured in the Streamlit app (bronze.collector_outcomes,
if the table exists) are unioned into the contact history, so tomorrow's
features include what the collectors did today.

Reads:
  <prefix>_bronze.{loan_master, loan_instalments, nach_presentations,
                   dialler_contacts, casa_credits, bureau_snapshot, collector_outcomes?}
  <prefix>_ref.product_map
Writes (drop + recreate every run):
  <prefix>_silver.{loan, instalment, nach_presentation, collection_contact,
                   salary_credit, bureau_score}

Usage:
  spark-submit build_silver.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_collections_delinquency_prediction"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def dedupe(df: DataFrame, *keys: str) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _write(df: DataFrame, name: str, partition_col=None) -> None:
    w = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    if partition_col is not None:
        w = w.partitionedBy(partition_col)
    w.createOrReplace()


def contacts_with_ptp(contacts: DataFrame, instalments: DataFrame, as_of) -> DataFrame:
    payments = instalments.where(F.col("paid_date").isNotNull()).select("loan_id", "paid_date").distinct()
    c = contacts.alias("c")
    kept = (c.where(F.col("ptp_date").isNotNull())
            .join(payments.alias("p"),
                  (F.col("c.loan_id") == F.col("p.loan_id"))
                  & (F.col("p.paid_date") >= F.to_date("c.contact_ts"))
                  & (F.col("p.paid_date") <= F.col("c.ptp_date")), "left")
            .groupBy("c.contact_id").agg(F.max(F.col("p.paid_date").isNotNull().cast("int")).alias("paid")))
    return (contacts.join(kept, "contact_id", "left")
            .withColumn("ptp_kept",
                        F.when(F.col("ptp_date").isNull(), None)
                        .when(F.col("paid") == 1, True)
                        .when(F.col("ptp_date") > F.lit(as_of), None)
                        .otherwise(False))
            .drop("paid"))


def run(spark: SparkSession, db_prefix: str) -> None:
    bronze, silver = f"{db_prefix}_bronze", f"{db_prefix}_silver"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver}")
    as_of = spark.table(f"{bronze}.loan_master").agg(F.max("batch_as_of")).first()[0]

    products = spark.table(f"{db_prefix}_ref.product_map").select("product_code", "product_name")
    loan = (dedupe(spark.table(f"{bronze}.loan_master"), "loan_id")
            .join(products, "product_code", "left")
            .select("loan_id", "customer_id", "product_code", "product_name", "branch_region", "disbursal_date",
                    "tenor_months", "emi_amount", "sanction_amount", "interest_rate",
                    F.upper(F.trim("repayment_mode")).alias("repayment_mode"),
                    "salary_account_flag", "bureau_score_at_origination", "due_day", "batch_as_of"))

    instalment = (dedupe(spark.table(f"{bronze}.loan_instalments"), "loan_id", "instalment_no")
                  .where(F.col("emi_amount") > 0)
                  .select("loan_id", "instalment_no", "due_date", "emi_amount", "paid_date", "paid_amount",
                          F.upper(F.trim("payment_channel")).alias("payment_channel")))

    nach = (dedupe(spark.table(f"{bronze}.nach_presentations"), "loan_id", "presentation_date", "presentation_seq")
            .select("loan_id", "presentation_date", "presentation_seq", "amount",
                    F.upper(F.trim("status")).alias("status"), "bounce_reason"))

    contact_cols = ["contact_id", "loan_id", "contact_ts", "channel", "outcome", "ptp_date", "ptp_amount"]
    contacts = (dedupe(spark.table(f"{bronze}.dialler_contacts"), "contact_id")
                .select(*contact_cols, F.lit("DIALLER").alias("source")))
    if spark.catalog.tableExists(f"{bronze}.collector_outcomes"):
        outcomes = (spark.table(f"{bronze}.collector_outcomes")
                    .where(F.to_date("contact_ts") <= F.lit(as_of))
                    .select(*contact_cols, F.lit("COLLECTOR_APP").alias("source")))
        logger.info("collector outcomes from the app: %d", outcomes.count())
        contacts = contacts.unionByName(outcomes)
    contacts = (contacts.withColumn("channel", F.upper(F.trim("channel")))
                .withColumn("outcome", F.upper(F.trim("outcome"))))
    contact = contacts_with_ptp(contacts, instalment, as_of)

    salary = (dedupe(spark.table(f"{bronze}.casa_credits"), "customer_id", "credit_date", "credit_type")
              .where(F.col("credit_type") == "SALARY").select("customer_id", "credit_date", "amount"))
    bureau = (dedupe(spark.table(f"{bronze}.bureau_snapshot"), "customer_id", "pull_date")
              .where(F.col("bureau_score").between(300, 900)).select("customer_id", "pull_date", "bureau_score"))

    _write(loan, f"{silver}.loan")
    _write(instalment, f"{silver}.instalment", F.years("due_date"))
    _write(nach, f"{silver}.nach_presentation", F.years("presentation_date"))
    _write(contact, f"{silver}.collection_contact", F.years("contact_ts"))
    _write(salary, f"{silver}.salary_credit", F.years("credit_date"))
    _write(bureau, f"{silver}.bureau_score", F.years("pull_date"))

    for t in ("loan", "instalment", "nach_presentation", "collection_contact", "salary_credit", "bureau_score"):
        logger.info("%s.%s: %d rows", silver, t, spark.table(f"{silver}.{t}").count())
    (spark.table(f"{silver}.collection_contact").where("ptp_date IS NOT NULL")
     .groupBy("ptp_kept").count().orderBy("ptp_kept").show())


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("coll-build-silver").getOrCreate()
    try:
        run(spark, args.db_prefix)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()
