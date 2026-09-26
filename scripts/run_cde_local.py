#!/usr/bin/env python3
"""
Run the CDE Spark jobs on a laptop against a local Iceberg (Hadoop) catalog,
so the same job files can be tested without a vcluster. Optionally exports
the gold features table to parquet for the CAI job / app in parquet mode.

  python scripts/run_cde_local.py all --as-of 2026-09-25
  python scripts/run_cde_local.py gold export
  python scripts/run_cde_local.py history

Stages: generate, validate, silver, gold, export, all (= the four jobs + export),
history (list Iceberg snapshots of a gold table).
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JOBS = ROOT / "cde" / "jobs"
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
DEFAULT_PREFIX = "rsingh_collections_delinquency_prediction"
STAGES = {
    "generate": "generate_loan_bronze.py",
    "validate": "validate_bronze.py",
    "silver": "build_silver.py",
    "gold": "build_gold_features.py",
}
EXPORT_TABLES = {"gold": ["collections_features"], "silver": ["loan"]}


def local_spark(warehouse: Path):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (SparkSession.builder.appName("coll-local")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.jars.packages", ICEBERG_PACKAGE)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", str(warehouse))
            .config("spark.sql.defaultCatalog", "local")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "6g")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def load_job(filename: str):
    spec = importlib.util.spec_from_file_location(filename[:-3], JOBS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def export_parquet(spark, db_prefix: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for layer, tables in EXPORT_TABLES.items():
        for table in tables:
            pdf = spark.table(f"{db_prefix}_{layer}.{table}").toPandas()
            name = "loan_master" if table == "loan" else table
            path = out_dir / f"{name}.parquet"
            pdf.to_parquet(path, index=False)
            print(f"exported {len(pdf)} rows -> {path.relative_to(ROOT)}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("stages", nargs="+", choices=[*STAGES, "export", "all", "history"])
    p.add_argument("--as-of", default=None)
    p.add_argument("--db-prefix", default=DEFAULT_PREFIX)
    p.add_argument("--loans", type=int, default=None)
    p.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse"))
    p.add_argument("--parquet-dir", default=str(ROOT / "data" / "parquet"))
    p.add_argument("--table", default="collections_features")
    args = p.parse_args()

    stages = [*STAGES, "export"] if "all" in args.stages else args.stages
    job_args = ["--db-prefix", args.db_prefix]
    if args.as_of:
        job_args += ["--as-of", args.as_of]
    if args.loans:
        job_args += ["--loans", str(args.loans)]

    spark = local_spark(Path(args.warehouse))
    spark.sparkContext.setLogLevel("WARN")
    try:
        for stage in stages:
            print(f"\n=== {stage} ===")
            if stage == "export":
                export_parquet(spark, args.db_prefix, Path(args.parquet_dir))
            elif stage == "history":
                spark.sql(f"SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added, "
                          f"summary['deleted-records'] AS deleted "
                          f"FROM {args.db_prefix}_gold.{args.table}.snapshots ORDER BY committed_at").show(truncate=False)
            else:
                try:
                    load_job(STAGES[stage]).main(job_args, spark=spark)
                except SystemExit as e:
                    if e.code:
                        sys.exit(f"stage '{stage}' failed (exit {e.code})")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
