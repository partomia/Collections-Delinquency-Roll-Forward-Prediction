"""
Airflow DAG (CDE): daily collections roll-forward pipeline.

  generate_loan_bronze -> validate_bronze -> build_silver -> build_gold_features   (CDE Spark)
    -> cai_daily_score                                                            (CAI Job via API v2)

The CAI step triggers the Cloudera AI job `cai/jobs/daily_score.py` and waits
for it, so one DAG run goes from the overnight LMS / NACH / dialler extracts to
the morning call list. It needs these Airflow Variables (CDE Airflow UI >
Admin > Variables):
  COLL_CAI_HOST        https://ml-xxxx.<env>.cloudera.site  (CAI workbench URL)
  COLL_CAI_PROJECT_ID  project id (from the project URL or API)
  COLL_CAI_JOB_ID      id of the daily scoring job
  COLL_CAI_API_KEY     CAI API v2 key (User settings > API keys)
If COLL_CAI_HOST is not set the CAI step is skipped, so the Spark part can be
tested on its own.

Scheduled daily at 00:30 UTC (06:00 IST): the run loads and scores the
business day just closed. Manual trigger (Trigger DAG w/ config):
{"as_of": "2026-09-25"}; empty = yesterday.

Job names must match cde/scripts/deploy_jobs.sh exactly (CDEJobRunOperator
fails with 404 "job not found" otherwise).
"""

import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

JOB_PREFIX = "rsingh-coll-dlq"
DB_PREFIX = "rsingh_collections_delinquency_prediction"
# Scheduled runs: the day before the interval end; manual runs: the as_of param (empty = yesterday).
AS_OF = ("{{ params.as_of or ((data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') "
         "if dag_run.run_type == 'scheduled' else (macros.datetime.utcnow() - macros.timedelta(days=1))"
         ".strftime('%Y-%m-%d')) }}")
TERMINAL_OK = {"succeeded"}
TERMINAL_BAD = {"failed", "stopped", "timedout"}
DAILY = "30 0 * * *"


def trigger_cai_job(as_of: str, **_):
    host = Variable.get("COLL_CAI_HOST", default_var="").rstrip("/")
    if not host:
        raise AirflowSkipException("COLL_CAI_HOST not set: skipping the CAI scoring step")
    project = Variable.get("COLL_CAI_PROJECT_ID")
    job = Variable.get("COLL_CAI_JOB_ID")
    headers = {"Authorization": f"Bearer {Variable.get('COLL_CAI_API_KEY')}", "Content-Type": "application/json"}
    # A job run ignores "arguments" (the job's own are used); the environment map is applied.
    env = {"COLL_TRIGGERED_BY": "airflow", "COLL_RUN_DATE": as_of}
    url = f"{host}/api/v2/projects/{project}/jobs/{job}/runs"
    resp = requests.post(url, json={"environment": env}, headers=headers, timeout=60)
    resp.raise_for_status()
    run_id = resp.json()["id"]
    print(f"Started CAI job run {run_id} with environment: {env}")

    deadline = time.time() + 60 * 60
    while time.time() < deadline:
        time.sleep(30)
        r = requests.get(f"{url}/{run_id}", headers=headers, timeout=60)
        r.raise_for_status()
        status = str(r.json().get("status", "")).lower().replace("engine_", "")
        print(f"CAI run {run_id}: {status}")
        if status in TERMINAL_OK:
            return run_id
        if status in TERMINAL_BAD:
            raise AirflowException(f"CAI job run {run_id} ended with status {status}")
    raise AirflowException(f"CAI job run {run_id} did not finish within 60 minutes")


default_args = {
    "owner": "collections",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="collections_roll_forward_pipeline",
    description="LMS / NACH / dialler extracts -> bronze/silver/gold (CDE) -> TabICL call list (CAI)",
    default_args=default_args,
    schedule_interval=DAILY,
    start_date=datetime(2026, 9, 27, 0, 30),
    catchup=False,
    is_paused_upon_creation=False,
    params={"as_of": ""},
    tags=["collections", "sma", "tabicl", "iceberg"],
) as dag:

    generate = CDEJobRunOperator(
        task_id="generate_loan_bronze",
        job_name=f"{JOB_PREFIX}-generate-loan-bronze",
        # run-time args replace the job's own args, so repeat --db-prefix
        overrides={"spark": {"args": ["--db-prefix", DB_PREFIX, "--as-of", AS_OF]}},
        wait=True,
    )
    validate = CDEJobRunOperator(task_id="validate_bronze", job_name=f"{JOB_PREFIX}-validate-bronze", wait=True)
    silver = CDEJobRunOperator(task_id="build_silver", job_name=f"{JOB_PREFIX}-build-silver", wait=True)
    gold = CDEJobRunOperator(task_id="build_gold_features", job_name=f"{JOB_PREFIX}-build-gold-features", wait=True)
    score = PythonOperator(
        task_id="cai_daily_score",
        python_callable=trigger_cai_job,
        op_kwargs={"as_of": AS_OF},
        retries=0,
    )

    generate >> validate >> silver >> gold >> score
