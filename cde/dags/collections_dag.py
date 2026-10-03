"""
Airflow DAG (CDE): daily collections roll-forward pipeline.

  generate_loan_bronze -> dq_bronze -> build_silver -> dq_silver -> build_gold_features -> dq_gold   (CDE Spark)
    -> cai_daily_score                                                                               (CAI Job via API v2)

dq_bronze/dq_silver/dq_gold are the same CDE job (cde/jobs/dq_check.py, Great
Expectations) run three times with --layer overridden per task. Critical
expectation failures fail the task (and stop the DAG before the next layer);
warnings are recorded but let the run continue. Every result, pass or fail, is
appended to <db_prefix>_ref.dq_results (Iceberg) tagged with this DAG run's
run_id, so a run's three layers can be grouped for a summary or a CDV dashboard.

The CAI step triggers the Cloudera AI job `cai/jobs/daily_score.py` and waits
for it, so one DAG run goes from the overnight LMS / NACH / dialler extracts to
the morning call list. It needs these Airflow Variables, set by
cde/scripts/set_airflow_variables.py before the DAG is deployed:
  COLL_CAI_HOST        https://federal-cml.federal.dp5i-5vkq.cloudera.site  (CAI workbench URL)
  COLL_CAI_PROJECT_ID  project id of rsingh-coll-dlq
  COLL_CAI_JOB_ID      id of the rsingh-coll-dlq-daily-score job
  COLL_CAI_API_KEY     CAI API v2 key (User settings > API keys; not the Model API key)
If COLL_CAI_HOST is not set the CAI step is skipped, so the Spark part can be
tested on its own.

Scheduled daily at 00:30 UTC (06:00 IST): the run loads and scores the
business day just closed. Clear of the other DAGs on the shared vcluster
(Mule 20:30, Spend 21:00, Churn 22:00 UTC). Manual trigger (Trigger DAG w/
config): {"as_of": "2026-09-25"}; empty = yesterday.

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
CAI_DEADLINE_MIN = 90
MAX_POLL_ERRORS = 10


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

    deadline, poll_errors = time.time() + CAI_DEADLINE_MIN * 60, 0
    while time.time() < deadline:
        time.sleep(30)
        # A dropped poll must not fail the task: the run carries on in CAI, and a
        # task retry would start a second run.
        try:
            r = requests.get(f"{url}/{run_id}", headers=headers, timeout=60)
            if r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
        except requests.RequestException as e:
            if isinstance(e, requests.HTTPError) and e.response is not None and e.response.status_code < 500:
                raise
            poll_errors += 1
            print(f"CAI run {run_id}: poll failed ({poll_errors}/{MAX_POLL_ERRORS}): {e}")
            if poll_errors >= MAX_POLL_ERRORS:
                raise AirflowException(f"CAI job run {run_id}: {MAX_POLL_ERRORS} failed polls in a row") from e
            continue
        poll_errors = 0
        status = str(r.json().get("status", "")).lower().replace("engine_", "")
        print(f"CAI run {run_id}: {status}")
        if status in TERMINAL_OK:
            return run_id
        if status in TERMINAL_BAD:
            raise AirflowException(f"CAI job run {run_id} ended with status {status}")
    raise AirflowException(f"CAI job run {run_id} did not finish within {CAI_DEADLINE_MIN} minutes")


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
    # In the past, or manual triggers run no tasks.
    start_date=datetime(2026, 9, 26, 0, 30),
    catchup=False,
    # Registered paused. Registering unpaused, or unpausing later, runs the latest closed
    # interval at once.
    is_paused_upon_creation=True,
    params={"as_of": ""},
    tags=["collections", "sma", "tabicl", "iceberg"],
) as dag:

    def dq_task(task_id: str, layer: str) -> CDEJobRunOperator:
        # run-time args replace the job's own args, so repeat --db-prefix; --pipeline-run
        # groups this run's three layers in dq_results under the DAG run_id
        return CDEJobRunOperator(
            task_id=task_id,
            job_name=f"{JOB_PREFIX}-dq-check",
            overrides={"spark": {"args": ["--db-prefix", DB_PREFIX, "--layer", layer, "--as-of", AS_OF,
                                           "--pipeline-run", "{{ run_id }}"]}},
            wait=True,
        )

    generate = CDEJobRunOperator(
        task_id="generate_loan_bronze",
        job_name=f"{JOB_PREFIX}-generate-loan-bronze",
        # run-time args replace the job's own args, so repeat --db-prefix
        overrides={"spark": {"args": ["--db-prefix", DB_PREFIX, "--as-of", AS_OF]}},
        wait=True,
    )
    dq_bronze = dq_task("dq_bronze", "bronze")
    silver = CDEJobRunOperator(task_id="build_silver", job_name=f"{JOB_PREFIX}-build-silver", wait=True)
    dq_silver = dq_task("dq_silver", "silver")
    gold = CDEJobRunOperator(task_id="build_gold_features", job_name=f"{JOB_PREFIX}-build-gold-features", wait=True)
    dq_gold = dq_task("dq_gold", "gold")
    score = PythonOperator(
        task_id="cai_daily_score",
        python_callable=trigger_cai_job,
        op_kwargs={"as_of": AS_OF},
        retries=0,
    )

    generate >> dq_bronze >> silver >> dq_silver >> gold >> dq_gold >> score
