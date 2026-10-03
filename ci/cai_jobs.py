"""
The CAI resources of this project: jobs, model and application, read by
ci/setup_cai.py (creates or adopts them) and ci/trigger_cai_pipeline.py, and
checked against docs/DEMO_RUNBOOK.md by the tests. Pure Python, so the GitHub
runner can import it without cmlapi.

The jobs have no CAI dependencies: daily, Airflow starts rsingh-coll-dlq-daily-score
(cde/dags/collections_dag.py); on a push, GitHub Actions starts GITHUB_CHAIN one by
one through the CAI API v2. A job run ignores arguments, so both pass settings
through the run's environment.

The GitHub chain is a candidate check, not a publish: the scoring job runs with
COLL_DRY_RUN=1, so it runs the holdout and scores the book but writes no table.
The daily DAG publishes.
"""
from __future__ import annotations

CAI_PROJECT_NAME = "rsingh-coll-dlq"
GIT_URL = "https://github.com/partomia/Collections-Delinquency-Roll-Forward-Prediction"
RUNTIME = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5"

SYNC_JOB = "rsingh-coll-dlq-sync-code"
DAILY_JOB = "rsingh-coll-dlq-daily-score"
BACKFILL_JOB = "rsingh-coll-dlq-backfill-history"
JOBS = [
    # name, script, vCPU, memory GB, GPUs, timeout minutes (the runbook's job table)
    {"name": SYNC_JOB, "script": "cai/jobs/sync_code.py", "cpu": 2, "memory": 8, "gpu": 0, "timeout": 60},
    {"name": DAILY_JOB, "script": "cai/jobs/daily_score.py", "cpu": 4, "memory": 16, "gpu": 0, "timeout": 60},
    # one-off, started by hand; COLL_BACKFILL_WEEKS in the run's environment (default 8)
    {"name": BACKFILL_JOB, "script": "cai/jobs/backfill_history.py", "cpu": 4, "memory": 16, "gpu": 0,
     "timeout": 300},
]
# CPU only: on federal GPUs cannot be scheduled from these projects (a 1-GPU run, and
# 8 vCPU / 32 GB, sat in ENGINE_SCHEDULING), so TabICL runs on CPU with the CPU
# context size (config/policy.yaml rows_cpu). go01 ran the job and model on an NVIDIA L4.
BY_NAME = {j["name"]: j for j in JOBS}
DEADLINE_MIN = 90                     # per job run, as the DAG (cde/dags/collections_dag.py CAI_DEADLINE_MIN)

MODEL = {"name": "rsingh-coll-dlq-roll-scorer", "file": "cai/model/predict.py", "cpu": 4, "memory": 16, "gpu": 0,
         "description": "Roll-forward probability, priority and what-if for SMA-0 loans"}
APP = {"name": "rsingh-coll-dlq-call-list", "subdomain": "rsingh-coll-dlq-call-list", "script": "app/run.py",
       "cpu": 2, "memory": 4, "description": "Streamlit collections call list over the published daily run"}

GITHUB_CHAIN = [SYNC_JOB, DAILY_JOB]


def github_env(name: str, sha: str) -> dict:
    """The environment of one job run in the GitHub chain."""
    if name == SYNC_JOB:
        return {"EXPECTED_GIT_SHA": sha[:12]}
    return {"COLL_TRIGGERED_BY": "github", "COLL_DRY_RUN": "1"}
