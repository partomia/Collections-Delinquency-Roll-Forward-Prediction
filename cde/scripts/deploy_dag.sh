#!/usr/bin/env bash
# Register/update the Airflow DAG as a `--type airflow` CDE job sourced from
# the repository. Re-run after every DAG change: `cde repository sync` alone
# does not refresh an already-registered DAG. Set the COLL_CAI_* Airflow
# Variables first (cde/scripts/set_airflow_variables.py).

set -euo pipefail

REPO_NAME="${REPO_NAME:-rsingh-coll-dlq-pipeline}"
DAG_JOB_NAME="${DAG_JOB_NAME:-rsingh-coll-dlq-orchestration}"
DAG_PATH="cde/dags/collections_dag.py"

cde repository sync --name "${REPO_NAME}"

if cde job describe --name "${DAG_JOB_NAME}" &>/dev/null; then
  type="$(cde job describe --name "${DAG_JOB_NAME}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('type'))")"
  if [[ "${type}" != "airflow" ]]; then
    echo "==> ${DAG_JOB_NAME} has type ${type}; recreating as airflow"
    cde job delete --name "${DAG_JOB_NAME}"
  else
    echo "==> Updating ${DAG_JOB_NAME}"
    cde job update --name "${DAG_JOB_NAME}" --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
    echo "Give Airflow ~30s to re-parse before triggering."
    exit 0
  fi
fi

echo "==> Creating ${DAG_JOB_NAME}"
cde job create --name "${DAG_JOB_NAME}" --type airflow --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
echo "DAG collections_roll_forward_pipeline registered paused (daily, 00:30 UTC)."
echo "Unpausing runs the latest closed interval at once:"
echo "  cde job schedule unpause --name ${DAG_JOB_NAME}"
echo "Run by hand: cde job run --name ${DAG_JOB_NAME} --config-json '{\"as_of\": \"YYYY-MM-DD\"}'"
