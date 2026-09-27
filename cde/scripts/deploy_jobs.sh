#!/usr/bin/env bash
# Create/sync the CDE Repository for this GitHub repo and (re)create the five
# Spark jobs, each reading its application file straight from the repo. The
# dq-check job is shared by all three data-quality DAG tasks; --layer is set
# per task at run time (see cde/dags/collections_dag.py), so its own --layer
# arg here is just a safe default for an ad hoc `cde job run`.
#
# After a code change: git push, then either re-run this script or just
#   cde repository sync --name rsingh-coll-dlq-pipeline
#
# Private repo? Store a GitHub PAT as a CDE credential first (the flag takes
# the credential NAME, not the token):
#   cde credential create --name my-github-pat --type basic --username <github-user>
#   GIT_CREDENTIAL=my-github-pat ./cde/scripts/deploy_jobs.sh
#
# Resources: the vcluster default (1 core / 1 GB) is too slow for ~2M
# instalments, so every job gets a 4-core / 8 GB driver and 2-8 executors
# (4 at start) of 4 cores / 8 GB: 32 task slots for the generator's 30 partitions.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/partomia/Collections-Delinquency-Roll-Forward-Prediction}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_NAME="${REPO_NAME:-rsingh-coll-dlq-pipeline}"
PYTHON_ENV="${PYTHON_ENV:-rsingh-coll-dlq-python-env}"
JOB_PREFIX="${JOB_PREFIX:-rsingh-coll-dlq}"
DB_PREFIX="${DB_PREFIX:-rsingh_collections_delinquency_prediction}"
REQUIREMENTS="$(cd "$(dirname "$0")/.." && pwd)/resources/requirements.txt"
RESOURCES=(--driver-cores 4 --driver-memory 8g --executor-cores 4 --executor-memory 8g
           --min-executors 2 --initial-executors 4 --max-executors 8
           --conf spark.sql.shuffle.partitions=64)

echo "==> Repository: ${REPO_NAME}"
if cde repository describe --name "${REPO_NAME}" &>/dev/null; then
  echo "    exists, syncing ${REPO_BRANCH}"
else
  create_args=(--name "${REPO_NAME}" --url "${REPO_URL}" --branch "${REPO_BRANCH}")
  [[ -n "${GIT_CREDENTIAL:-}" ]] && create_args+=(--credential "${GIT_CREDENTIAL}")
  cde repository create "${create_args[@]}"
fi
cde repository sync --name "${REPO_NAME}"

echo "==> Python environment resource: ${PYTHON_ENV}"
cde resource create --name "${PYTHON_ENV}" --type python-env 2>/dev/null || true
cde resource upload --name "${PYTHON_ENV}" --local-path "${REQUIREMENTS}"
echo "    building (1-3 min); jobs fail fast until it is ready:"
for _ in $(seq 1 30); do
  status="$(cde resource describe --name "${PYTHON_ENV}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")"
  echo "    status: ${status}"
  [[ "${status}" == "ready" ]] && break
  [[ "${status}" == "failed" ]] && { echo "python-env build failed"; exit 1; }
  sleep 20
done

create_job() {
  local name=$1 file=$2
  shift 2
  if cde job describe --name "${name}" &>/dev/null; then
    cde job delete --name "${name}"
  fi
  echo "==> Creating job ${name} (${file})"
  cde job create --name "${name}" --type spark \
    --mount-1-resource "${REPO_NAME}" \
    --application-file "${file}" \
    --python-env-resource-name "${PYTHON_ENV}" \
    "${RESOURCES[@]}" \
    --arg=--db-prefix --arg="${DB_PREFIX}" "$@"
}

create_job "${JOB_PREFIX}-generate-loan-bronze" "cde/jobs/generate_loan_bronze.py"
create_job "${JOB_PREFIX}-dq-check"             "cde/jobs/dq_check.py" --arg=--layer --arg=bronze
create_job "${JOB_PREFIX}-build-silver"         "cde/jobs/build_silver.py"
create_job "${JOB_PREFIX}-build-gold-features"  "cde/jobs/build_gold_features.py"

echo ""
echo "Jobs deployed from ${REPO_NAME}. Run one:"
echo "  cde job run --name ${JOB_PREFIX}-generate-loan-bronze --wait"
echo "Then register the DAG: ./cde/scripts/deploy_dag.sh"
