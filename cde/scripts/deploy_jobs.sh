#!/usr/bin/env bash
# Create/sync the CDE Repository for this GitHub repo and (re)create the four
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
# Resources: a 2-core / 4 GB driver and executors of 4 cores / 8 GB, 1 min /
# 2 initial / 4 max. The vcluster's YuniKorn queue must fit the driver plus the
# initial executors up front (PySpark adds 40% memory overhead), or the run is
# rejected with "queue ... cannot fit application". The federal queue caps at
# 27 vCPU / ~110 GB, shared by several projects; go01 ran a 4-core / 8 GB driver
# and 2-8 executors, 4 at start (DRIVER_CORES=4 DRIVER_MEMORY=8g MIN_EXECUTORS=2
# INITIAL_EXECUTORS=4 MAX_EXECUTORS=8).
#
# This script does not delete jobs it no longer defines; remove orphans by hand.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/partomia/Collections-Delinquency-Roll-Forward-Prediction}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_NAME="${REPO_NAME:-rsingh-coll-dlq-pipeline}"
PYTHON_ENV="${PYTHON_ENV:-rsingh-coll-dlq-python-env}"
JOB_PREFIX="${JOB_PREFIX:-rsingh-coll-dlq}"
DB_PREFIX="${DB_PREFIX:-rsingh_collections_delinquency_prediction}"
REQUIREMENTS="$(cd "$(dirname "$0")/.." && pwd)/resources/requirements.txt"
RESOURCES=(--driver-cores "${DRIVER_CORES:-2}" --driver-memory "${DRIVER_MEMORY:-4g}"
           --executor-cores "${EXECUTOR_CORES:-4}" --executor-memory "${EXECUTOR_MEMORY:-8g}"
           --min-executors "${MIN_EXECUTORS:-1}" --initial-executors "${INITIAL_EXECUTORS:-2}" --max-executors "${MAX_EXECUTORS:-4}"
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
echo "    building (~6 min with Great Expectations); jobs fail fast until it is ready:"
for _ in $(seq 1 30); do
  status="$(cde resource describe --name "${PYTHON_ENV}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")"
  echo "    status: ${status}"
  [[ "${status}" == "ready" ]] && break
  [[ "${status}" == "failed" ]] && { echo "python-env build failed"; exit 1; }
  sleep 20
done
[[ "${status}" == "ready" ]] || { echo "python-env not ready after 10 minutes"; exit 1; }

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
echo "Jobs deployed from ${REPO_NAME}. Run the chain for one as-of date (--wait can return"
echo "early on this vcluster: poll 'cde run describe --id <run id>' until it ends):"
echo "  cde job run --name ${JOB_PREFIX}-generate-loan-bronze --arg=--db-prefix --arg=${DB_PREFIX} --arg=--as-of --arg=YYYY-MM-DD"
echo "  cde job run --name ${JOB_PREFIX}-dq-check --arg=--db-prefix --arg=${DB_PREFIX} --arg=--layer --arg=bronze --arg=--as-of --arg=YYYY-MM-DD"
echo "  ... build-silver, dq-check silver, build-gold-features, dq-check gold"
echo "Then register the DAG: ./cde/scripts/deploy_dag.sh"
