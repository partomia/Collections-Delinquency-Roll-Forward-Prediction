#!/usr/bin/env bash
# Load the lakehouse day by day so the gold table has one Iceberg snapshot per
# daily load, just like real overnight extracts: each load adds the day's SMA-0
# rows and fills in the labels that matured. Afterwards run the CAI backfill
# (cai/jobs/backfill_history.py) to create matching call lists.
#
#   ./cde/scripts/backfill_drill.sh                         # the last 3 Fridays, then yesterday
#   ./cde/scripts/backfill_drill.sh 2026-09-11 2026-09-18   # explicit dates
#
# Run-time --arg values replace the job's own args, so --db-prefix is passed
# again here. Run the chain back to back: the shared vcluster scales down
# after ~15 min idle and a cold scale-up can take 20+ minutes.

set -euo pipefail

JOB_PREFIX="${JOB_PREFIX:-rsingh-coll-dlq}"
DB_PREFIX="${DB_PREFIX:-rsingh_collections_delinquency_prediction}"

if [[ $# -gt 0 ]]; then
  dates=("$@")
else
  read -r -a dates <<< "$(python3 - <<'EOF'
from datetime import date, timedelta
y = date.today() - timedelta(days=1)
friday = y - timedelta(days=(y.weekday() - 4) % 7)   # latest Friday on or before yesterday
out = sorted({friday - timedelta(weeks=k) for k in range(3)} | {y})
print(" ".join(d.isoformat() for d in out))
EOF
)"
fi

run() { cde job run --name "$1" --arg=--db-prefix --arg="${DB_PREFIX}" "${@:2}" --wait; }

dq() { run "${JOB_PREFIX}-dq-check" --arg=--layer --arg="$1" --arg=--as-of --arg="$2" --arg=--pipeline-run --arg="drill-$2"; }

for as_of in "${dates[@]}"; do
  echo "==================== as of ${as_of} ($(date '+%H:%M:%S'))"
  run "${JOB_PREFIX}-generate-loan-bronze" --arg=--as-of --arg="${as_of}"
  dq bronze "${as_of}"
  run "${JOB_PREFIX}-build-silver"
  dq silver "${as_of}"
  run "${JOB_PREFIX}-build-gold-features"
  dq gold "${as_of}"
done

echo ""
echo "Gold snapshots, one per load. In Hue (Impala):"
echo "  DESCRIBE HISTORY ${DB_PREFIX}_gold.collections_features;"
echo "Now create the matching call lists from a CAI session:"
echo "  python cai/jobs/backfill_history.py --weeks 8"
