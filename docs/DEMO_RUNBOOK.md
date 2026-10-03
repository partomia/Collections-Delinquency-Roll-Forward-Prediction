# Demo runbook (about 12 minutes)

On the federal environment (from 3 Oct 2026). Secrets (`COLL_IMPALA_PASSWORD`,
the model access key, any API key) are never written out; take them from `.env`
or your own User Settings each time.

| What | Name | ID |
|---|---|---|
| CAI workbench | federal | `https://federal-cml.federal.dp5i-5vkq.cloudera.site` |
| CAI project | `rsingh-coll-dlq` | (set by `ci/setup_cai.py`) |
| CAI jobs | `rsingh-coll-dlq-daily-score`, `rsingh-coll-dlq-sync-code`, `rsingh-coll-dlq-backfill-history` | |
| Model | `rsingh-coll-dlq-roll-scorer` | |
| Application | `rsingh-coll-dlq-call-list` | |
| CDE jobs | `rsingh-coll-dlq-{generate-loan-bronze,dq-check,build-silver,build-gold-features}` | |
| CDE DAG job | `rsingh-coll-dlq-orchestration` | dagID `collections_roll_forward_pipeline` |

## Setup (one-time, scripted from the laptop)

```bash
set -a; source .env; set +a
./cde/scripts/deploy_jobs.sh                          # CDE: repository, env, Spark jobs
python ci/setup_cai.py --dry-run                      # CAI: what would change
python ci/setup_cai.py --no-serving --sync            # CAI: project, environment, jobs; then sync-code
python ci/setup_cai.py --sync                         # + model and app, once a run is published
python cde/scripts/set_airflow_variables.py --dry-run
python cde/scripts/set_airflow_variables.py           # Airflow: COLL_CAI_* Variables
./cde/scripts/deploy_dag.sh                           # CDE: DAG, registered paused
```

The CAI jobs (`ci/cai_jobs.py`; the tests keep this table and the code in step):

| Job | Script | Size | Timeout |
|---|---|---|---|
| `rsingh-coll-dlq-sync-code` | `cai/jobs/sync_code.py` | 2 vCPU / 8 GB / 0 GPU | 60 min |
| `rsingh-coll-dlq-daily-score` | `cai/jobs/daily_score.py` | 4 vCPU / 16 GB / 0 GPU | 60 min |
| `rsingh-coll-dlq-backfill-history` | `cai/jobs/backfill_history.py` | 4 vCPU / 16 GB / 0 GPU | 300 min |

`rsingh-coll-dlq-backfill-history` is a one-off, started by hand with
`COLL_BACKFILL_WEEKS` in the run's environment (default 8).

CPU only on federal: a GPU, or 8 vCPU / 32 GB, cannot be scheduled from this
project, so the job and model run TabICL on CPU with the 10,000-row context
(`config/policy.yaml` `rows_cpu`). go01 ran them on an NVIDIA L4 with 50,000 rows.

## Before the demo (30 minutes ahead)

- CDE Job Runs: today's 06:00 IST DAG run succeeded (all seven tasks green,
  including the three `dq_*` Great Expectations gates).
- App History tab: today's run with `triggered_by = airflow`, plus 4+ backfilled run dates.
- Model `rsingh-coll-dlq-roll-scorer` restarted after today's run; its Test tab shows today's `run_date`.
- Open the app and run one Hue query 5 minutes before: the Impala virtual
  warehouse auto-suspends and the first query after a pause can take minutes.
- Hue open on `sql/reports.sql`; the Airflow UI open on the DAG grid.
- Showing CDE live? Check `cde run list --filter 'status[eq]running'` first (the
  vcluster queue is shared), then trigger the DAG well ahead: a cold vcluster
  needs a few minutes to scale up (measured run times: `docs/PROJECT_LOG.md`).

## 1. The question (1 min)

Collections teams cannot call everyone in early delinquency. Every morning the
SMA-0 book (1-30 DPD, RBI early-stress class) has thousands of loans and the
dialler has capacity for a fraction. Which loans get a senior agent today, which
get an IVR call with a payment link, and which only an SMS?

## 2. The pipeline (2 min): CDE Airflow UI

- DAG `collections_roll_forward_pipeline`, daily: LMS / NACH / dialler / CASA /
  bureau extracts → `dq_bronze` (Great Expectations) → silver → `dq_silver` →
  gold features (Iceberg MERGE) → `dq_gold` → CAI scoring job via API.
- Each `dq_*` gate is the same CDE job (`dq_check.py`) run with `--layer`
  overridden per task. Critical failures stop the DAG (yesterday's call list
  stands); warnings are recorded but let the run continue. Every check lands
  in `ref.dq_results`, tagged with this DAG run's `run_id` — point at query 9
  in `sql/reports.sql` for a pass/fail summary by layer.
- The gold table has one row per SMA-0 loan per weekly snapshot, all numeric,
  and the label "reached SMA-1 within 30 days" fills in as it matures: each
  load is one Iceberg snapshot.

## 3. Today's call list (3 min): app, first tab

- SMA-0 loans scored, overdue in SMA-0, expected rolls, expected overdue at risk.
- Priority = roll probability × overdue amount: a likely roll on a large loan
  outranks a likely roll on a small one. Point at the risk signals column
  ("no salary credit in 30d; 2 broken PTP in 3m"): what the collector opens with.
- Move the **Agent call today** slider from 10% to 5%: the book re-bands
  instantly, no re-scoring. Cut-offs belong to the collections head, not the model.

## 4. Can we trust it? (2 min): Model trust tab

- The talking point: "the top 10% of calls catch X% of the loans that actually
  rolled", and the agent bands (top 40%) catch Y%, against calling the most
  overdue first (DPD alone). Collections heads think in dialler capacity, not AUC.
- The holdout is honest: context ends 30 days before the test weeks, as if the
  model had been deployed then. Decile chart: predicted vs actual roll rate.
- Why DPD is a strong baseline: a loan at 25 DPD only has to stay unpaid 6 more
  days. The model adds most on low-DPD loans that will still roll.

## 5. Live what-if (2 min): Loan what-if tab

- Pick a loan near the top with "no salary credit in 30d". Score it as is, then
  set salary credit = Yes and contacts reached = 2: the roll probability drops.
  Scored live by the CAI model endpoint.
- TabICL has no training step: `fit` stores the labelled context and learning
  happens at prediction time. The same model family works for any tabular
  question on the platform.

## 6. Close the loop (1 min): Collector outcomes tab

- Record "REACHED, PTP ₹X in 3 days" for the loan. It lands in
  `bronze.collector_outcomes`; tomorrow's silver load unions it into the contact
  history, so tomorrow's features and context include it. No retraining cycle.

## 7. Audit and time travel (1 min): History tab + Hue

- Lineage table: pinned checkpoint, TabICL version, context window, and the
  Iceberg snapshot id of gold each run read.
- In Hue: `DESCRIBE HISTORY` on `collections_features`, then query 8 in
  `sql/reports.sql` with `FOR SYSTEM_VERSION AS OF <source_snapshot_id>` to
  rebuild the exact context a past call list learned from. Query 6 checks
  whether a past top band actually rolled.

## Honest framing (close)

Synthetic data, demo pipeline, not a validated credit model. The model only
ranks; contact hours, conduct and frequency stay with policy and the RBI fair
practices code. The context holds borrower rows, so it stays inside the
governed project with the same masking policies as the gold table.
