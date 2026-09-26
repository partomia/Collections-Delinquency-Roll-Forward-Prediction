# Build plan — Collections: Delinquency Roll-Forward Prediction on Cloudera AI

Every day, score each SMA-0 loan (1–30 DPD) for the probability that it rolls
into SMA-1 (31+ DPD) within 30 days, rank the book by
`priority = p_roll × overdue_amount`, and hand the collections team a
capacity-aware call list. The model is **TabICL v2** (in-context learning, no
training): `fit` stores labelled history from a gold Iceberg table, learning
happens inside `predict_proba`.

Demo of a governed data + scoring pipeline, not a validated credit model.
The model only ranks accounts; contact rules follow the RBI fair practices code.

## Platform mapping

Same CDP environment as `partomia/ALM-IRRBB-CASA-Behavioural-Forecasting` and
`partomia/insurance-claim-approval` (same CDE vcluster, CDW Impala VW, CAI workbench).

| Layer | Service | What runs there |
|---|---|---|
| Ingest + medallion | CDE (Spark 3, Iceberg) | generate bronze, validate, silver daily, gold features + labels |
| Orchestration | CDE Airflow | daily DAG chains the Spark jobs, then triggers the CAI scoring job |
| Scoring | CAI Job | holdout check, save context, score today's SMA-0 book, write call list |
| On-demand / what-if | CAI Model Deployment | `predict.py`, scores loans sent by the app or dialler |
| SQL + reports | CDW Impala (Hue) | call-list reports, roll-rate trends, Iceberg time travel |
| Demo UI | CAI Application (Streamlit) | call list, capacity cut-offs, holdout capture, what-if, outcomes |

Names:

- Databases: `rsingh_collections_delinquency_prediction_{bronze,silver,gold,ref}`
  (requested as `rsingh_Collections_Delinquency_Prediction_*`; Hive/Impala store
  names lowercase). Prefix set once in `config/collections.yaml`, `--db-prefix`
  / `DB_PREFIX` on the CDE side.
- CDE resources: `rsingh-coll-dlq-*` (repository, python-env, jobs, DAG).
- CAI project: `collections-tabicl`; model `collections-roll-scorer`;
  job `collections-daily-score`; application `Collections Call List`.

## Data flow

```
CDE  generate_loan_bronze   -> bronze.loan_master          (loan × static attributes)
                               bronze.loan_daily_ledger    (loan × day: due, paid, dpd, overdue)
                               bronze.nach_presentations   (loan × presentation: bounced flag, reason)
                               bronze.dialler_contacts     (loan × attempt: reached, PTP date/amount, PTP kept)
                               bronze.casa_credits         (loan × credit: salary flag, amount)
                               bronze.bureau_snapshot      (loan × month: bureau score)
                               ref.product_map             (product_code → name, tenor, EMI band)
CDE  validate_bronze        -> gate (fails the DAG: nulls, dup keys, dpd monotonic, dates)
CDE  build_silver_daily     -> silver.loan_daily_status    (clean ledger + RBI class SMA-0/1/2/NPA)
                               silver.loan_events_daily    (bounces, contacts, PTPs, salary credits per day)
CDE  build_gold_features    -> gold.collections_features   (loan × snapshot_date, SMA-0 only; MERGE;
                                                            label = dpd >= 31 in (d, d+30], NULL if d > today-30)
CAI  daily_score            -> gold.collections_call_list  (run_date × loan: p_roll, priority, treatment)
                               gold.collections_holdout    (run_date: AUC, capture@10/40%, lift by decile)
                               gold.collections_model_run  (run_date: tabicl version, context rows,
                                                            input snapshot id, cut-offs used, trigger)
                               models/coll_context.parquet (context for the endpoint)
App  outcome capture        -> bronze.collector_outcomes   (reached / PTP / paid; feeds tomorrow's features)
CDW  Hue reports, time travel     CAI  Streamlit app + model endpoint
```

Unlike the design doc's `createOrReplace`, the call list is appended per
`run_date` (DELETE + INSERT for a rerun), so yesterday's list stays queryable
and Iceberg time travel shows what the collector saw on any day.

## Gold feature table (from the design doc)

`gold.collections_features`, `PARTITIONED BY SPEC (month(snapshot_date))`, Iceberg v2:

`loan_id, snapshot_date, product_code, dpd_now, overdue_amount, emi_amount,
overdue_to_emi, months_on_book, max_dpd_12m, nach_bounces_6m, broken_ptp_3m,
contacts_reached_30d, salary_credit_last_30d, bureau_score, rolled_to_sma1_30d`

All numeric, encoded in Spark. Snapshots: month-end for 12 months of history
(the labelled context) plus the latest day (the population to score).

## Synthetic data (demo)

- ~200k active retail loans across products (1 personal, 2 two-wheeler,
  3 credit card, 4 home, 5 gold/MSME), ~18 months of daily ledger.
- A hidden per-loan stress factor drives NACH bounces, broken PTPs, missing
  salary credits and bureau drift, so the features carry real signal; a
  salary credit or a reached contact lowers the chance of rolling.
- Target: SMA-0 ≈ 8–10% of the book per snapshot (~15–20k loans), roll rate
  to SMA-1 ≈ 12–18%, giving ~150–200k labelled rows over 12 snapshots.
- Deterministic by seed and prefix-stable: a later as-of date only adds days,
  like real daily loads. Home Credit instalments stay an option, not the default.

## Layout (mirrors the ALM project)

```
coll/          shared logic: config, features, TabICL wrapper, prioritise, holdout, storage, scoring
cde/jobs/      Spark jobs (self-contained, PySpark + stdlib only)
cde/dags/      Airflow DAG (daily)
cde/scripts/   deploy_jobs.sh, deploy_dag.sh, backfill_drill.sh
cai/jobs/      daily_score.py, backfill_history.py
cai/model/     predict.py (endpoint), test_endpoint.py
app/           Streamlit app + CAI launcher (run.py)
config/        collections.yaml (names, storage, model), policy.yaml (treatment cut-offs, context size)
sql/           Hue reports and time-travel queries
docs/          demo runbook
scripts/       run_cde_local.py (CDE jobs on a laptop with local Spark + Iceberg)
tests/         unit tests with a stub classifier (no checkpoint download)
```

## Phases

- [ ] **0. Scaffold** — layout, `config/collections.yaml` + `config/policy.yaml`,
  requirements (tabicl pinned, CPU torch wheels), `cdsw-build.sh`, `.env.example`, venv.
- [ ] **1. Bronze** — synthetic loan book generator (CDE Spark job), ref product map.
- [ ] **2. Silver + gold** — validation gate, daily status with RBI SMA classes,
  event roll-ups, gold features + 30-day forward label, MERGE per snapshot.
  Verified locally on Spark + Iceberg (`scripts/run_cde_local.py`).
- [ ] **3. Core logic** (`coll/`) — feature list, TabICL wrapper (HF download or
  local `model_path` for air-gapped), `prioritise` with cut-offs from policy,
  holdout metrics (AUC, capture of rolls in top 10% / 40%, decile lift),
  storage backends (Impala / parquet). Unit tests with a stub model.
- [ ] **4. Daily job** — `cai/jobs/daily_score.py`: holdout on the latest
  labelled month, context = most recent N labelled rows, save
  `models/coll_context.parquet`, score today, write call list + holdout + run
  record. `backfill_history.py` for a few past run dates (history for the app).
- [ ] **5. Model endpoint** — `predict.py` (`{"loans": [...]}` → p_roll,
  priority_score), test script with the what-if example from the doc.
- [ ] **6. Streamlit app** — call list with treatment bands, capacity sliders
  (top %, next %) that re-band live without re-scoring, holdout capture chart
  ("top 10% of calls catch X% of rolls"), roll-rate trend, single-loan what-if
  via the endpoint, collector outcome capture written back to Iceberg.
- [ ] **7. Orchestration + docs** — daily Airflow DAG (CDE Spark → CAI job via
  API v2), CDE deploy/backfill scripts, Hue SQL, README, demo runbook.

## Method (demo policy, `config/policy.yaml`)

- Label: loan in SMA-0 on `d` reaches dpd ≥ 31 on any day in `(d, d+30]`.
  Only snapshots with `d <= today − 30` are labelled, so no leakage into context.
- Context: most recent 20,000 labelled rows (CPU); raise to 50–100k with a GPU.
  Holdout: context from older months, test on the latest labelled month.
- Priority = `p_roll × overdue_amount`; treatment by percentile across the book:
  top 10% `AGENT_CALL_TODAY`, next 30% `AGENT_OR_IVR`, rest `SMS_REMINDER`.
  Cut-offs are business settings (policy file / app sliders), not model settings.
- Trust number: capture of actual rolls in the top 10% of calls, next to AUC.

## Decisions

1. CAI reads and writes Iceberg through CDW Impala with `impyla`, as in the ALM
   project. Where the design doc and the ALM project differ, the ALM pattern wins.
2. Weekly snapshots (every Friday, Sat–Fri weeks as in ALM) plus the as-of date.
3. GPU is available in CAI: context defaults to 50,000 rows on GPU, 10,000 on CPU.
