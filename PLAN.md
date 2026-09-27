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
| Ingest + medallion | CDE (Spark 3, Iceberg) | generate bronze, validate, silver entities, gold features + labels |
| Orchestration | CDE Airflow | daily DAG chains the Spark jobs, then triggers the CAI scoring job |
| Scoring | CAI Job | holdout check, context selection, score today's SMA-0 book, write call list |
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

## Decisions

1. CAI reads and writes Iceberg through CDW Impala with `impyla`, as in the ALM
   project. Where the design doc and the ALM project differ, the ALM pattern wins.
2. Weekly snapshots (every Friday) plus the as-of date.
3. GPU is available in CAI: context defaults to 50,000 rows on GPU, 10,000 on CPU.
4. Call list, holdout and model run are kept per run date (DELETE + INSERT on a
   rerun), not replaced, so past call lists stay queryable.
5. Every run pins the gold Iceberg snapshot it reads and records the context
   window; the endpoint rebuilds that exact context at start-up instead of
   relying on a file copied at model build time.
6. Contact history includes collector outcomes from the app (closing the loop).

## Phases

- [x] **0. Scaffold** — layout, config, requirements (tabicl 2.2.0, pinned checkpoint), venv.
- [x] **1. Bronze** — synthetic loan book (hidden stress, shocks, NACH, dialler, salary, bureau), prefix-stable.
- [x] **2. Silver + gold** — validation gate, deduplicated entities with PTP kept/broken,
  weekly SMA-0 features with the 30-day forward label, MERGE per load. Verified
  locally against an independent Python computation (exact match).
- [x] **3. Core logic** (`coll/`) — TabICL wrapper, priority bands + risk signals,
  holdout (capture vs DPD baseline), Impala / parquet storage, daily pipeline. Unit tests.
- [x] **4. Daily job** — `daily_score.py`, `backfill_history.py` (labels known on each run date only).
- [x] **5. Model endpoint** — `predict.py` with what-if, context rebuilt from the run's snapshot, KV cache.
- [x] **6. Streamlit app** — call list with capacity sliders, model trust, book trend,
  what-if, collector outcomes, lineage; CAI launcher + Dockerfile. Headless app test.
- [x] **7. Orchestration + docs** — daily Airflow DAG, CDE deploy/backfill scripts, Hue SQL, README, runbook.
- [x] **8. On Cloudera** — CDE jobs and the daily DAG on the vcluster (scheduled run
  27 Sep 00:30 UTC succeeded), CDW checks and Hue queries; CAI project `collections-tabicl`
  on an NVIDIA L4, job `collections-daily-score` triggered by Airflow, model
  `collections-roll-scorer`, application `Collections Call List`; ten run dates of history
  (31 Jul - 26 Sep). Collector outcomes from the app not yet exercised on the platform.

## Paused (27 Sep 2026) — possible next steps

- Restart the model endpoint automatically after the daily run (an extra DAG step
  calling the CAI API), so what-ifs always use the latest context.
- Save a collector outcome in the app and check it reaches the next day's silver contact history.
- Try a GPU context larger than 50,000 rows and compare holdout capture.
