# Build plan — Collections: Delinquency Roll-Forward Prediction on Cloudera AI

> Picking this up in a new session? Read `docs/PROJECT_LOG.md` first — a
> dated, timestamped history of every build step and platform change, kept
> current enough to fully recover context without replaying this conversation.

Every day, score each SMA-0 loan (1–30 DPD) for the probability that it rolls
into SMA-1 (31+ DPD) within 30 days, rank the book by
`priority = p_roll × overdue_amount`, and hand the collections team a
capacity-aware call list. The model is **TabICL v2** (in-context learning, no
training): `fit` stores labelled history from a gold Iceberg table, learning
happens inside `predict_proba`.

Demo of a governed data + scoring pipeline, not a validated credit model.
The model only ranks accounts; contact rules follow the RBI fair practices code.

## Platform mapping

Built on go01 (26-27 Sep 2026; left as it was), moved to the **federal**
environment from 3 Oct 2026, the same way as Customer-Churn-Prediction,
Mule-Account-Identifier and Spend-Analytics: CDE vcluster `bxjjm2cr` (shared by
those projects), CDW Impala
`coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site:443`
(`cliservice`, LDAP workload user), CAI workbench
`https://federal-cml.federal.dp5i-5vkq.cloudera.site`. Federal Iceberg tables live
under `s3a://federal-buk-574bcea0/data/warehouse/tablespace/external/hive/`; the
databases were rebuilt from scratch with the generator, not copied.

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
- CAI (federal, all in `ci/cai_jobs.py`): project `rsingh-coll-dlq`; jobs
  `rsingh-coll-dlq-daily-score` and `rsingh-coll-dlq-sync-code`; model
  `rsingh-coll-dlq-roll-scorer`; application `rsingh-coll-dlq-call-list`.
  On go01: project `collections-tabicl`, model `collections-roll-scorer`, job
  `collections-daily-score`, application `Collections Call List`.
- Schedule: daily 00:30 UTC. Neighbours on the shared vcluster: Mule 20:30,
  Spend 21:00, Churn 22:00 UTC (`cde job list`, 3 Oct 2026).

## Decisions

1. CAI reads and writes Iceberg through CDW Impala with `impyla`, as in the ALM
   project. Where the design doc and the ALM project differ, the ALM pattern wins.
2. Weekly snapshots (every Friday) plus the as-of date.
3. Context defaults to 50,000 rows on GPU, 10,000 on CPU (go01 had a GPU; federal
   does not, decision 10).
4. Call list, holdout and model run are kept per run date (DELETE + INSERT on a
   rerun), not replaced, so past call lists stay queryable.
5. Every run pins the gold Iceberg snapshot it reads and records the context
   window; the endpoint rebuilds that exact context at start-up instead of
   relying on a file copied at model build time.
6. Contact history includes collector outcomes from the app (closing the loop).
7. **CAI is set up over the API v2 from the laptop**, as in Churn and Spend.
   `ci/cai_jobs.py` holds every CAI resource (60-min job timeout, not the UI's 15),
   `ci/setup_cai.py` creates or adopts them by name and corrects drift
   (`--dry-run`, `--no-serving` until a run is published, `--sync`), and
   `cde/scripts/set_airflow_variables.py` sets only the four `COLL_CAI_*` Variables
   through the vcluster's Airflow API. Names carry the `rsingh-coll-dlq` prefix.
8. **`rsingh-coll-dlq-sync-code` replaces `git pull` in a session, and GitHub checks
   each push on CAI.** On a push to main, `cai-pipeline` (after `test`) runs sync-code
   to the pushed commit, then the daily job with `COLL_DRY_RUN=1`: real model,
   holdout and book scoring, no table written. A notice and green until the
   `CAI_URL` secret is set. The daily DAG publishes.
9. **The DAG registers paused** (`is_paused_upon_creation=True`): an unpaused
   registration, or unpausing later, runs the latest closed interval at once. Its
   CAI poll survives a dropped connection (RequestException and 5xx retried, up to
   10 polls in a row; 4xx re-raised), since a task retry would start a second CAI run.
10. **CAI runs on CPU on federal.** GPUs cannot be scheduled from these projects (in
   the churn move a 1-GPU run, and 8 vCPU / 32 GB, sat in `ENGINE_SCHEDULING`), so the
   daily job and the model are 4 vCPU / 16 GB with no GPU. This project has no model
   family switch: TabICL picks `cuda`, then `mps`, then `cpu`, and without CUDA the
   context is `rows_cpu` = 10,000 rows (go01: 50,000 on an L4). First federal run
   (2026-10-02, 10.2 min on 4 vCPU): AUC 0.850, top 10% capture 32.2% (DPD 29.8%),
   top 40% 80.4% (DPD 71.7%), value capture 55.2%; inside go01's nine-run ranges
   (AUC 0.85-0.87, 30-37%, 80-82%, 52-56%), AUC at the low end with the 5x smaller
   context. The context stays at 10,000 rows on CPU.
11. **CDE jobs are sized for the shared federal queue** (27 vCPU / ~110 GB): a 2-core /
   4 GB driver and 4-core / 8 GB executors, 1 min / 2 initial / 4 max, all overridable
   in `deploy_jobs.sh`. go01's 4-core driver with 4 initial executors is rejected
   before it starts ("cannot fit application").
12. **The backfill scores each date in its own process.** Two federal backfill runs
   stopped mid-date and still reported `ENGINE_SUCCEEDED`; on the laptop one process
   peaks at 9.3-10.4 GiB and holds memory between dates, so the 16 GB engine most
   likely ran out. `backfill_history.py` now runs `daily_score.py` per date; memory is
   released each time and a killed date fails the job by name. The re-run wrote every
   date (527-678 s each).

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
- [x] **9. Scripted Cloudera setup** (decisions 7-9): `ci/cai_jobs.py`, `ci/setup_cai.py`,
  `ci/trigger_cai_pipeline.py`, `cai/jobs/sync_code.py`, `cde/scripts/set_airflow_variables.py`,
  `.github/workflows/ci.yml` (`test` + `cai-pipeline`), DAG paused on creation with a
  tolerant CAI poll, federal Impala host and CDE sizing (decisions 10-11).
- [ ] **10. Federal, from scratch**: CDE jobs and the chain for one as_of, CAI project and
  first CPU run, model and app, 8-week backfill, Airflow Variables, DAG (paused, then
  unpaused), first scheduled run, GitHub -> CAI check. Done on 3 Oct except the first
  scheduled run (4 Oct 00:30 UTC); the unpause run (2026-10-02 interval) succeeded in
  30 min, CI run 37118112471 green.

## Open (federal, 3 Oct 2026)

- First scheduled DAG run, 4 Oct 00:30 UTC: not yet observed.
- A CAI engine that is killed can be reported as `ENGINE_SUCCEEDED` (decision 12).
  The daily job is one date (peak ~9-10 GiB of 16 GB), but Airflow would not notice a
  silent end; a check that the run date reached `collections_model_run` would.
- GPU group `mlgpu-c63b` (g5.12xlarge, max 1) exists but jobs here cannot get a GPU.
- CPU group `mlcpu-d697` raised from max 10 to 4-14 for the model deployment; the
  modify call returned a 500 but applied.
- Collector outcomes from the app not yet exercised on federal.
- Three older commits carry `Co-Authored-By` trailers; left as they are (history
  already pushed).

## Paused (27 Sep 2026) — possible next steps

- Restart the model endpoint automatically after the daily run (an extra DAG step
  calling the CAI API), so what-ifs always use the latest context.
- Save a collector outcome in the app and check it reaches the next day's silver contact history.
- Try a GPU context larger than 50,000 rows and compare holdout capture.
- [x] **Data quality** (27 Sep 2026): the `validate_bronze` gate is retired; `cde/jobs/dq_check.py`
  runs Great Expectations suites for all three layers (bronze/silver/gold), one job reused
  three times in the DAG (`generate → dq_bronze → silver → dq_silver → gold → dq_gold → CAI`),
  `--layer` set per task via `overrides`. Critical failures exit 1 (DAG stops, yesterday's call
  list stands); warnings are recorded and the run continues. Every check, pass or fail, is
  appended to Iceberg `ref.dq_results` with the checked table's snapshot id and a
  `pipeline_run` tag (`{{ run_id }}`) that groups one run's three layers. Impala summary
  queries added (`sql/reports.sql` #9-11: latest run by layer, critical failures, pass-rate
  trend). Verified end-to-end against the local Iceberg warehouse (`scripts/run_cde_local.py dq`)
  and then live on the CDE vcluster: `rsingh-coll-dlq-python-env` rebuilt with
  `great_expectations==1.23.2`, `rsingh-coll-dlq-validate-bronze` job deleted, the new
  `rsingh-coll-dlq-dq-check` job created (4-core/8 GB driver, 4-core/8 GB executors, same as
  the other jobs), DAG redeployed. Ran the full chain manually for `as_of=2026-09-27`
  (`generate → dq_bronze → silver → dq_silver → gold → dq_gold`, all `succeeded`); `ref.dq_results`
  confirms 0 critical failures and one correctly-flagged warning (latest labelled roll rate 46.8%
  on a small n=1306 sample, above the 10-40% band) that recorded and did not block the run. The
  next *scheduled* DAG run (00:30 UTC) will exercise the new gate through Airflow itself. Not
  done: write-audit-publish on gold with Iceberg branches (check branch support on the vcluster
  first) — still optional/future.
- [x] **Cloudera Data Visualization** (3 Oct 2026, federal): two dashboards as code
  (`dataviz/build_dashboard.py`, `docs/DATAVIZ.md`) in the federal CDW instance shared with
  the Spend project, in this project's workspace `rsingh-coll-dlq`, through our own
  connection `rsingh-coll-dlq-impala` (public endpoint, LDAP). Six views in
  `rsingh_collections_delinquency_prediction_report`; no table changes. Built in the UI
  is no longer needed: the export file is generated and imported by the script.
