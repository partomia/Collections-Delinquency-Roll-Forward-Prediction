# Project Log

Purpose: recover full context in a new session without replaying any prior
conversation. Chronological, most-recent-day-last. Times are IST (UTC+5:30)
unless marked UTC — the platform (CDE CLI, Impala) reports UTC; this repo's
git commits are already in IST. Cross-refs: `PLAN.md` (living status and next
steps), `README.md` (architecture and how to run), `docs/DEMO_RUNBOOK.md`
(demo script).

## 2026-09-26 — Day 1: initial build (Cursor, Opus 5.5)

| Time | Commit | What |
|---|---|---|
| 12:29 | `ca98177` | Initial commit |
| 18:52 | `a6ddf09` | Phase 0: scaffold, config, requirements, plan |
| 19:01 | `f3af7c3` | Phases 1-2: synthetic loan book bronze, `validate_bronze` gate, deduplicated silver entities, gold weekly features + 30-day roll label |
| 19:16 | `6d8775b` | Phases 3-4: `coll/` package (TabICL wrapper, priority bands, holdout, storage, daily pipeline), CAI daily/backfill jobs, unit tests |
| 19:20 | `797c847` | Phase 5: model endpoint — what-if, context rebuilt from the run's Iceberg snapshot |
| 19:22 | `ac9c273` | Phase 6: Streamlit app (call list, model trust, what-if, collector outcomes, lineage), CAI launcher, Dockerfile |
| 19:23 | `b55b006` | Phase 7a: daily Airflow DAG with CAI trigger, CDE deploy/backfill scripts |
| 19:30 | `9c3b08e` | Phase 7b: README, demo runbook, Hue report queries, plan status |
| 20:08 | `90e45be` | Fix: Impala reserved word (`rows`) in the holdout table's column name |
| 20:49 | `9bf580a` | Fix: CAI scripts ignore unknown args (Jupyter-kernel job runtime passes `-f`) |
| 22:32 | `c197fc5` | Fix: endpoint uses the latest *run date*, not the latest *write* (backfills write older dates later) |
| 22:57 | `a7aeb75` | Fix: DAG `start_date` moved into the past so manual triggers actually run tasks |
| 23:03 | `bb9106c` | CDE jobs sized 4-core/8 GB driver, 2-8 executors of 4-core/8 GB |
| 23:41 | `cea2c46` | App: readable treatment labels in the call-list table |

Sometime after 23:41 IST (no exact timestamp logged — this happened directly
against the Cloudera platform, not as a commit), **Phase 8** landed on the
real environment: CDE repository/jobs/DAG created on the vcluster, CDW checks
via Hue, CAI project `collections-tabicl` set up on an NVIDIA L4 with job
`collections-daily-score`, model `collections-roll-scorer` deployed, app
`Collections Call List` deployed, and ten run dates of history backfilled
(31 Jul – 26 Sep).

## 2026-09-27 — Day 2

### Morning: docs, plan paused, next steps drafted (Cursor)

| Time | Commit | What |
|---|---|---|
| 00:30 UTC / 06:00 IST | — | Regularly scheduled DAG run (`as_of=2026-09-26`) succeeds end-to-end on the vcluster, still on the old `validate_bronze` gate |
| 11:21 | `4603335` | Docs: deployment learnings, timings, daily operation, troubleshooting, demo prep; plan paused |
| 13:41 | `5c9562b` | Plan: Great Expectations data-quality design and Cloudera Data Visualization dashboard drafted as next steps; `COLL_CDV_*` placeholders added to `.env.example` |
| ~13:58 | *(uncommitted)* | Cursor agent (Opus 5.5) writes `cde/jobs/dq_check.py` (444 lines: GX suites for bronze/silver/gold, results to `ref.dq_results`) and a diff to `scripts/run_cde_local.py` (three `dq_*` stages). Left **uncommitted** — Cursor's plan/quota runs out before this is wired into the DAG or deployed. |

### Afternoon: continued in Claude Code — review, fix, wire up, deploy live

Picked up from the uncommitted Cursor diff (`git status` showed `scripts/run_cde_local.py` modified and `cde/jobs/dq_check.py` untracked).

1. Reviewed `dq_check.py` and the `run_cde_local.py` diff — design: per-check severity (`critical` stops the DAG, `warning` records and continues), one `ref.dq_results` row per expectation with the checked table's Iceberg snapshot id.
2. Found and fixed a real bug in `run_cde_local.py`'s `load_job()`: it loaded job files via `importlib` without registering the module in `sys.modules`, which broke `dq_check.py`'s `@dataclass` (field-type resolution needs `sys.modules[cls.__module__]`). The naive fix (always register) then broke `generate_loan_bronze.py`'s Spark UDFs — once the module was in `sys.modules`, cloudpickle serialized its closures *by reference* instead of *by value*, and Spark workers couldn't re-import a synthetic module. Final fix: register only for the duration of `exec_module`, then remove it before the job runs.
3. Verified all three `dq_*` stages, then the full `all` sequence, locally against `data/warehouse` (Hadoop/Iceberg catalog) — 0 critical failures across bronze/silver/gold; `pytest` (26 tests) stayed green throughout.
4. Wired the DQ gate everywhere `validate_bronze` used to be:
   - `cde/dags/collections_dag.py` — `validate_bronze` task replaced by three tasks (`dq_bronze`, `dq_silver`, `dq_gold`) sharing one CDE job (`dq-check`); `--layer` and `--pipeline-run {{ run_id }}` set per task via `overrides`.
   - `cde/scripts/deploy_jobs.sh` — creates the `dq-check` job instead of `validate-bronze`.
   - `cde/scripts/backfill_drill.sh` — runs `dq-check` after each of bronze/silver/gold in the backfill loop.
   - `cde/jobs/validate_bronze.py` deleted (`dq_check.py`'s bronze checks are a strict superset).
   - `great_expectations==1.23.2` pinned in `cde/resources/requirements.txt` and `requirements-dev.txt`.
   - `README.md` (architecture diagram, table list, timings), `sql/reports.sql` (+3 Impala summary queries over `ref.dq_results`), `PLAN.md` updated.
5. **14:36** — commit `3236fa8` "CDE: replace validate_bronze with a Great Expectations dq_check gate"; pushed to `origin/main`.
6. **14:36–14:44** — `cde repository sync`; rebuilt `rsingh-coll-dlq-python-env` with `great_expectations` added (took ~6 min — longer than the "1-3 min" the deploy script's comment expects, since GX pulls in real dependencies where the env was previously empty).
7. Ran `cde/scripts/deploy_jobs.sh`: recreated the 4 existing jobs plus the new `rsingh-coll-dlq-dq-check` (4-core/8 GB driver, 4-core/8 GB executors — same sizing as the rest). The script doesn't delete jobs it no longer creates, so manually deleted the now-orphaned `rsingh-coll-dlq-validate-bronze`. Ran `cde/scripts/deploy_dag.sh` to redeploy the DAG.
8. **09:22:44–09:39:51 UTC** (14:52:44–15:09:51 IST) — ran the full CDE chain **manually** (`cde job run`, bypassing Airflow) for `as_of=2026-09-27`, to validate on the live vcluster before touching the DAG:

   | Run | Job | Result |
   |---|---|---|
   | 2284 | generate-loan-bronze | succeeded |
   | 2285 | dq-check `--layer bronze` | succeeded — 55 checks, 0 critical failures |
   | 2286 | build-silver | succeeded |
   | 2287 | dq-check `--layer silver` | succeeded — 25 checks, 0 critical failures |
   | 2288 | build-gold-features | succeeded |
   | 2289 | dq-check `--layer gold` | succeeded — 24 checks, 0 critical failures |

   Verified directly against Impala (`ref.dq_results`), not just job exit
   codes: `cde run logs` truncates/garbles output on this vcluster once GX's
   `tqdm` progress bars write `\r`-heavy lines to stdout, so the log tail is
   not reliable evidence — the Iceberg table is the source of truth.
9. **15:11** — commit `f0354e1` "Plan: dq_check deployed and verified live on the CDE vcluster"; pushed and synced.

### Evening: prefix question, Airflow-captured orchestration

10. User asked whether Iceberg tables/databases were missing the `rsingh`
    prefix. Checked live via Impala: every table (`dq_results`, `product_map`,
    `loan_master`, `collections_features`, …) follows the same convention —
    the prefix lives on the **database**
    (`rsingh_collections_delinquency_prediction_{bronze,silver,gold,ref}`),
    never repeated on the table name. `dq_results` matches the pre-existing
    schema exactly (same S3 path pattern, same `SHOW CREATE TABLE` shape).
    User confirmed they had been looking at something else — no change made.
11. User pointed out the DQ runs weren't visible in the Airflow UI — correct,
    because step 8 used direct `cde job run` calls, which bypass Airflow
    entirely. Triggered the actual DAG instead:
    - **14:43:22–15:04:21 UTC** (20:13:22–20:34:21 IST) —
      `cde job run --name rsingh-coll-dlq-orchestration --config-json '{"as_of": "2026-09-27"}'`
      → DAG run id **2290** (`dagRunID=cde-job-run-2290`), **succeeded**,
      ~21 min including the CAI step.
    - Per-task CDE job runs confirmed, in the correct DAG order:

      | Run | Task | Window (UTC) |
      |---|---|---|
      | 2291 | generate_loan_bronze | 14:46:21–14:51:01 |
      | 2292 | dq_bronze | 14:51:17–14:53:36 |
      | 2293 | build_silver | 14:53:53–14:55:31 |
      | 2294 | dq_silver | 14:55:48–14:58:11 |
      | 2295 | build_gold_features | 14:58:24–14:59:41 |
      | 2296 | dq_gold | 14:59:59–15:01:31 |

    - `cde run fg-status` (per-task Airflow status) is **not available** on
      this vcluster ("fine-grained job status service not initialized"), and
      `cde run ui` doesn't support opening Airflow directly from the CLI —
      the table above was reconstructed from each underlying CDE job's own
      run history, filtered by start time.
    - Confirmed in `ref.dq_results`: `pipeline_run = 'cde-job-run-2290'`
      groups all three layers (bronze 55 checks / silver 25 checks / gold 24
      checks), 0 critical failures, one correctly-flagged warning (gold:
      "latest labelled roll rate within 10-40%" — actual 46.8% on snapshot
      `2026-08-28`, n=1306, a real small-sample signal, not a bug) — same
      result as the manual run in step 8, this time properly captured as
      Airflow task instances.
12. **23:06** — this log written (`docs/PROJECT_LOG.md`); `README.md` and
    `docs/DEMO_RUNBOOK.md` corrected to match (seven DAG tasks, not five; the
    `dq_*` gates named explicitly; ~20 min measured run time; pointers to
    this log added).

## 2026-10-03 — Move to the federal environment (Cursor, Opus 5.5)

Moved end to end from go01 to federal, the same way as Customer-Churn-Prediction
(its PLAN decisions 14-18, PROJECT_LOG phases 9-10). go01 resources were left as they were.

### Phase 9: scripted Cloudera setup, federal config (code)

- Read-only checks on federal first: `cde job list` shows the scheduled DAGs Mule
  20:30, Spend 21:00, Churn 22:00 UTC (gdl paused), so 00:30 UTC stays; no running
  runs; runtime `ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5`
  `ENABLED` in the CAI API; no `rsingh-coll-dlq` project; Impala LDAP login works
  and no `rsingh_coll*` database exists.
- `config/collections.yaml`: Impala host `coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site`.
- `deploy_jobs.sh`: 2-core / 4 GB driver, 4-core / 8 GB executors, 1 / 2 / 4, each
  overridable by environment variable; exits if the python-env is not ready.
- DAG: `is_paused_upon_creation=True`; the CAI poll retries RequestException and 5xx
  (up to 10 in a row), re-raises 4xx; deadline 90 min.
- New: `ci/cai_jobs.py`, `ci/setup_cai.py`, `ci/trigger_cai_pipeline.py`,
  `cai/jobs/sync_code.py`, `cde/scripts/set_airflow_variables.py`,
  `.github/workflows/ci.yml` (`test` + `cai-pipeline`), `requirements-ci.txt`,
  `COLL_DRY_RUN` in `daily_score.py`, `COLL_BACKFILL_WEEKS` in `backfill_history.py`
  and a manual `rsingh-coll-dlq-backfill-history` job, `tests/test_ci_trigger.py`,
  `tests/test_orchestration.py`.
- CI steps reproduced locally in scratch folders: `run_cde_local.py all --loans 5000`
  in 50 s, then the stub scoring. It found that `export` failed with a
  `--parquet-dir` outside the repo (printing a relative path); fixed.
- TabICL on the laptop CPU (`COLL_MODEL_DEVICE=cpu`), 10,000-row context: fit 6.1 s,
  61 rows/s scoring.
- Commit `029fc12`, pushed.

### Phase 10: federal, from scratch (times UTC)

- 07:32-07:39 `deploy_jobs.sh`: repository `rsingh-coll-dlq-pipeline`, python-env
  `rsingh-coll-dlq-python-env` (built in ~6 min) and the four jobs, 395 s in all.
- 07:47-08:03 chain for as_of 2026-10-02, run by run (`cde run describe` polled):
  generate 249 (235 s), dq bronze 250 (158 s), silver 251 (112 s), dq silver 252
  (159 s), gold 253 (108 s), dq gold 254 (102 s); 16.5 min in all, no queue rejection
  at 2-core / 4 GB driver, 1 / 2 / 4 executors.
- `ref.dq_results` for `manual-2026-10-02`: bronze 41 critical + 14 warning, silver
  24 + 1, gold 12 + 12; 104 checks, 0 critical failures; one warning, latest labelled
  roll rate 46.8% on 2026-08-28 (n=1,306), the same as on go01 (the generator is
  prefix-stable).
- Impala after the chain: bronze `loan_master` 106,556, `loan_instalments` 2,042,966,
  `nach_presentations` 1,701,525, `dialler_contacts` 780,769, `casa_credits` 976,303,
  `bureau_snapshot` 2,140,067; silver `loan` 106,556, `instalment` 2,042,568,
  `collection_contact` 780,575; gold `collections_features` 187,942 rows, 79 snapshots
  (2025-04-04 .. 2026-10-02), labelled to 2026-08-28, roll rate 24.5%, 756 SMA-0
  loans on 2026-10-02. Location
  `s3a://federal-buk-574bcea0/data/warehouse/tablespace/external/hive/rsingh_collections_delinquency_prediction_gold.db/collections_features`.
- `setup_cai.py --no-serving --sync`: project `rsingh-coll-dlq` (`z436-4kbz-4uvi-q3sh`)
  cloned; jobs sync-code `xgby-791p-l0oz-4p3p`, daily-score `tdi8-nf35-teoc-8mv3`,
  backfill-history `t0fq-8rwv-2oj1-wch8`. Two calls right after the project was
  created failed with `SSL: UNEXPECTED_EOF_WHILE_READING` (a job POST, then the
  environment PATCH); re-running the idempotent script converged (three runs, the
  last two "up to date"). The laptop's polls kept dropping too (URLError,
  RemoteDisconnected), so `ci/trigger_cai_pipeline.py` now retries GETs (10 tries,
  4xx not retried); POSTs are never retried (a second run).
- Sync-code run: scheduled 07:51:05, running 07:54:16, succeeded 08:06:05 (11.8 min,
  requirements installed).
- First daily run, `triggered_by=manual`, run date 2026-10-02: scheduling 13 s
  (4 vCPU / 16 GB), running 08:08:01-08:18:12 (10.2 min; pipeline 601.9 s). Run
  `20261002-488d4d04`, device cpu, context 10,000 rows (2025-08-29 .. 2026-08-28),
  holdout 2026-08-07 .. 08-28 on 9,638 loans with a 10,000-row context: roll rate
  24.4%, AUC 0.850, top 10% catch 32.2% of rolls (DPD alone 29.8%), top 40% 80.4%
  (DPD alone 71.7%), top 10% catch 55.2% of rolled overdue. 756 scored: 75
  AGENT_CALL_TODAY, 227 AGENT_OR_IVR, 454 SMS_REMINDER; 10 holdout deciles.
  Against go01 (L4, 50,000 rows, nine runs): AUC 0.85-0.87, top 10% 30-37% (DPD
  28-30%), top 40% 80-82% (DPD 72-74%), value 52-56%. Federal sits inside the go01
  ranges, AUC at the low end: same data, a 5x smaller context on CPU.
- `setup_cai.py` with `COLL_ENDPOINT_API_KEY` = the CAI API key (as in churn): model
  `rsingh-coll-dlq-roll-scorer` (`5f90c5d1-f165-4fb9-9a75-b209934e64b9`, build
  `df8e8680-f1a7-43a3-9320-5676eda7722e`, 4 vCPU / 16 GB, no GPU), endpoint URL and
  keys in the project environment, application `rsingh-coll-dlq-call-list`
  (`wloe-l1ll-u2bm-31c0`).
- Backfill-history run `lzwh60yx5d1984jm` started (8 weeks; one week is about one
  daily run, ~10 min).
- `set_airflow_variables.py`: the four `COLL_CAI_*` Variables created; a second run
  reports them up to date. `deploy_dag.sh`: `rsingh-coll-dlq-orchestration` created,
  schedule `30 0 * * *`, `paused: true`, `pausedUponCreation: true`, no runs.

## Recovery cheat-sheet

**Environment.** Federal from 3 Oct 2026 (go01 before, left as it was): see
`PLAN.md` platform mapping. Iceberg on
`s3a://federal-buk-574bcea0/data/warehouse/tablespace/external/hive/<db>.db/<table>`
(go01: `s3a://go01-demo/warehouse/tablespace/external/hive/`).

**Naming.** `DB_PREFIX = rsingh_collections_delinquency_prediction`; four
Iceberg databases (`_bronze`, `_silver`, `_gold`, `_ref`). Tables
inside a database are **not** re-prefixed (`dq_results`, `product_map`,
`loan_master`, `collections_features`, …) — the prefix lives on the database
only, consistently, project-wide. CDE resources: `rsingh-coll-dlq-*`
(repository `rsingh-coll-dlq-pipeline`, python-env
`rsingh-coll-dlq-python-env`, jobs `rsingh-coll-dlq-{generate-loan-bronze,
dq-check, build-silver, build-gold-features}`, DAG job
`rsingh-coll-dlq-orchestration`). CAI on federal (`ci/cai_jobs.py`): project
`rsingh-coll-dlq`, jobs `rsingh-coll-dlq-{daily-score,sync-code,backfill-history}`,
model `rsingh-coll-dlq-roll-scorer`, app `rsingh-coll-dlq-call-list`. On go01:
project `collections-tabicl`, model `collections-roll-scorer`, job
`collections-daily-score`, app `Collections Call List`.

**Redeploy after a code change:**
```bash
git push                          # GitHub cai-pipeline runs sync-code + a dry-run score on CAI
cde repository sync --name rsingh-coll-dlq-pipeline
set -a; source .env; set +a; python ci/setup_cai.py --sync   # CAI project to origin/main (if no GitHub check)
# only if cde/resources/requirements.txt changed:
cde resource upload --name rsingh-coll-dlq-python-env --local-path cde/resources/requirements.txt
# poll: cde resource describe --name rsingh-coll-dlq-python-env   (until status == ready)
bash cde/scripts/deploy_jobs.sh   # recreates jobs; does NOT delete jobs no longer listed there
bash cde/scripts/deploy_dag.sh    # re-registers the DAG; give Airflow ~30s to re-parse
```
Local test before any of the above: `.venv/bin/python scripts/run_cde_local.py dq`
(or `all`) against `data/warehouse`. Impala from a laptop: `set -a; source .env; set +a`
then `coll.storage.get_storage().query(sql)` (needs `COLL_IMPALA_USER`/`PASSWORD` in `.env`).

**Known gotchas:**
- `cde run logs` on this vcluster garbles/truncates driver stdout once GX's
  `tqdm` progress bars write `\r`-heavy lines — don't trust it as evidence of
  what a `dq-check` run actually found; query `ref.dq_results` via Impala.
- Python-env rebuilds take longer than the "1-3 min" the deploy script
  comments expect once real dependencies are added (~6 min observed for
  `great_expectations`).
- `cde run fg-status` (per-task Airflow status) is not available on this
  vcluster; reconstruct per-task status from each underlying CDE job's run
  history instead (filter by job name + start time).
- zsh: `status` is a read-only special variable — don't use it as a shell
  loop variable name in polling scripts (`st` works fine).

**Current status (as of 2026-09-27 23:06 IST):** `validate_bronze` retired;
`dq_check.py` live on the vcluster and verified two ways — a manual job
chain and a real Airflow-triggered DAG run — both with 0 critical failures.
Tonight's regularly scheduled run (00:30 UTC / 06:00 IST) will exercise the
new gate as normal daily operation, no further action needed for that.

**Not started:**
- Cloudera Data Visualization dashboards over `ref.dq_results` — needs live
  `COLL_CDV_*` credentials (still placeholders in `.env`) and creates real
  resources on the CDP environment; design is in `PLAN.md` (own connection
  `rsingh-collections-impala`, workspace, datasets via the Admin API, export
  to `cdv/`).
- Write-audit-publish on gold with Iceberg branches — optional/future, check
  branch support on the vcluster first.
- Restart the model endpoint automatically after the daily run.
- Exercise the collector-outcome loop (app → `bronze.collector_outcomes` →
  next day's silver contact history) on the live platform.
- Try a GPU context larger than 50,000 rows and compare holdout capture.
