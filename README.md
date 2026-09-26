# Collections: Delinquency Roll-Forward Prediction on Cloudera

Collections teams cannot call everyone in early delinquency, so the model
decides who gets the scarce human effort first. Every day, each SMA-0 loan
(1–30 days past due) is scored for its chance of rolling into SMA-1 (31+ DPD)
within 30 days, ranked by `priority = p_roll × overdue amount`, and mapped to
a treatment band sized to dialler capacity. The model is **TabICL v2**, a
tabular foundation model that learns in context from labelled history in a
gold Iceberg table: no training step, no retraining cycle.

> Demo of the data and scoring pipeline on synthetic data, not a validated
> credit model. The model only ranks accounts; contact hours, conduct and
> frequency follow the RBI fair practices code for recovery agents.

## Architecture

```mermaid
flowchart LR
  A[CDE Spark<br/>generate_loan_bronze] --> B[validate_bronze<br/>gate]
  B --> C[build_silver]
  C --> D[build_gold_features<br/>MERGE, snapshot per load]
  D --> E[CAI Job<br/>daily_score<br/>TabICL v2]
  E --> F[(gold: call_list<br/>holdout, model_run)]
  F --> G[CAI Application<br/>Streamlit call list]
  F --> H[CDW Impala / Hue<br/>reports, time travel]
  G -. what-if .-> I[CAI Model endpoint<br/>predict.py]
  G -. collector outcomes .-> J[(bronze:<br/>collector_outcomes)]
  J -. tomorrow's features .-> C
  AF[CDE Airflow DAG<br/>daily] -. orchestrates .-> A
  AF -. API v2 .-> E
```

| Layer | Cloudera service | Code |
|---|---|---|
| Ingest + medallion | CDE Spark, Iceberg | `cde/jobs/` |
| Orchestration | CDE Airflow | `cde/dags/collections_dag.py` |
| Daily scoring | CAI Job (GPU) | `cai/jobs/daily_score.py` |
| On-demand / what-if API | CAI Model Deployment | `cai/model/predict.py` |
| Reports, time travel | CDW Impala (Hue) | `sql/reports.sql` |
| Demo UI | CAI Application | `app/` |

Databases: `rsingh_collections_delinquency_prediction_{bronze,silver,gold,ref}`
(prefix in `config/collections.yaml`, and `--db-prefix` / `DB_PREFIX` for the
CDE side). CDE resources are named `rsingh-coll-dlq-*`.

| Table | Written by | Grain |
|---|---|---|
| `bronze.loan_master` | CDE generate | loan: product, EMI, tenor, repayment mode, salary account flag |
| `bronze.loan_instalments` | CDE generate | loan × instalment: due date, paid date (NULL = unpaid) |
| `bronze.nach_presentations` | CDE generate | loan × NACH presentation: SUCCESS / BOUNCED, reason |
| `bronze.dialler_contacts` | CDE generate | loan × attempt: channel, outcome, promise-to-pay |
| `bronze.casa_credits` | CDE generate | customer × salary credit |
| `bronze.bureau_snapshot` | CDE generate | customer × monthly bureau pull |
| `bronze.collector_outcomes` | CAI app | loan × outcome recorded by a collector |
| `ref.product_map` | CDE generate | product code → name, tenor and rate bands |
| `silver.{loan, instalment, nach_presentation, collection_contact, salary_credit, bureau_score}` | CDE silver | deduplicated entities; PTP kept / broken derived |
| `gold.collections_features` | CDE gold (MERGE) | loan × weekly snapshot, SMA-0 only: 13 numeric features + label |
| `gold.collections_call_list` | CAI job | run date × loan: p_roll, priority, rank, treatment, risk signals |
| `gold.collections_holdout` | CAI job | run date × decile: roll rate, cumulative capture |
| `gold.collections_model_run` | CAI job | run date: checkpoint, context window, gold snapshot id, holdout metrics |

## Method

- **Label**: a loan in SMA-0 on snapshot date *d* rolled if it is 31+ DPD on any
  day in (*d*, *d* + 30]. NULL until 30 days have passed; each daily load fills
  in the labels that matured (Iceberg MERGE, one snapshot per load).
- **Snapshots**: every Friday for 18 months plus the as-of date (today's book).
- **Features** (all numeric, built in Spark): product, DPD now, overdue amount,
  EMI, overdue ÷ EMI, months on book, max DPD in 12 months, NACH bounces in
  6 months, broken PTPs in 3 months, contacts reached in 30 days, salary credit
  in 30 days, salary account with the bank, bureau score.
- **Context**: the most recent labelled rows (50,000 on GPU, 10,000 on CPU,
  `config/policy.yaml`). A run for date *D* only uses labels known on *D*
  (snapshots up to *D* − 30 days), so backfills are honest.
- **Holdout (trust number)**: score the latest 4 labelled weeks with a context
  ending 30 days before them. Reported as capture: "the top 10% of calls catch
  X% of the loans that actually rolled", next to calling by DPD alone.
- **Priority** = p_roll × overdue amount. **Treatment** by percentile across the
  book: top 10% agent call today (field visit if unreachable), next 30% agent
  or IVR with a payment link, rest SMS / WhatsApp. Cut-offs are business
  settings (policy file, app sliders), not model settings.
- **Lineage**: every run records the pinned checkpoint
  (`tabicl-classifier-v2-20260212.ckpt`), the TabICL version, the context window
  and the Iceberg snapshot id of gold it read; the endpoint rebuilds exactly that
  context with `FOR SYSTEM_VERSION AS OF`.

Synthetic data: 150,000 loans (about 106,000 active by the as-of date) from
April 2024. Each loan has a hidden monthly stress level (loan risk from product
and origination bureau, AR(1) noise, occasional income shocks, a book-wide
post-festive and monsoon factor). Stress drives missed EMIs, missing salary
credits, bureau drift, contactability and kept promises; a salary credit, a
reached contact or a NACH re-presentation raises the chance of curing on a
given day. About 24% of SMA-0 loans roll within 30 days. History is
prefix-stable: a later as-of date only adds days.

## Run locally

Python 3.11 and Java 17.

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
export HF_HOME=$PWD/.hf_cache

# CDE jobs on local Spark + Iceberg (about 1 minute), export gold to data/parquet
.venv/bin/python scripts/run_cde_local.py all --as-of 2026-09-25
.venv/bin/python scripts/run_cde_local.py history            # Iceberg snapshots of gold

# CAI jobs on parquet (first run downloads the TabICL checkpoint, ~110 MB)
.venv/bin/python cai/jobs/daily_score.py --backend parquet
.venv/bin/python cai/jobs/backfill_history.py --weeks 4 --backend parquet
.venv/bin/python cai/model/test_endpoint.py --local           # demo loan + salary-credit what-if

COLL_STORAGE_BACKEND=parquet .venv/bin/streamlit run app/streamlit_app.py
.venv/bin/pytest -q
```

Add `--stub` to the CAI scripts for a logistic-regression stand-in (no checkpoint).

## Deploy on Cloudera

### 1. CDE: Spark jobs and Airflow DAG

CDE CLI configured for the vcluster (`~/.cde/config.yaml`), repo pushed to GitHub.

```bash
./cde/scripts/deploy_jobs.sh      # CDE Repository rsingh-coll-dlq-pipeline + 4 Spark jobs
./cde/scripts/deploy_dag.sh       # Airflow job rsingh-coll-dlq-orchestration (daily)
./cde/scripts/backfill_drill.sh   # optional: a few daily loads = a few gold snapshots
```

After a code change: `git push`, then `cde repository sync --name rsingh-coll-dlq-pipeline`
(re-run `deploy_dag.sh` if the DAG changed). On a shared vcluster the first job
after idle can wait 20+ minutes for scale-up.

### 2. CAI project and session

New project `collections-tabicl` from this Git repo, Python 3.11 runtime.
Project Settings > Advanced > environment variables (inherited by sessions,
jobs, models and apps):

| Variable | Value |
|---|---|
| `HF_HOME` | `/home/cdsw/.hf_cache` |
| `COLL_IMPALA_USER` / `COLL_IMPALA_PASSWORD` | workload user / password (LDAP) |
| `COLL_IMPALA_HOST` | only if not the VW host in `config/collections.yaml` |

Session terminal (GPU profile if available):

```bash
git pull
pip3 install -r requirements.txt
python -c "import tabicl, torch; print('ok', torch.__version__, torch.cuda.is_available())"

# reads gold via Impala, downloads the checkpoint once, holdout + scoring, writes no table
python cai/jobs/daily_score.py --dry-run

# endpoint logic in-process: demo loan, then with a salary credit and two reached contacts
python cai/model/test_endpoint.py --local --context impala
```

For an air-gapped customer: download `tabicl-classifier-v2-20260212.ckpt` from
`jingang/TabICL` on a connected machine, upload it to `models/`, and set
`COLL_MODEL_MODEL_PATH=/home/cdsw/models/tabicl-classifier-v2-20260212.ckpt`.

### 3. CAI Job

Jobs > New Job: name `collections-daily-score`, script `cai/jobs/daily_score.py`,
arguments empty (Airflow passes `COLL_RUN_DATE` and `COLL_TRIGGERED_BY`),
Python 3.11, GPU profile (or 4 vCPU / 16 GB), schedule Manual. Run it once,
then optionally `cai/jobs/backfill_history.py --weeks 8` from a session for
history.

### 4. CAI Model Deployment

Model Deployments > New Model: name `collections-roll-scorer`, file
`cai/model/predict.py`, function `predict`, Python 3.11, GPU profile (or 4 vCPU /
16 GB), 1 replica, authentication on. Example input: the output of
`python cai/model/test_endpoint.py --print-request`. Each replica rebuilds the
context of the latest daily run from gold at start-up; restart the model to pick
up a newer run.

### 5. CAI Application

Applications > New Application: name `Collections Call List`, subdomain
`collections`, script `app/run.py`, Python 3.11, 2 vCPU / 4 GB. So what-ifs call
the model endpoint, set:

| Variable | Where to find it |
|---|---|
| `COLL_ENDPOINT_URL` | model Overview, URL in the sample curl (`https://modelservice.<domain>/model`) |
| `COLL_ENDPOINT_ACCESS_KEY` | model Settings |
| `COLL_ENDPOINT_API_KEY` | User Settings > API Keys, Model API key (authentication is on) |

### 5a. Airflow triggers the CAI Job

Print the ids in a session:

```bash
python - <<'EOF'
import os, cmlapi
c = cmlapi.default_client()
pid = os.environ["CDSW_PROJECT_ID"]
print("COLL_CAI_HOST       =", "https://" + os.environ["CDSW_DOMAIN"])
print("COLL_CAI_PROJECT_ID =", pid)
for j in c.list_jobs(pid).jobs:
    print("COLL_CAI_JOB_ID     =", j.id, "(", j.name, ")")
EOF
```

Create an API v2 key (User Settings > API Keys), then in the CDE Airflow UI >
Admin > Variables set `COLL_CAI_HOST`, `COLL_CAI_PROJECT_ID`, `COLL_CAI_JOB_ID`
and `COLL_CAI_API_KEY`. Without `COLL_CAI_HOST` the DAG skips the CAI step.

### 6. Run anywhere else

The app has no Cloudera-only dependency: `docker build -t collections-app .`
and run it on parquet exports or against CDW Impala (see `Dockerfile`).

## Layout

```
coll/          shared logic: config, features, TabICL wrapper, priority bands, holdout, storage, pipeline, scoring
cde/jobs/      Spark jobs (self-contained, PySpark + stdlib only)
cde/dags/      Airflow DAG (daily)
cde/scripts/   deploy_jobs.sh, deploy_dag.sh, backfill_drill.sh
cai/jobs/      daily_score.py, backfill_history.py
cai/model/     predict.py (endpoint), test_endpoint.py
app/           Streamlit app + CAI launcher
config/        collections.yaml (names, storage, model), policy.yaml (context, holdout, treatment bands)
sql/           Hue report and time-travel queries
docs/          demo runbook
scripts/       run_cde_local.py (CDE jobs on a laptop)
```

## Sources

- [TabICL on Hugging Face](https://huggingface.co/jingang/TabICL) · [TabICL (GitHub)](https://github.com/soda-inria/tabicl)
- [RBI: early recognition of financial distress (SMA classes)](https://www.rbi.org.in/)
- [Cloudera AI: creating and deploying a model](https://docs.cloudera.com/machine-learning/cloud/models/index.html)
