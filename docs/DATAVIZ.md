# Dashboards in Cloudera Data Visualization

Two dashboards, built as code and imported into the federal CDW Data Visualization instance
(`COLL_CDV_URL`, shared with other projects): **Collections Roll-Forward - Call List & Model
Trust** (the business view of the published call list) and **Collections Roll-Forward - Data
Health** (the pipeline's Great Expectations gates). Both sit in this project's own workspace
`rsingh-coll-dlq`. Nothing in the pipeline reads them; they only read.

| Layer | What | Where |
|---|---|---|
| Views | 6 flat reporting views, one per dataset | `sql/dataviz_views.sql` -> `rsingh_collections_delinquency_prediction_report` |
| Connection | `rsingh-coll-dlq-impala`: impyla, CDW `federal-impala-1` public endpoint, port 443, HTTP `cliservice`, TLS, LDAP as the workload user | created by `dataviz/build_dashboard.py` |
| Workspace | `rsingh-coll-dlq`: the workload user manages, Everyone views | created by `dataviz/build_dashboard.py` |
| Datasets, visuals, dashboards | 6 datasets (`Collections DLQ - ...`), 36 visuals, 2 dashboards of 7 sheets, fixed UUIDs | `dataviz/build_dashboard.py` -> `dataviz/collections_dashboards.json` |

## Call List & Model Trust

- **Today's call list**: SMA-0 loans on the list, overdue on the list (lakh), agent calls today,
  expected rolls without action; loans by treatment and product; expected overdue rolling by
  product; loans by DPD bucket and treatment; the morning's top 25 calls with action and risk
  signals.
- **Model trust**: latest holdout AUC, top 10% capture, DPD alone at top 10%, value capture;
  holdout cumulative capture by decile against random calling; top 10% capture per run, model
  against DPD alone; every run with its trust numbers.
- **Book trends**: weekly 30-day roll rate to SMA-1 by product; SMA-0 loans per snapshot;
  overdue in the SMA-0 book.

## Data Health

Over `v_dq` (one row per check result, with a category, a near-miss flag and the row count of
row-count checks) and `v_dq_run` (one row per pipeline run and layer). A near miss is a check
that passed but found unexpected rows, absorbed by its threshold.

- **Health now** (latest run of each layer): checks, pass rate, critical failures, warnings,
  near misses, rows under check; checks by layer and category and by severity; a scorecard per
  table.
- **Trends**: pass rate by business date and layer; passed and failed per pipeline run; bronze
  volume per table.
- **Pipeline runs**: when the silver and gold gates ran after the bronze gate; rows checked per
  run and layer; every gate run (DAG or manual) with its counts.
- **Check details**: the near misses, every failed check (the latest-roll-rate warning shows
  here; warnings do not stop the pipeline), and the full check catalogue of the latest run.

## Build or rebuild

```bash
set -a; source .env; set +a
python scripts/run_impala_sql.py sql/dataviz_views.sql   # views (and the KPI check queries)
python dataviz/build_dashboard.py                        # connection, workspace, file, import, move
python dataviz/build_dashboard.py --verify               # every visual's query through the Data API
```

The script authenticates with the instance's API key (`COLL_CDV_API_KEY`). The import matches
artefacts by UUID, so a rerun updates the dashboards in place. The import ignores the file's
workspace and puts dashboards in the importer's Private workspace; the script then moves every
visual of this project's datasets into `rsingh-coll-dlq` (a POST to the visual's own admin API
URL; a POST to the collection creates a copy). `--verify` sends every visual's query through
the Data API, so it checks Data Visualization's own connection to Impala, not the laptop's.

## Notes

- The instance is shared: the script only creates or updates the connection, workspace,
  datasets and dashboards named in `dataviz/build_dashboard.py`.
- The connection stores the workload password inside Data Visualization; rotate it there
  (Data -> connection -> Edit) when the password changes.
- The instance's own `federal-impala-1` connection uses a cluster-internal host and a service
  user; this project's connection uses the public endpoint as the workload user.
