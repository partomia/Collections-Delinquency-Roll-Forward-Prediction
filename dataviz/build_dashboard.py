#!/usr/bin/env python3
"""
The Cloudera Data Visualization dashboards of this project, as code: "Collections
Roll-Forward - Call List & Model Trust" (the business view) and "Collections Roll-Forward -
Data Health" (the pipeline's data quality gates).

Datasets, visuals and sheets are declared below; this script turns them into a Data
Visualization export file (dataviz/collections_dashboards.json) and imports it through the
migration REST API of the CDW Data Visualization instance at COLL_CDV_URL, into this
project's own workspace (WORKSPACE). The instance is shared with other projects: only the
connection, workspace, datasets and dashboards named here are created or updated. UUIDs
are fixed per artefact, so an import updates the dashboards in place. Datasets read the
rsingh_collections_delinquency_prediction_report views (sql/dataviz_views.sql); see
docs/DATAVIZ.md.

  set -a; source .env; set +a
  python dataviz/build_dashboard.py              # connection, workspace (if missing), file, import
  python dataviz/build_dashboard.py --no-import  # write the file only
  python dataviz/build_dashboard.py --verify     # every visual's query through the Data API

Needs COLL_CDV_URL, COLL_CDV_API_KEY and, for the column types and a new connection,
COLL_IMPALA_USER / COLL_IMPALA_PASSWORD. The password goes to the Data Visualization
connection only; it is never printed or written to the file.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import uuid
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT = ROOT / "dataviz" / "collections_dashboards.json"
DB = "rsingh_collections_delinquency_prediction_report"
CONNECTION = "rsingh-coll-dlq-impala"
WORKSPACE = "rsingh-coll-dlq"
NS = uuid.UUID("3b8e5d27-41c6-4f0a-9d2e-c7a1f6b09e58")
DATASET_PK0 = 7100
VISUAL_PK0 = 7200

DATASETS = {                          # key: (name, view, integer columns that are dimensions)
    "calls": ("Collections DLQ - Call list", "v_call_list", {"is_latest", "priority_rank", "product_code", "dpd_now"}),
    "book": ("Collections DLQ - Book weekly", "v_book_weekly", set()),
    "runs": ("Collections DLQ - Model runs", "v_model_run", {"is_latest"}),
    "holdout": ("Collections DLQ - Holdout deciles", "v_holdout", {"is_latest", "decile"}),
    "dq": ("Collections DLQ - Data quality", "v_dq", {"is_latest", "layer_order"}),
    "dqrun": ("Collections DLQ - DQ runs", "v_dq_run", {"is_latest", "layer_order"}),
}

LATEST = "[is_latest] = 1"
LAKH = "sum([overdue_amount]) / 100000"
ROLL_RATE = "sum([rolls]) / sum([labelled])"

# A visual: dims are (column, alias); measures are (expression, alias); filters are expressions;
# pos is (column, row, width, height) on a 64-column grid.
CALLS_AND_TRUST = [
    ("Today's call list", [
        dict(type="kpi", ds="calls", title="SMA-0 loans on today's list", measures=[("sum(1)", "Loans")],
             filters=[LATEST], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="calls", title="Overdue on the list (lakh INR)", measures=[(f"round({LAKH}, 2)", "Overdue (lakh)")],
             filters=[LATEST], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="calls", title="Agent calls today",
             measures=[("sum(case when [treatment] = 'AGENT_CALL_TODAY' then 1 else 0 end)", "Agent calls")],
             filters=[LATEST], pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="calls", title="Expected rolls to SMA-1 without action",
             measures=[("round(sum([p_roll]), 0)", "Expected rolls")], filters=[LATEST], pos=(49, 1, 16, 10)),
        dict(type="trellis-bars", ds="calls", title="Loans by treatment and product",
             x=[("treatment", "Treatment")], measures=[("sum(1)", "Loans")], color=[("product_name", "Product")],
             filters=[LATEST], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="calls", title="Expected overdue rolling (lakh INR) by product",
             x=[("product_name", "Product")], measures=[("sum([expected_roll_inr]) / 100000", "Expected roll (lakh)")],
             filters=[LATEST], sort_desc=True, pos=(33, 11, 32, 22)),
        dict(type="trellis-bars", ds="calls", title="Loans by days past due and treatment",
             x=[("dpd_bucket", "DPD")], measures=[("sum(1)", "Loans")], color=[("treatment", "Treatment")],
             filters=[LATEST], pos=(1, 33, 64, 20)),
        dict(type="table", ds="calls", title="The morning's top 25 calls",
             dims=[("priority_rank", "Rank"), ("loan_id", "Loan"), ("product_name", "Product"), ("dpd_now", "DPD"),
                   ("treatment", "Treatment"), ("action", "Action"), ("risk_signals", "Risk signals")],
             measures=[("round(sum([overdue_amount]), 0)", "Overdue (INR)"), ("round(max([p_roll]), 3)", "p(roll)")],
             filters=[LATEST, "[priority_rank] <= 25"], sort_dim="priority_rank", limit=25, pos=(1, 53, 64, 26)),
    ]),
    ("Model trust", [
        dict(type="kpi", ds="runs", title="Holdout AUC (latest run)", measures=[("max([holdout_auc])", "AUC")],
             filters=[LATEST], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="runs", title="Top 10% of calls catch (share of rolls)",
             measures=[("max([capture_top10])", "Model")], filters=[LATEST], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="runs", title="DPD alone, top 10%", measures=[("max([dpd_only_capture_top10])", "DPD alone")],
             filters=[LATEST], pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="runs", title="Top 10% catch (share of rolled overdue INR)",
             measures=[("max([value_capture_top10])", "Value capture")], filters=[LATEST], pos=(49, 1, 16, 10)),
        dict(type="trellis-lines", ds="holdout", title="Holdout: share of rolls caught, calling by p(roll) against random",
             x=[("decile", "Decile (top 10% steps)")],
             measures=[("max([cum_capture])", "Model"), ("max([random_cum_capture])", "Random")],
             filters=[LATEST], pos=(1, 11, 32, 24)),
        dict(type="trellis-lines", ds="runs", title="Top 10% capture per run: model against DPD alone",
             x=[("run_date", "Run date")],
             measures=[("max([capture_top10])", "Model"), ("max([dpd_only_capture_top10])", "DPD alone")],
             pos=(33, 11, 32, 24)),
        dict(type="table", ds="runs", title="Every run: trust numbers",
             dims=[("run_date", "Run date"), ("triggered_by", "Triggered by"), ("device", "Device")],
             measures=[("max([holdout_auc])", "AUC"), ("max([capture_top10])", "Top 10%"),
                       ("max([dpd_only_capture_top10])", "DPD top 10%"), ("max([capture_top40])", "Top 40%"),
                       ("max([dpd_only_capture_top40])", "DPD top 40%"), ("max([value_capture_top10])", "Value top 10%"),
                       ("sum([scored_loans])", "Scored"), ("max([duration_s])", "Seconds")],
             sort_dim="run_date", sort_asc=False, pos=(1, 35, 64, 24)),
    ]),
    ("Book trends", [
        dict(type="trellis-lines", ds="book", title="Weekly roll rate to SMA-1 within 30 days, by product",
             x=[("snapshot_date", "Snapshot")], measures=[(ROLL_RATE, "Roll rate")], color=[("product_name", "Product")],
             filters=["[labelled] > 0"], pos=(1, 1, 64, 24)),
        dict(type="trellis-bars", ds="book", title="SMA-0 loans per weekly snapshot",
             x=[("snapshot_date", "Snapshot")], measures=[("sum([loans])", "Loans")], color=[("product_name", "Product")],
             filters=["[snapshot_date] >= '2026-04-01'"], pos=(1, 25, 32, 22)),
        dict(type="trellis-lines", ds="book", title="Overdue in the SMA-0 book (lakh INR)",
             x=[("snapshot_date", "Snapshot")], measures=[("sum([overdue_inr]) / 100000", "Overdue (lakh)")],
             pos=(33, 25, 32, 22)),
    ]),
]

CRITICAL_FAILED = "sum(case when [severity] = 'critical' then [failed] else 0 end)"
WARNINGS_FAILED = "sum(case when [severity] = 'warning' then [failed] else 0 end)"

DATA_HEALTH = [
    ("Health now", [
        dict(type="kpi", ds="dq", title="Checks in the latest run", measures=[("sum(1)", "Checks")],
             filters=[LATEST], pos=(1, 1, 11, 10)),
        dict(type="kpi", ds="dq", title="Pass rate (%)", measures=[("round(100 * avg([passed]), 1)", "Pass rate %")],
             filters=[LATEST], pos=(12, 1, 11, 10)),
        dict(type="kpi", ds="dq", title="Critical failures (stop the pipeline)",
             measures=[(CRITICAL_FAILED, "Critical failures")], filters=[LATEST], pos=(23, 1, 10, 10)),
        dict(type="kpi", ds="dq", title="Warnings raised", measures=[(WARNINGS_FAILED, "Warnings")],
             filters=[LATEST], pos=(33, 1, 11, 10)),
        dict(type="kpi", ds="dq", title="Near misses (passed, with bad rows)",
             measures=[("sum([near_miss])", "Near misses")], filters=[LATEST], pos=(44, 1, 10, 10)),
        dict(type="kpi", ds="dq", title="Rows under check (millions)",
             measures=[("round(sum([row_count]) / 1000000, 1)", "Rows (M)")], filters=[LATEST], pos=(54, 1, 11, 10)),
        dict(type="trellis-bars", ds="dq", title="What is checked: checks by layer and category",
             x=[("layer", "Layer")], measures=[("sum(1)", "Checks")], color=[("category", "Category")],
             filters=[LATEST], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="dq", title="How strict: checks by category and severity",
             x=[("category", "Category")], measures=[("sum(1)", "Checks")], color=[("severity", "Severity")],
             filters=[LATEST], sort_desc=True, pos=(33, 11, 32, 22)),
        dict(type="table", ds="dq", title="Table scorecard (latest run of each layer)",
             dims=[("layer_order", "Step"), ("layer", "Layer"), ("table_name", "Table"), ("run_ts", "Checked at")],
             measures=[("sum(1)", "Checks"), ("sum([passed])", "Passed"), ("sum([failed])", "Failed"),
                       ("sum([near_miss])", "Near misses"), ("max([row_count])", "Rows")],
             filters=[LATEST], sort_dim="layer_order", pos=(1, 33, 64, 26)),
    ]),
    ("Trends", [
        dict(type="trellis-lines", ds="dq", title="Pass rate by business date and layer",
             x=[("as_of", "As of")], measures=[("avg([passed])", "Pass rate")], color=[("layer", "Layer")],
             pos=(1, 1, 32, 22)),
        dict(type="trellis-bars", ds="dqrun", title="Checks passed and failed per pipeline run",
             x=[("run_label", "Pipeline run")], measures=[("sum([passed])", "Passed"), ("sum([failed])", "Failed")],
             pos=(33, 1, 32, 22)),
        dict(type="trellis-lines", ds="dq", title="Volume drift: bronze rows per table",
             x=[("as_of", "As of")], measures=[("max([row_count])", "Rows")], color=[("table_name", "Table")],
             filters=["[layer] = 'bronze'", "[row_count] is not null"], pos=(1, 23, 64, 24)),
    ]),
    ("Pipeline runs", [
        dict(type="trellis-bars", ds="dqrun", title="When each gate ran: minutes after the bronze gate",
             x=[("run_label", "Pipeline run")], measures=[("max([minutes_after_bronze])", "Minutes")],
             color=[("layer", "Layer")], pos=(1, 1, 32, 22)),
        dict(type="trellis-bars", ds="dqrun", title="Rows checked per run and layer (millions)",
             x=[("run_label", "Pipeline run")], measures=[("sum([rows_checked]) / 1000000", "Rows (M)")],
             color=[("layer", "Layer")], pos=(33, 1, 32, 22)),
        dict(type="table", ds="dqrun", title="Every DQ gate run",
             dims=[("checked_at", "Checked at"), ("run_label", "Pipeline run"), ("run_type", "Type"),
                   ("as_of", "Business date"), ("layer", "Layer")],
             measures=[("max([minutes_after_bronze])", "Min after bronze"), ("sum([checks])", "Checks"),
                       ("sum([failed])", "Failed"), ("sum([critical_failed])", "Critical"),
                       ("sum([warnings_failed])", "Warnings"), ("sum([near_misses])", "Near misses"),
                       ("sum([table_count])", "Tables"), ("sum([rows_checked])", "Rows checked")],
             sort_dim="checked_at", sort_asc=False, pos=(1, 23, 64, 26)),
    ]),
    ("Check details", [
        dict(type="table", ds="dq", title="Near misses in the latest run: passed, but found bad rows",
             dims=[("layer", "Layer"), ("table_name", "Table"), ("check_name", "Check"), ("column_name", "Columns"),
                   ("severity", "Severity")],
             measures=[("max([unexpected_count])", "Unexpected rows"), ("max([unexpected_pct])", "Unexpected %")],
             filters=[LATEST, "[near_miss] = 1"], pos=(1, 1, 64, 14)),
        dict(type="table", ds="dq", title="Failed checks, all runs (warnings here do not stop the pipeline)",
             dims=[("as_of", "As of"), ("run_label", "Pipeline run"), ("layer", "Layer"), ("table_name", "Table"),
                   ("check_name", "Check"), ("severity", "Severity"), ("observed_value", "Observed")],
             measures=[("sum([unexpected_count])", "Unexpected rows")],
             filters=["[failed] = 1"], sort_dim="as_of", sort_asc=False, pos=(1, 15, 64, 14)),
        dict(type="table", ds="dq", title="Check catalogue (latest run)",
             dims=[("layer_order", "Step"), ("layer", "Layer"), ("table_name", "Table"), ("category", "Category"),
                   ("check_name", "Check"), ("column_name", "Columns"), ("severity", "Severity"),
                   ("observed_value", "Observed")],
             measures=[("sum([passed])", "Passed")],
             filters=[LATEST], sort_dim="layer_order", limit=300, pos=(1, 29, 64, 34)),
    ]),
]

DASHBOARDS = [
    dict(key="calls", pk=7000, ds="calls", title="Collections Roll-Forward - Call List & Model Trust",
         sheets=CALLS_AND_TRUST,
         subtitle="Daily SMA-0 call list, holdout trust against DPD alone, and book trends (federal CDW)"),
    dict(key="health", pk=7001, ds="dq", title="Collections Roll-Forward - Data Health", sheets=DATA_HEALTH,
         subtitle="Great Expectations gates of the collections pipeline: bronze, silver, gold (ref.dq_results)"),
]

SHELVES = {
    "kpi": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("compare_shelf", 1, 2), ("label_shelf", 1, 2),
            ("tooltip_shelf", 1, 2), ("x_shelf", 1, 1), ("y_shelf", 1, 1), ("filters_shelf", 2, 3)],
    "table": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-bars": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                     ("drill_shelf", 1, 1), ("label_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-lines": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                      ("filters_shelf", 2, 3)],
}


def uid(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def column_types(ds_key: str) -> dict[str, str]:
    from coll.storage import get_storage
    view = DATASETS[ds_key][1]
    df = get_storage("impala").query(f"DESCRIBE {DB}.{view}")
    return {r["name"]: r["type"].upper() for _, r in df.iterrows()}


def is_dim(ds_key: str, col: str, typ: str) -> bool:
    return col in DATASETS[ds_key][2] or not any(t in typ for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL"))


def dataset_record(key: str, pk: int, types: dict[str, str], conn_id: int) -> dict:
    name, view, _ = DATASETS[key]
    used_by = [d["pk"] for d in DASHBOARDS if any(v["ds"] == key for _, items in d["sheets"] for v in items)]
    table = f"{DB}.{view}"
    cols = [{"alias": c, "type": t, "name": c, "isdim": is_dim(key, c, t)} for c, t in types.items()]
    return {"model": "datasets.dataset", "pk": pk, "fields": {
        "dataconnection": conn_id, "dataset_name": name, "dataset_type": "singletable", "dataset_detail": table,
        "dataset_description": f"{table} (sql/dataviz_views.sql)",
        "dataset_info": json.dumps([{"tablename": table, "columns": cols}]),
        "dataset_tablenames": json.dumps([table]), "uuid": uid("dataset", key), "imported_uuid": None,
        "cache_sequence": 0, "dataset_settings": "{}", "search_enabled": False, "dashboards": used_by,
        "version_id": pk, "version_group_id": pk, "is_active_version": True,
        "version_name": "collections-roll-forward", "is_named_version": False}}


def dim_item(col: str, alias: str, typ: str) -> dict:
    return {"dataset_colname": col, "dataset_coltype": typ, "expression_for_trigger": f"[{col}]", "col_alias": alias}


def measure_item(expr: str, alias: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "expr_hasagg": True, "col_alias": alias,
            "dataset_colname": alias, "dataset_coltype": "DOUBLE"}


def filter_item(expr: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "filter_input": {}, "filter_data": [],
            "dataset_colname": "", "dataset_coltype": "STRING", "filter_column": ""}


def visual_uid(dash: dict, sheet: str, title: str) -> str:
    return uid("visual", dash["key"], sheet, title)


def dashboard_uid(dash: dict) -> str:
    return uid("dashboard", dash["key"])


def visual_record(v: dict, pk: int, dash: dict, sheet: str, types: dict[str, str], dataset_pk: int,
                  ws_id: int) -> dict:
    kind = v["type"]
    shelves = {name: [] for name, _, _ in SHELVES[kind]}
    sources = {}

    def add_dims(shelf, pairs):
        for col, alias in pairs:
            shelves[shelf].append(dim_item(col, alias, types[col]))
            sources[f"[{col}] as 'sub:{alias}'"] = shelf

    def add_measures(shelf, pairs):
        for expr, alias in pairs:
            shelves[shelf].append(measure_item(expr, alias))
            sources[f"{expr} as 'sub:{alias}'"] = shelf

    if kind in ("kpi", "table"):
        add_dims("dimensions_shelf", v.get("dims", []))
        add_measures("aggregates_shelf", v["measures"])
    else:
        add_dims("x_shelf", v["x"])
        add_measures("y_shelf", v["measures"])
        add_dims("color_shelf", v.get("color", []))
    for expr in v.get("filters", []):
        shelves["filters_shelf"].append(filter_item(expr))
        sources[expr] = "filters_shelf"
    if v.get("sort_desc"):
        shelves["y_shelf"][0]["order"] = {"priority": 1, "ascending": False}
    if v.get("sort_dim"):
        shelf = "dimensions_shelf" if kind == "table" else "x_shelf"
        item = next(i for i in shelves[shelf] if i["dataset_colname"] == v["sort_dim"])
        item["order"] = {"priority": 1, "ascending": v.get("sort_asc", True)}
    report = {
        "report_title": v["title"], "report_subtitle": "", "dashboard_id": dash["pk"],
        "limit": v.get("limit", 1000), "sample_pct": "Off", "selected_segments": [], "report_derived_data": [],
        "click_behaviors": {}, "sort_orders_asc": {}, "user_settings": {}, **shelves,
        "core": {"viz_type": kind, "saved_shelf_sources": sources,
                 "shelves": [{"name": n, "shelf_type": s, "column_type": c} for n, s, c in SHELVES[kind]]},
    }
    return {"model": "reports.report", "pk": pk, "fields": {
        "report_name": "", "report_description": f"{dash['title']} / {sheet}", "dataset": dataset_pk,
        "workspace": ws_id, "report_type": kind, "report_mode": "", "dashboard_url_name": "",
        "report_data": json.dumps({"report_data": report, "report_type": kind}), "shared_visual_dashboards": None,
        "parent_report": None, "uuid": visual_uid(dash, sheet, v["title"]), "imported_uuid": None,
        "has_css_styles": False, "report_search_text": ""}}


def widgets(pairs: list[tuple[int, tuple]]) -> list[dict]:
    return [{"col": c, "row": r, "size_x": w, "size_y": h, "id": f"uri-{i}-widget-{pk}"}
            for i, (pk, (c, r, w, h)) in enumerate(pairs, 1)]


def build(conn_id: int, ws_id: int, version: dict, types: dict[str, dict[str, str]] | None = None) -> dict:
    ds_pk = {k: DATASET_PK0 + i for i, k in enumerate(DATASETS)}
    types = types or {k: column_types(k) for k in DATASETS}
    visuals, dashboards, pk = [], [], VISUAL_PK0
    for d in DASHBOARDS:
        sheets = []
        for order, (sheet, items) in enumerate(d["sheets"], 1):
            placed = []
            for v in items:
                pk += 1
                visuals.append(visual_record(v, pk, d, sheet, types[v["ds"]], ds_pk[v["ds"]], ws_id))
                placed.append((pk, v["pos"]))
            sheets.append({"sheet_id": order, "order": order, "sheet_handle_title": sheet, "behaviors": {},
                           "visual_widgets": widgets(placed), "control_widgets": []})
        body = {"report_title": d["title"], "numColumns": 64, "report_subtitle": d["subtitle"],
                "dashboard_widgets": sheets[0]["visual_widgets"], "dashboard_sheets": sheets,
                "user_settings": {"dashboard_width": "1280", "display_filters": "true",
                                  "permit_csv_download_dashboard": "true"},
                "global_control_widgets": [], "control_widgets": [], "click_behavior": {}}
        dashboards.append({"model": "reports.report", "pk": d["pk"], "fields": {
            "report_name": d["title"], "report_description": "docs/DATAVIZ.md",
            "dataset": ds_pk[d["ds"]], "workspace": ws_id, "report_type": "dashboard", "report_mode": None,
            "dashboard_url_name": "", "report_data": json.dumps(body), "shared_visual_dashboards": "[]",
            "parent_report": None, "uuid": dashboard_uid(d), "imported_uuid": None, "has_css_styles": False,
            "report_search_text": None}})
    return {"segments": [], "staticasset": [], "dashboards": dashboards, "appgroupmembership": [],
            "reportannotation": [], "events": [], "customcss": [], "reportimage": [], "dateranges": [],
            "visuals": visuals, "colorpalette": [], "appgroups": [],
            "datasets": [dataset_record(k, ds_pk[k], types[k], conn_id) for k in DATASETS], "version": version}


class DataViz:
    """The CDW Data Visualization instance at COLL_CDV_URL, authenticated with its API key."""

    def __init__(self):
        p = urllib.parse.urlparse(os.environ["COLL_CDV_URL"])
        self.url = f"{p.scheme or 'https'}://{p.netloc or p.path.split('/', 1)[0]}"
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"apikey {os.environ['COLL_CDV_API_KEY']}"

    def get(self, path: str, **params):
        r = self.s.get(self.url + path, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, items: list[dict]) -> list[dict]:
        r = self.s.post(self.url + path, data={"data": json.dumps(items)}, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"POST {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def connection(self) -> int:
        found = next((c for c in self.get("/arc/adminapi/v1/connections") if c["name"] == CONNECTION), None)
        if found:
            print(f"connection {CONNECTION}: exists ({found['id']})")
            return found["id"]
        from coll.config import settings
        imp = settings()["impala"]
        params = {"HOST": imp["host"], "PORT": str(imp.get("port", 443)), "USERNAME": os.environ["COLL_IMPALA_USER"],
                  "MODE": "http", "HS2_HTTP_SQLPATH": imp.get("http_path", "cliservice"), "SOCK": "ssl",
                  "AUTH": "ldap", "SOCKET_TIMEOUT": 600, "IMPERSONATION": False, "APP_NAME": "viz",
                  "CONCURRENCY": 100, "CONCURRENCY_USER": 5, "QUERY_TIMEOUT": 120,
                  "QUERY_LOADING_WARNING_SECONDS": 20, "CACHE": {"ENABLED": 1, "RETENTION": 1800}}
        body = {"name": CONNECTION, "type": "impyla", "info": {"PARAMS": params},
                "password": os.environ["COLL_IMPALA_PASSWORD"]}
        conn_id = self.post("/arc/adminapi/v1/connections", [body])[0]["id"]
        print(f"connection {CONNECTION}: created ({conn_id})")
        return conn_id

    def workspace(self) -> int:
        found = next((w for w in self.get("/arc/adminapi/v1/workspaces") if w["name"] == WORKSPACE), None)
        if found:
            print(f"workspace {WORKSPACE}: exists ({found['id']})")
            return found["id"]
        user = os.environ["COLL_IMPALA_USER"]
        body = {"name": WORKSPACE, "desc": "Collections roll-forward prediction (rsingh-coll-dlq)",
                "editable": True, "acl": [[1, 3, user], [2, 1, "Everyone"]]}
        ws_id = self.post("/arc/adminapi/v1/workspaces", [body])[0]["id"]
        print(f"workspace {WORKSPACE}: created ({ws_id})")
        return ws_id

    def version(self) -> dict:
        return self.get("/arc/migration/api/export/", dashboards="[]", filename="version", dry_run="False")["version"]

    def verify(self) -> int:
        """Run every visual's query through the Data API (Data Visualization -> its connection -> Impala)."""
        ids = {d["name"]: d["id"] for d in self.get("/arc/adminapi/v1/datasets")}
        failed = 0
        for sheet, items in [s for d in DASHBOARDS for s in d["sheets"]]:
            for v in items:
                dims = v.get("dims", []) + v.get("x", []) + v.get("color", [])
                dsreq = {"version": 1, "type": "SQL", "limit": v.get("limit", 1000),
                         "dimensions": [{"type": "SIMPLE", "expr": f"[{c}] as '{a}'"} for c, a in dims],
                         "aggregates": [{"expr": f"{e} as '{a}'"} for e, a in v["measures"]],
                         "filters": v.get("filters", []), "dataset_id": ids[DATASETS[v["ds"]][0]]}
                r = self.s.post(self.url + "/arc/api/data", data={"version": 1, "dsreq": json.dumps(dsreq)},
                                timeout=300)
                rows = json.loads(r.json()["rows"]) if r.status_code == 200 else None
                ok = rows is not None
                failed += not ok
                first = rows[0] if rows else (None if ok else r.text[:200])
                print(f"{'ok  ' if ok else 'FAIL'} {sheet} / {v['title']}: "
                      f"{len(rows) if ok else r.status_code} rows, first {first}")
        return failed

    def import_file(self, path: Path) -> None:
        with path.open("rb") as f:
            r = self.s.post(self.url + "/arc/migration/api/import/", files={"import_file": f},
                            data={"dry_run": "False", "dataconnection_name": CONNECTION}, timeout=300)
        if r.status_code != 200:
            raise SystemExit(f"import: HTTP {r.status_code} {r.text[:500]}")
        done = r.json()
        print("import: " + ", ".join(f"{len(done.get(k, []))} {k}" for k in ("datasets", "visuals", "dashboards")))

    def move_to_workspace(self, ws_id: int) -> None:
        """The import ignores the file's workspace (it lands in the importer's Private one).
        Move every visual and dashboard of this project's datasets; POST to the visual's own
        URL updates it (POST to the collection would create a copy)."""
        ours = {d["id"] for d in self.get("/arc/adminapi/v1/datasets") if d["name"] in {n for n, _, _ in DATASETS.values()}}
        moved = 0
        for v in self.get("/arc/adminapi/v1/visuals", detail="true"):
            if v.get("dataset_id") not in ours or v.get("workspace_id") == ws_id:
                continue
            body = {k: v[k] for k in ("id", "title", "type", "description", "dataset_id", "data")}
            body["workspace_id"] = ws_id
            r = self.s.post(f"{self.url}/arc/adminapi/v1/visuals/{v['id']}", data={"data": json.dumps([body])}, timeout=60)
            if r.status_code != 200:
                raise SystemExit(f"move visual {v['id']}: HTTP {r.status_code} {r.text[:200]}")
            moved += 1
        print(f"workspace {WORKSPACE}: moved {moved} visuals and dashboards")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--no-import", action="store_true", help="write the export file only")
    p.add_argument("--verify", action="store_true", help="only run every visual's query through the Data API")
    args = p.parse_args()
    viz = DataViz()
    if args.verify:
        return 1 if viz.verify() else 0
    conn_id, ws_id = viz.connection(), viz.workspace()
    doc = build(conn_id, ws_id, viz.version())
    OUT.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(doc['datasets'])} datasets, {len(doc['visuals'])} visuals, "
          f"{len(doc['dashboards'])} dashboards")
    if not args.no_import:
        viz.import_file(OUT)
        viz.move_to_workspace(ws_id)
        print(f"open {viz.url}/arc/apps/ -> Visuals -> workspace {WORKSPACE}: "
              + ", ".join(d["title"] for d in DASHBOARDS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
