"""Daily scoring run: holdout check, context selection, score the SMA-0 book,
write the call list.

A run for run_date D only uses labels known on D: snapshots up to D - 30 days
(the label horizon). That keeps backfills of past run dates honest, and for a
normal daily run it is simply every matured label.

All reads are pinned to one Iceberg snapshot of gold.collections_features
(Impala backend), recorded in collections_model_run together with the context
window, so the endpoint and any audit can rebuild the exact context later.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from coll import holdout as ho
from coll.config import ROOT, table
from coll.config import policy as load_policy
from coll.features import COLUMNS, FEATURES
from coll.model import build_model, context_rows, has_cuda, model_id, new_classifier, predict_roll, tabicl_version
from coll.prioritise import prioritise

logger = logging.getLogger(__name__)

CONTEXT_FILE = ROOT / "models" / "coll_context.parquet"


def select_context(storage, *, date_to: date, rows: int, lookback_weeks: int,
                   snapshot_id: str | None) -> tuple[pd.DataFrame, date]:
    """Most recent `rows` labelled rows on or before date_to (newest first, then loan_id)."""
    date_from = date_to - timedelta(weeks=lookback_weeks)
    ctx = storage.features(labelled=True, date_from=date_from, date_to=date_to, limit=rows,
                           snapshot_id=snapshot_id)
    if ctx.empty:
        raise ValueError(f"no labelled rows between {date_from} and {date_to}")
    return ctx, date_from


def save_context(ctx: pd.DataFrame, meta: dict, path: Path | None = None) -> None:
    path = path or CONTEXT_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    ctx[COLUMNS].to_parquet(path, index=False)
    path.with_suffix(".json").write_text(json.dumps(meta, default=str, indent=2))
    logger.info("saved %d context rows to %s", len(ctx), path)


def device_name(clf) -> str:
    dev = getattr(clf, "device_", None) or getattr(clf, "device", None)
    if dev is not None and not isinstance(dev, str):
        dev = getattr(dev, "type", str(dev))
    return str(dev or ("cuda" if has_cuda() else "cpu"))


def run_daily(storage, factory=new_classifier, run_date: date | None = None, triggered_by: str = "manual",
              write: bool = True, n_context: int | None = None, run_holdout: bool = True,
              save_context_file: bool = True) -> dict:
    t0 = time.time()
    pol = load_policy()
    horizon = int(pol["label"]["horizon_days"])
    lookback = int(pol["context"]["lookback_weeks"])
    n_context = n_context or context_rows()

    snap_id = storage.snapshot_id("collections_features")
    latest = storage.latest_snapshot_date(snap_id)
    run_date = run_date or latest
    known_to = run_date - timedelta(days=horizon)
    labelled = [d for d in storage.labelled_dates(snap_id) if d <= known_to]
    if not labelled:
        raise ValueError(f"no labelled snapshots on or before {known_to}")
    run_id = f"{run_date:%Y%m%d}-{uuid.uuid4().hex[:8]}"
    logger.info("run %s: scoring snapshot %s, labels known to %s, gold snapshot %s, context %d rows",
                run_id, run_date, labelled[-1], snap_id, n_context)

    summary, dec = {}, pd.DataFrame()
    test_from = test_to = None
    hctx_rows = None
    if run_holdout:
        h = pol["holdout"]
        test_from, test_to, hctx_to = ho.test_window(labelled, int(h["test_weeks"]), int(h["gap_days"]))
        test = storage.features(labelled=True, date_from=test_from, date_to=test_to, snapshot_id=snap_id)
        max_rows = int(h.get("max_rows", 0) or 0)
        if max_rows and len(test) > max_rows:
            test = test.sample(max_rows, random_state=7).reset_index(drop=True)
        hctx, _ = select_context(storage, date_to=hctx_to, rows=n_context, lookback_weeks=lookback,
                                 snapshot_id=snap_id)
        hctx_rows = len(hctx)
        p_test = predict_roll(build_model(hctx, factory), test)
        summary, dec = ho.summarise(test, p_test), ho.deciles(test, p_test)
        pct = lambda k: 100 * (summary[k] or 0)  # noqa: E731
        logger.info("holdout %s..%s: %d loans, roll rate %.1f%%, AUC %.3f | top 10%% of calls catch %.0f%% of "
                    "actual rolls (DPD alone %.0f%%), top 40%% catch %.0f%% (DPD alone %.0f%%), top 10%% catch "
                    "%.0f%% of rolled overdue INR", test_from, test_to, len(test), pct("holdout_roll_rate"),
                    summary["holdout_auc"] or 0, pct("capture_top10"), pct("dpd_only_capture_top10"),
                    pct("capture_top40"), pct("dpd_only_capture_top40"), pct("value_capture_top10"))

    ctx, ctx_from = select_context(storage, date_to=labelled[-1], rows=n_context, lookback_weeks=lookback,
                                   snapshot_id=snap_id)
    ctx_meta = {"run_id": run_id, "run_date": run_date, "context_from": ctx_from, "context_to": labelled[-1],
                "context_rows": len(ctx), "source_table": table("collections_features"),
                "source_snapshot_id": snap_id, "model_id": model_id(), "features": FEATURES}
    if save_context_file:
        save_context(ctx, ctx_meta)
    clf = build_model(ctx, factory)

    book = storage.features(date_from=run_date, date_to=run_date, snapshot_id=snap_id)
    if book.empty:
        raise ValueError(f"no SMA-0 rows for snapshot {run_date} (run dates must be snapshot dates)")
    calls = prioritise(book, predict_roll(clf, book))
    calls.insert(0, "run_id", run_id)
    calls.insert(0, "run_date", run_date)
    bands = calls["treatment"].value_counts().to_dict()
    logger.info("scored %d SMA-0 loans: %s; mean p_roll %.3f", len(calls), bands, calls["p_roll"].mean())

    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    model_run = pd.DataFrame([{
        "run_date": run_date, "run_id": run_id, "run_ts": now, "model_id": model_id(),
        "tabicl_version": tabicl_version(), "device": device_name(clf), "snapshot_date": run_date,
        "scored_loans": len(calls), "context_rows": len(ctx), "context_from": ctx_from,
        "context_to": labelled[-1], "source_table": table("collections_features"),
        "source_snapshot_id": snap_id, "holdout_test_from": test_from, "holdout_test_to": test_to,
        "holdout_context_rows": hctx_rows, **{k: summary.get(k) for k in (
            "holdout_rows", "holdout_roll_rate", "holdout_auc", "capture_top10", "capture_top40",
            "value_capture_top10", "dpd_only_capture_top10", "dpd_only_capture_top40")},
        "policy_json": json.dumps(pol, sort_keys=True), "triggered_by": triggered_by,
        "duration_s": round(time.time() - t0, 1),
    }])
    out = {"collections_call_list": calls, "collections_model_run": model_run}
    if not dec.empty:
        out["collections_holdout"] = dec.assign(run_date=run_date, run_id=run_id)
    if write:
        for key, df in out.items():
            storage.replace_run(key, df, run_date)
    out["summary"] = {**summary, "run_id": run_id, "run_date": run_date, "scored_loans": len(calls),
                      "bands": bands, "context_rows": len(ctx)}
    return out
