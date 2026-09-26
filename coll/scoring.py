"""Request handling shared by the CAI model endpoint and the app's in-process mode.

Request:
  {"loans": [{"loan_id": "PL-0552190", "product_code": 1, "dpd_now": 18, "overdue_amount": 24500,
              "emi_amount": 12250, "overdue_to_emi": 2.0, "months_on_book": 14, "max_dpd_12m": 22,
              "nach_bounces_6m": 3, "broken_ptp_3m": 1, "contacts_reached_30d": 0,
              "salary_credit_last_30d": 0, "salary_account_flag": 1, "bureau_score": 671}],
   "what_if": {"salary_credit_last_30d": 1, "contacts_reached_30d": 2}}   # optional
Response:
  {"scores": [{"loan_id", "p_roll", "priority_score", "risk_signals",
               "p_roll_what_if", "priority_score_what_if"}],        # what-if fields only if sent
   "model": {"model_id", "run_id", "context_rows", "context_to", "source_snapshot_id"}}

Treatment bands are percentiles across the whole book, so they come from the
daily call list, not from a single-loan request.
"""

from __future__ import annotations

import json
import logging
import math

import pandas as pd

from coll.features import COLUMNS, FEATURES
from coll.model import build_model, model_id, new_classifier, predict_roll
from coll.prioritise import risk_signals

logger = logging.getLogger(__name__)

MAX_LOANS = 1000


def validate(req: dict) -> str | None:
    loans = req.get("loans") if isinstance(req, dict) else None
    if not loans or not isinstance(loans, list):
        return "send {'loans': [ {...}, ... ]}"
    if len(loans) > MAX_LOANS:
        return f"at most {MAX_LOANS} loans per request"
    for i, loan in enumerate(loans):
        if not isinstance(loan, dict):
            return f"loans[{i}] must be an object"
        missing = [f for f in FEATURES if f not in loan]
        if missing:
            return f"loans[{i}] ({loan.get('loan_id', '?')}): missing fields {missing}"
        try:
            if any(not math.isfinite(float(loan[f])) for f in FEATURES):
                return f"loans[{i}]: feature values must be finite numbers"
        except (TypeError, ValueError):
            return f"loans[{i}]: feature values must be numbers"
    what_if = req.get("what_if")
    if what_if is not None:
        if not isinstance(what_if, dict) or not what_if:
            return "what_if must be an object of feature overrides"
        unknown = [k for k in what_if if k not in FEATURES]
        if unknown:
            return f"what_if: unknown features {unknown}"
        try:
            [float(v) for v in what_if.values()]
        except (TypeError, ValueError):
            return "what_if values must be numbers"
    return None


def _frame(loans: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(loans)
    if "loan_id" not in df:
        df["loan_id"] = [f"loan-{i}" for i in range(len(df))]
    df[FEATURES] = df[FEATURES].astype(float)
    return df


def score(req: dict, clf, meta: dict | None = None) -> dict:
    err = validate(req)
    if err:
        return {"error": err}
    df = _frame(req["loans"])
    p = predict_roll(clf, df)
    df["p_roll"] = p
    what_if = req.get("what_if")
    if what_if:
        alt = df.copy()
        for k, v in what_if.items():
            alt[k] = float(v)
        if "overdue_amount" in what_if or "emi_amount" in what_if:
            alt["overdue_to_emi"] = (alt["overdue_amount"] / alt["emi_amount"]).round(3)
        df["p_roll_what_if"] = predict_roll(clf, alt)
        df["overdue_what_if"] = alt["overdue_amount"]
    scores = []
    for r in df.itertuples():
        s = {"loan_id": str(r.loan_id), "p_roll": round(float(r.p_roll), 4),
             "priority_score": round(float(r.p_roll) * float(r.overdue_amount), 0),
             "risk_signals": risk_signals(r)}
        if what_if:
            s["p_roll_what_if"] = round(float(r.p_roll_what_if), 4)
            s["priority_score_what_if"] = round(float(r.p_roll_what_if) * float(r.overdue_what_if), 0)
        scores.append(s)
    return {"scores": scores, "model": meta or {"model_id": model_id()}}


def load_context(source: str = "auto") -> tuple[pd.DataFrame, dict]:
    """Context for the endpoint: rebuilt from gold exactly as the latest daily
    run selected it (same Iceberg snapshot, window and row count), or the
    parquet file that run saved.

    source: impala | file | auto (impala if credentials are set, else file).
    """
    from coll.config import settings
    from coll.pipeline import CONTEXT_FILE

    imp = settings()["impala"]
    if source == "impala" or (source == "auto" and imp["user"] and imp["password"]):
        from coll.storage import get_storage

        storage = get_storage("impala")
        runs = storage.read("collections_model_run")
        if runs.empty:
            raise RuntimeError("no daily run in collections_model_run yet: run cai/jobs/daily_score.py first")
        run = runs.sort_values("run_ts").iloc[-1]
        ctx = storage.features(labelled=True, date_from=pd.Timestamp(run["context_from"]).date(),
                               date_to=pd.Timestamp(run["context_to"]).date(), limit=int(run["context_rows"]),
                               snapshot_id=run["source_snapshot_id"] or None)
        meta = {k: (str(run[k]) if run[k] is not None else None) for k in
                ("run_id", "run_date", "context_from", "context_to", "source_snapshot_id")}
        meta["source"] = "impala"
    else:
        if not CONTEXT_FILE.exists():
            raise RuntimeError(f"{CONTEXT_FILE} not found: run cai/jobs/daily_score.py first")
        ctx = pd.read_parquet(CONTEXT_FILE)
        meta_file = CONTEXT_FILE.with_suffix(".json")
        meta = json.loads(meta_file.read_text()) if meta_file.exists() else {}
        meta = {k: meta.get(k) for k in ("run_id", "run_date", "context_from", "context_to", "source_snapshot_id")}
        meta["source"] = "file"
    meta.update(model_id=model_id(), context_rows=len(ctx))
    logger.info("endpoint context: %d rows from %s (run %s)", len(ctx), meta["source"], meta.get("run_id"))
    return ctx[COLUMNS], meta


def load_scorer(source: str = "auto", factory=new_classifier, kv_cache: bool | str | None = None):
    """(classifier fitted on the context, meta). With kv_cache the context is
    encoded once at startup, so each request only pays for its own rows."""
    from coll.config import settings

    ctx, meta = load_context(source)
    if kv_cache is None:
        kv_cache = settings()["model"]["endpoint_kv_cache"] or False
    kwargs = {"kv_cache": kv_cache} if factory is new_classifier else {}
    return build_model(ctx, factory, **kwargs), meta
