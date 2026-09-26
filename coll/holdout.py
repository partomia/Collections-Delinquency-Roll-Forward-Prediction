"""Holdout check: score the latest labelled weeks with a context built only
from what was known then.

The test set is the latest `test_weeks` labelled snapshots. Context rows come
from snapshots at least `gap_days` before the first test snapshot, so every
context label was observed before the test date, as if the model had been
deployed on that day.

The talking point for a collections head is capture, not AUC: "the top 10% of
calls catch X% of the loans that actually rolled".
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from coll.features import LABEL


def test_window(labelled_dates: list[date], test_weeks: int, gap_days: int) -> tuple[date, date, date]:
    """(test_from, test_to, context_to) from the distinct labelled snapshot dates."""
    dates = sorted(set(labelled_dates))
    if len(dates) < test_weeks + 1:
        raise ValueError(f"need more than {test_weeks} labelled snapshots, have {len(dates)}")
    test = dates[-test_weeks:]
    return test[0], test[-1], test[0] - timedelta(days=gap_days)


def capture_at(y: np.ndarray, score: np.ndarray, share: float, weight: np.ndarray | None = None) -> float | None:
    """Share of actual rolls (or of rolled overdue amount) in the top `share` by score."""
    y = np.asarray(y, dtype=float)
    w = y if weight is None else y * np.asarray(weight, dtype=float)
    if w.sum() <= 0:
        return None
    k = max(1, int(round(len(y) * share)))
    top = np.argsort(-np.asarray(score, dtype=float), kind="stable")[:k]
    return float(w[top].sum() / w.sum())


def auc(y: np.ndarray, p: np.ndarray) -> float | None:
    from sklearn.metrics import roc_auc_score

    y = np.asarray(y, dtype=int)
    if y.min() == y.max():
        return None
    return float(roc_auc_score(y, p))


def summarise(test: pd.DataFrame, p: np.ndarray) -> dict:
    y = test[LABEL].to_numpy(dtype=int)
    prio = p * test["overdue_amount"].to_numpy(dtype=float)
    overdue = test["overdue_amount"].to_numpy(dtype=float)
    r = lambda x: None if x is None else round(x, 4)  # noqa: E731
    return {
        "holdout_rows": int(len(test)),
        "holdout_roll_rate": r(float(y.mean())) if len(y) else None,
        "holdout_auc": r(auc(y, p)),
        "capture_top10": r(capture_at(y, p, 0.10)),
        "capture_top40": r(capture_at(y, p, 0.40)),
        "value_capture_top10": r(capture_at(y, prio, 0.10, overdue)),
        "dpd_only_capture_top10": r(capture_at(y, test["dpd_now"].to_numpy(dtype=float), 0.10)),
    }


def deciles(test: pd.DataFrame, p: np.ndarray) -> pd.DataFrame:
    """Rows, rolls and cumulative capture by decile of p_roll (1 = highest)."""
    df = pd.DataFrame({"p": p, "y": test[LABEL].to_numpy(dtype=int)}).sort_values("p", ascending=False, kind="stable")
    df["decile"] = (np.arange(len(df)) * 10 // max(len(df), 1)) + 1
    g = df.groupby("decile").agg(rows=("y", "size"), rolls=("y", "sum"), mean_p_roll=("p", "mean")).reset_index()
    total = max(int(g["rolls"].sum()), 1)
    g["roll_rate"] = (g["rolls"] / g["rows"]).round(4)
    g["cum_capture"] = (g["rolls"].cumsum() / total).round(4)
    g["mean_p_roll"] = g["mean_p_roll"].round(4)
    return g
