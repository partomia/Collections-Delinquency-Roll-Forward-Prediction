"""Call priority and treatment bands for the SMA-0 book.

priority_score = p_roll x overdue_amount, so a likely roll on a large loan
outranks a likely roll on a small one. Treatment bands are percentiles of the
priority across the whole book, with cut-offs from config/policy.yaml (business
settings, not model settings), so they come from the daily call list rather
than from a single-loan request.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from coll.config import policy

CALL_COLUMNS = ["loan_id", "snapshot_date", "product_code", "dpd_now", "overdue_amount", "emi_amount"]


def bands_from_policy() -> list[dict]:
    return policy()["treatment"]["bands"]


def assign_bands(priority_pct: pd.Series, bands: list[dict]) -> tuple[pd.Series, pd.Series]:
    """priority_pct in (0, 1], 0 = highest priority. Returns (treatment, action)."""
    codes = np.full(len(priority_pct), bands[-1]["code"], dtype=object)
    actions = np.full(len(priority_pct), bands[-1].get("action", ""), dtype=object)
    pct = priority_pct.to_numpy()
    for band in reversed(bands):
        mask = pct <= float(band["upto_pct"])
        codes[mask] = band["code"]
        actions[mask] = band.get("action", "")
    return pd.Series(codes, index=priority_pct.index), pd.Series(actions, index=priority_pct.index)


def risk_signals(row) -> str:
    """Plain-language flags a collector can act on. Business rules on the
    inputs, not an explanation of the model's score."""
    out = []
    if row.dpd_now >= 20:
        out.append(f"{int(row.dpd_now)} DPD, close to SMA-1")
    if row.broken_ptp_3m >= 1:
        out.append(f"{int(row.broken_ptp_3m)} broken PTP in 3m")
    if row.nach_bounces_6m >= 2:
        out.append(f"{int(row.nach_bounces_6m)} NACH bounces in 6m")
    if row.salary_account_flag == 1 and row.salary_credit_last_30d == 0:
        out.append("no salary credit in 30d")
    if row.max_dpd_12m >= 31:
        out.append(f"was {int(row.max_dpd_12m)} DPD in last 12m")
    if row.contacts_reached_30d == 0:
        out.append("not reached in 30d")
    if row.bureau_score < 650:
        out.append(f"bureau {int(row.bureau_score)}")
    return "; ".join(out[:3])


def prioritise(df: pd.DataFrame, p_roll, bands: list[dict] | None = None) -> pd.DataFrame:
    bands = bands or bands_from_policy()
    out = df[CALL_COLUMNS].copy().reset_index(drop=True)
    out["p_roll"] = np.round(np.asarray(p_roll, dtype=float), 4)
    out["priority_score"] = np.round(out["p_roll"] * out["overdue_amount"], 2)
    out = out.sort_values(["priority_score", "loan_id"], ascending=[False, True]).reset_index(drop=True)
    out["priority_rank"] = np.arange(1, len(out) + 1)
    out["priority_pct"] = np.round(out["priority_rank"] / max(len(out), 1), 6)
    out["treatment"], out["action"] = assign_bands(out["priority_pct"], bands)
    signals = df.set_index("loan_id").apply(risk_signals, axis=1)
    out["risk_signals"] = out["loan_id"].map(signals)
    return out
