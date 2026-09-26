import numpy as np
import pandas as pd

from coll.prioritise import assign_bands, prioritise
from tests.conftest import synthetic_features

BANDS = [{"code": "A", "upto_pct": 0.10}, {"code": "B", "upto_pct": 0.40}, {"code": "C", "upto_pct": 1.0}]


def test_priority_is_p_times_overdue_and_sorted():
    df = synthetic_features(weeks=1, per_week=200)
    p = np.linspace(0.01, 0.99, len(df))
    out = prioritise(df, p, BANDS)
    assert (np.diff(out["priority_score"]) <= 0).all()
    merged = out.merge(df[["loan_id", "overdue_amount"]], on="loan_id", suffixes=("", "_src"))
    assert np.allclose(merged["priority_score"], (merged["p_roll"] * merged["overdue_amount_src"]).round(2))
    assert list(out["priority_rank"]) == list(range(1, len(df) + 1))


def test_band_shares():
    df = synthetic_features(weeks=1, per_week=1000)
    out = prioritise(df, np.random.default_rng(1).random(len(df)), BANDS)
    shares = out["treatment"].value_counts(normalize=True)
    assert abs(shares["A"] - 0.10) < 0.002 and abs(shares["B"] - 0.30) < 0.002
    assert out.loc[out["treatment"] == "A", "priority_score"].min() >= out.loc[
        out["treatment"] == "B", "priority_score"].max()


def test_assign_bands_bounds():
    codes, _ = assign_bands(pd.Series([0.05, 0.10, 0.11, 0.40, 0.41, 1.0]), BANDS)
    assert list(codes) == ["A", "A", "B", "B", "C", "C"]


def test_risk_signals_present():
    df = synthetic_features(weeks=1, per_week=50)
    df.loc[0, ["dpd_now", "broken_ptp_3m", "salary_credit_last_30d"]] = [25, 2, 0]
    out = prioritise(df, np.full(len(df), 0.5), BANDS)
    s = out.set_index("loan_id").loc[df.loc[0, "loan_id"], "risk_signals"]
    assert "25 DPD" in s and "2 broken PTP" in s
