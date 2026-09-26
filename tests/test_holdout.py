from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from coll import holdout as ho
from coll.features import LABEL


def test_test_window_has_gap():
    dates = [date(2026, 1, 2) + timedelta(weeks=w) for w in range(20)]
    t_from, t_to, ctx_to = ho.test_window(dates, 4, 30)
    assert t_to == dates[-1] and t_from == dates[-4]
    assert ctx_to == t_from - timedelta(days=30)
    with pytest.raises(ValueError):
        ho.test_window(dates[:3], 4, 30)


def test_capture_perfect_and_random():
    y = np.array([1] * 10 + [0] * 90)
    assert ho.capture_at(y, y.astype(float), 0.10) == 1.0
    assert ho.capture_at(y, -y.astype(float), 0.10) == 0.0
    assert ho.capture_at(np.zeros(10), np.arange(10), 0.1) is None


def test_value_capture_weights_amount():
    y = np.array([1, 1, 0, 0])
    amt = np.array([100.0, 900.0, 1.0, 1.0])
    score = np.array([0.9, 0.1, 0.5, 0.4])  # top 25% = the small roll only
    assert ho.capture_at(y, score, 0.25, amt) == pytest.approx(0.1)


def test_deciles_cum_capture_ends_at_one():
    rng = np.random.default_rng(0)
    test = pd.DataFrame({LABEL: rng.integers(0, 2, 500)})
    d = ho.deciles(test, rng.random(500))
    assert list(d["decile"]) == list(range(1, 11))
    assert d["loans"].sum() == 500
    assert d["cum_capture"].iloc[-1] == pytest.approx(1.0)
