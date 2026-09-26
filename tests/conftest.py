import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from coll.features import COLUMNS, LABEL  # noqa: E402


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def generator():
    return _load(ROOT / "cde" / "jobs" / "generate_loan_bronze.py")


@pytest.fixture(scope="session")
def gold_job():
    pytest.importorskip("pyspark")
    return _load(ROOT / "cde" / "jobs" / "build_gold_features.py")


def synthetic_features(weeks: int = 30, per_week: int = 300, seed: int = 0,
                       last: date = date(2026, 9, 25)) -> pd.DataFrame:
    """Small gold-like feature table: roll probability rises with dpd and bounces,
    falls with a salary credit. The last 5 weeks (30-day horizon) are unlabelled."""
    rng = np.random.default_rng(seed)
    rows = []
    for w in range(weeks):
        d = last - timedelta(weeks=weeks - 1 - w)
        n = per_week
        dpd = rng.integers(1, 31, n)
        bounces = rng.poisson(1.0, n)
        sal = rng.integers(0, 2, n)
        emi = rng.choice([3000.0, 9000.0, 28000.0], n)
        logit = -3.0 + 0.12 * dpd + 0.5 * bounces - 1.0 * sal
        y = (rng.random(n) < 1 / (1 + np.exp(-logit))).astype(float)
        labelled = d + timedelta(days=30) <= last
        for i in range(n):
            rows.append({
                "loan_id": f"L{w:02d}{i:04d}", "snapshot_date": d, "product_code": int(rng.integers(1, 6)),
                "dpd_now": int(dpd[i]), "overdue_amount": float(emi[i] + 590 * min(bounces[i], 2)),
                "emi_amount": float(emi[i]), "overdue_to_emi": 1.0, "months_on_book": int(rng.integers(3, 60)),
                "max_dpd_12m": int(dpd[i] + rng.integers(0, 20)), "nach_bounces_6m": int(bounces[i]),
                "broken_ptp_3m": int(rng.integers(0, 2)), "contacts_reached_30d": int(rng.integers(0, 3)),
                "salary_credit_last_30d": int(sal[i]), "salary_account_flag": 1,
                "bureau_score": int(rng.integers(550, 850)), LABEL: y[i] if labelled else np.nan,
            })
    return pd.DataFrame(rows)[COLUMNS]


@pytest.fixture()
def parquet_storage(tmp_path, monkeypatch):
    """ParquetStorage over a temp dir holding a synthetic features table."""
    from coll.config import table
    from coll.storage import ParquetStorage

    df = synthetic_features()
    df.to_parquet(tmp_path / f"{table('collections_features').split('.')[-1]}.parquet", index=False)
    monkeypatch.setattr("coll.pipeline.CONTEXT_FILE", tmp_path / "models" / "coll_context.parquet")
    return ParquetStorage(tmp_path)
