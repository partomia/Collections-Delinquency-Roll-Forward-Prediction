from datetime import date, timedelta

import pandas as pd

from coll.features import FEATURES, LABEL
from coll.model import StubClassifier
from coll.pipeline import run_daily


def test_run_daily_writes_outputs(parquet_storage):
    out = run_daily(parquet_storage, factory=StubClassifier, n_context=2000)
    calls = out["collections_call_list"]
    assert len(calls) == 300 and set(calls["snapshot_date"]) == {date(2026, 9, 25)}
    run = out["collections_model_run"].iloc[0]
    # labels known to run_date - 30 days; context never goes past that
    assert run["context_to"] <= date(2026, 9, 25) - timedelta(days=30)
    assert run["holdout_test_to"] == run["context_to"]
    assert run["holdout_auc"] > 0.6
    assert run["context_rows"] == 2000
    stored = parquet_storage.read("collections_call_list")
    assert len(stored) == 300
    # a rerun for the same day replaces, never duplicates
    run_daily(parquet_storage, factory=StubClassifier, n_context=2000)
    assert len(parquet_storage.read("collections_call_list")) == 300
    assert len(parquet_storage.read("collections_model_run")) == 1
    assert len(parquet_storage.read("collections_holdout")) == 10


def test_backfill_run_uses_only_known_labels(parquet_storage):
    run_date = date(2026, 8, 7)
    out = run_daily(parquet_storage, factory=StubClassifier, run_date=run_date, write=False,
                    n_context=5000, save_context_file=False)
    run = out["collections_model_run"].iloc[0]
    assert run["context_to"] <= run_date - timedelta(days=30)
    assert set(out["collections_call_list"]["snapshot_date"]) == {run_date}


def test_context_is_most_recent_rows(parquet_storage, tmp_path):
    run_daily(parquet_storage, factory=StubClassifier, n_context=500, run_holdout=False)
    ctx = pd.read_parquet(tmp_path / "models" / "coll_context.parquet")
    assert len(ctx) == 500 and ctx[LABEL].notna().all()
    assert set(FEATURES) <= set(ctx.columns)
    assert ctx["snapshot_date"].nunique() == 2  # 300 rows per week, newest first


def test_feature_lists_match_cde_job(gold_job):
    assert gold_job.FEATURES == FEATURES and gold_job.LABEL == LABEL
