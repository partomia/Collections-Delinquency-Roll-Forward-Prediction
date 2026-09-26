"""Headless render of every tab of the Streamlit app against a small parquet
lakehouse scored with the logistic stand-in (no TabICL checkpoint needed)."""

from datetime import date

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from coll.config import ROOT  # noqa: E402
from coll.model import StubClassifier  # noqa: E402
from coll.pipeline import run_daily  # noqa: E402


@pytest.fixture
def parquet_env(parquet_storage, monkeypatch):
    for d in (date(2026, 9, 18), date(2026, 9, 25)):
        run_daily(parquet_storage, factory=StubClassifier, run_date=d, n_context=2000)
    monkeypatch.setenv("COLL_STORAGE_BACKEND", "parquet")
    monkeypatch.setenv("COLL_STORAGE_PARQUET_DIR", str(parquet_storage.dir))
    monkeypatch.setenv("COLL_ENDPOINT_URL", "")
    from coll import client, config
    from coll.scoring import load_scorer

    config.settings.cache_clear()
    monkeypatch.setattr(client, "_local", load_scorer("file", factory=StubClassifier))
    yield
    config.settings.cache_clear()


def test_app_renders_what_if_and_outcome(parquet_env, parquet_storage):
    at = AppTest.from_file(str(ROOT / "app" / "streamlit_app.py"), default_timeout=60)
    at.run()
    assert not at.exception, at.exception
    assert any("SMA-0 loans scored" in m.label for m in at.metric)
    assert any("Top 10% of calls catch" in m.label for m in at.metric)

    at.button(key="whatif_score").click().run()
    assert not at.exception, at.exception
    assert any("with changes" in m.label for m in at.metric)

    next(b for b in at.button if b.label == "Save outcome").click().run()
    assert not at.exception, at.exception
    assert len(parquet_storage.read("collector_outcomes")) == 1
