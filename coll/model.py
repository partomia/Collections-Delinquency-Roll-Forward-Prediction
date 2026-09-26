"""TabICL v2 classifier setup.

TabICL learns in context: fit() only stores the labelled rows (and, with
kv_cache, precomputes their representations); the learning happens inside
predict_proba. So "the model" is the pinned checkpoint plus the context rows,
and both are recorded for every run.
"""

from __future__ import annotations

import logging
from importlib.metadata import PackageNotFoundError, version

import numpy as np
import pandas as pd

from coll.config import policy, settings
from coll.features import FEATURES, LABEL

logger = logging.getLogger(__name__)


def has_cuda() -> bool:
    try:
        import torch

        return torch.cuda.is_available()
    except ImportError:
        return False


def context_rows() -> int:
    ctx = policy()["context"]
    return int(ctx["rows_gpu"] if has_cuda() else ctx["rows_cpu"])


def tabicl_version() -> str | None:
    try:
        return version("tabicl")
    except PackageNotFoundError:
        return None


def new_classifier(kv_cache: bool | str = False):
    from tabicl import TabICLClassifier

    cfg = settings()["model"]
    kwargs = dict(n_estimators=int(cfg["n_estimators"]), checkpoint_version=cfg["checkpoint_version"],
                  random_state=int(cfg["random_state"]), kv_cache=kv_cache)
    if cfg["model_path"]:
        kwargs.update(model_path=cfg["model_path"], allow_auto_download=False)
    if cfg["device"]:
        kwargs["device"] = cfg["device"]
    return TabICLClassifier(**kwargs)


def build_model(ctx: pd.DataFrame, factory=new_classifier, **kwargs):
    """Fit on the context rows: stores them, no training."""
    clf = factory(**kwargs)
    clf.fit(ctx[FEATURES].astype(float), ctx[LABEL].astype(int))
    return clf


def predict_roll(clf, df: pd.DataFrame, batch: int = 4096) -> np.ndarray:
    """P(roll to SMA-1 within 30 days), scored in batches to bound GPU memory."""
    X = df[FEATURES].astype(float)
    out = [clf.predict_proba(X.iloc[i:i + batch])[:, 1] for i in range(0, len(X), batch)]
    return np.concatenate(out) if out else np.array([])


def model_id() -> str:
    cfg = settings()["model"]
    return f"{cfg['hf_repo']}/{cfg['checkpoint_version']}"


class StubClassifier:
    """Stand-in with the fit / predict_proba interface, for tests and quick
    smoke runs without the checkpoint: logistic regression on standardised
    features."""

    def __init__(self, **_):
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        self._clf = make_pipeline(StandardScaler(), LogisticRegression(max_iter=500))

    def fit(self, X, y):
        self._clf.fit(np.asarray(X, dtype=float), np.asarray(y, dtype=int))
        return self

    def predict_proba(self, X):
        return self._clf.predict_proba(np.asarray(X, dtype=float))
