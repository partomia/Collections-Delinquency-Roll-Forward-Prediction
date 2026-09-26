"""Call the loan scorer: the CAI model endpoint if configured, otherwise
in-process (TabICL on the saved context, or the logistic stand-in if TabICL is
not installed, so the app still runs on a light host)."""

from __future__ import annotations

import logging

import requests

from coll.config import settings
from coll.scoring import score

logger = logging.getLogger(__name__)
_local = None


def endpoint_configured() -> bool:
    return bool(settings()["endpoint"]["url"])


def call_endpoint(req: dict, timeout: int = 120) -> dict:
    ep = settings()["endpoint"]
    headers = {"Content-Type": "application/json"}
    if ep["api_key"]:
        headers["Authorization"] = f"Bearer {ep['api_key']}"
    resp = requests.post(ep["url"], json={"accessKey": ep["access_key"], "request": req},
                         headers=headers, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    if isinstance(body, dict) and "response" in body:
        return body["response"]
    return body


def local_scorer(source: str = "file"):
    global _local
    if _local is None:
        from coll.scoring import load_scorer

        try:
            import tabicl  # noqa: F401

            _local = load_scorer(source)
        except ImportError:
            from coll.model import StubClassifier

            logger.warning("tabicl not installed: using the logistic regression stand-in")
            _local = load_scorer(source, factory=StubClassifier)
    return _local


def score_loans(req: dict) -> tuple[dict, str]:
    """Returns (response, source) where source is 'endpoint' or 'in-process'."""
    if endpoint_configured():
        return call_endpoint(req), "endpoint"
    clf, meta = local_scorer()
    return score(req, clf, meta), "in-process"
