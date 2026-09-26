import copy
import sys
from pathlib import Path

import pytest

from coll.features import FEATURES, LABEL
from coll.model import StubClassifier, build_model
from coll.scoring import score, validate
from tests.conftest import synthetic_features

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "cai" / "model"))
from test_endpoint import EXAMPLE  # noqa: E402


@pytest.fixture(scope="module")
def clf():
    df = synthetic_features(weeks=10)
    return build_model(df[df[LABEL].notna()], StubClassifier)


def test_example_request_scores(clf):
    resp = score(EXAMPLE, clf, {"model_id": "stub"})
    s = resp["scores"][0]
    assert s["loan_id"] == "PL-0552190" and 0 < s["p_roll"] < 1
    assert abs(s["priority_score"] - s["p_roll"] * 24500) <= 3
    # salary credit lowers the roll probability in the synthetic fixture
    assert s["p_roll_what_if"] < s["p_roll"]


def test_no_what_if_fields_without_what_if(clf):
    req = {"loans": EXAMPLE["loans"]}
    assert "p_roll_what_if" not in score(req, clf)["scores"][0]


@pytest.mark.parametrize("mutate, message", [
    (lambda r: r.pop("loans"), "send"),
    (lambda r: r["loans"][0].pop("dpd_now"), "missing fields"),
    (lambda r: r["loans"][0].update(dpd_now="x"), "numbers"),
    (lambda r: r["loans"][0].update(dpd_now=float("nan")), "finite"),
    (lambda r: r.update(what_if={"not_a_feature": 1}), "unknown"),
])
def test_validation_errors(mutate, message):
    req = copy.deepcopy(EXAMPLE)
    mutate(req)
    err = validate(req)
    assert err and message in err


def test_example_has_every_feature():
    assert set(FEATURES) <= set(EXAMPLE["loans"][0])
