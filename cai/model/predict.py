"""
CAI Model Deployment: roll probability for loans sent on demand, e.g. when a
collector opens an account or a what-if changes salary_credit_last_30d.

Model rsingh-coll-dlq-roll-scorer (ci/cai_jobs.py, created by ci/setup_cai.py):
File cai/model/predict.py, Function predict, Python 3.11 runtime, 4 vCPU / 16 GB,
CPU only. The model build runs cdsw-build.sh.

At start-up each replica rebuilds the context of the latest daily run from
gold via Impala (same Iceberg snapshot, window and row count; needs
COLL_IMPALA_USER / COLL_IMPALA_PASSWORD in the model's environment), or reads
models/coll_context.parquet if Impala is not configured. Restart the model to
pick up a newer daily run. Request/response format: see coll/scoring.py.
"""

import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from coll.scoring import load_scorer, score  # noqa: E402

try:
    import cml.models_v1 as models

    cml_model = models.cml_model
except ImportError:  # outside CAI (local tests)
    def cml_model(fn):
        return fn

CLF, META = load_scorer(os.environ.get("COLL_ENDPOINT_CONTEXT", "auto"))


@cml_model
def predict(args):
    return score(args, CLF, META)
