#!/usr/bin/env python3
"""
Try the scorer with the demo loan, then the same loan after a salary credit and
two reached contacts (the live what-if from the demo).

  python cai/model/test_endpoint.py --print-request      # JSON for the model's Test tab
  python cai/model/test_endpoint.py --local              # in-process, context from models/
  python cai/model/test_endpoint.py --local --stub       # no checkpoint needed
  python cai/model/test_endpoint.py                      # the deployed endpoint (COLL_ENDPOINT_*)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

EXAMPLE = {
    "loans": [{
        "loan_id": "PL-0552190", "product_code": 1, "dpd_now": 18, "overdue_amount": 24500,
        "emi_amount": 12250, "overdue_to_emi": 2.0, "months_on_book": 14, "max_dpd_12m": 22,
        "nach_bounces_6m": 3, "broken_ptp_3m": 1, "contacts_reached_30d": 0,
        "salary_credit_last_30d": 0, "salary_account_flag": 1, "bureau_score": 671,
    }],
    "what_if": {"salary_credit_last_30d": 1, "contacts_reached_30d": 2},
}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--print-request", action="store_true")
    p.add_argument("--local", action="store_true", help="score in-process instead of calling the endpoint")
    p.add_argument("--stub", action="store_true", help="with --local: logistic stand-in instead of TabICL")
    p.add_argument("--context", default="file", help="with --local: impala | file | auto")
    args = p.parse_args()

    if args.print_request:
        print(json.dumps(EXAMPLE, indent=2))
        return
    t0 = time.time()
    if args.local:
        from coll.model import StubClassifier, new_classifier
        from coll.scoring import load_scorer, score

        clf, meta = load_scorer(args.context, factory=StubClassifier if args.stub else new_classifier)
        t1 = time.time()
        resp = score(EXAMPLE, clf, meta)
        print(f"context built in {t1 - t0:.1f}s, request scored in {time.time() - t1:.2f}s")
    else:
        from coll.client import call_endpoint, endpoint_configured

        if not endpoint_configured():
            sys.exit("COLL_ENDPOINT_URL is not set (or use --local)")
        resp = call_endpoint(EXAMPLE)
        print(f"endpoint answered in {time.time() - t0:.2f}s")
    print(json.dumps(resp, indent=2))


if __name__ == "__main__":
    main()
