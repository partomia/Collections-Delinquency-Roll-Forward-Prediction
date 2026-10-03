#!/usr/bin/env python3
"""
CAI Job: daily roll-forward scoring of the SMA-0 book.

  1. Holdout: context from older weeks, test on the latest labelled weeks,
     prints "top 10% of calls catch X% of actual rolls".
  2. Context: most recent labelled rows (50k on GPU, 10k on CPU), saved to
     models/coll_context.parquet for the endpoint.
  3. Scores today's SMA-0 loans, ranks by p_roll x overdue amount, assigns
     treatment bands, writes gold.collections_call_list / _holdout / _model_run.

CAI job rsingh-coll-dlq-daily-score (ci/cai_jobs.py): Python 3.11 runtime,
4 vCPU / 16 GB, CPU only. Airflow passes the run date and trigger through the
environment (COLL_RUN_DATE, COLL_TRIGGERED_BY), since a job run ignores
arguments; the GitHub -> CAI check sets COLL_DRY_RUN=1.

  python cai/jobs/daily_score.py --dry-run                 # reads, scores, writes nothing
  python cai/jobs/daily_score.py --backend parquet --stub  # offline smoke run, no checkpoint
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from coll.model import StubClassifier, new_classifier  # noqa: E402
from coll.pipeline import run_daily  # noqa: E402
from coll.storage import get_storage  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", default=os.environ.get("COLL_RUN_DATE") or None,
                   help="snapshot date to score, YYYY-MM-DD (default: latest snapshot in gold)")
    p.add_argument("--backend", default=None, help="impala | parquet (default: config)")
    p.add_argument("--context-rows", type=int, default=None, help="override the policy context size")
    p.add_argument("--no-holdout", action="store_true")
    p.add_argument("--stub", action="store_true", help="logistic regression stand-in instead of TabICL")
    p.add_argument("--dry-run", action="store_true", default=os.environ.get("COLL_DRY_RUN") == "1",
                   help="do not write any table (env COLL_DRY_RUN=1: the GitHub -> CAI check)")
    p.add_argument("--triggered-by", default=os.environ.get("COLL_TRIGGERED_BY") or "cai-job")
    args, _ = p.parse_known_args()  # a Jupyter-kernel job runtime adds -f <kernel.json>

    run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date() if args.run_date else None
    out = run_daily(get_storage(args.backend), factory=StubClassifier if args.stub else new_classifier,
                    run_date=run_date, triggered_by=args.triggered_by, write=not args.dry_run,
                    n_context=args.context_rows, run_holdout=not args.no_holdout)
    s = out["summary"]
    print(f"\nrun {s['run_id']}: {s['scored_loans']} SMA-0 loans scored, bands {s['bands']}")
    if s.get("capture_top10") is not None:
        print(f"holdout AUC {s['holdout_auc']:.3f} | top 10% of calls catch {s['capture_top10']:.0%} of actual rolls "
              f"(DPD alone {s['dpd_only_capture_top10']:.0%}), top 40% catch {s['capture_top40']:.0%} "
              f"(DPD alone {s['dpd_only_capture_top40']:.0%}); top 10% catch {s['value_capture_top10']:.0%} "
              f"of rolled overdue INR")
    print(out["collections_call_list"].head(10)[["loan_id", "dpd_now", "overdue_amount", "p_roll",
                                                  "priority_score", "treatment", "risk_signals"]].to_string(index=False))


if __name__ == "__main__":
    main()
