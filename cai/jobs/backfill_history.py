#!/usr/bin/env python3
"""
One-off: run the daily scoring for the last N weekly snapshot dates, oldest
first, so the app has a history of call lists and holdout results from day
one. Each run only uses labels known on its own run date (see coll/pipeline.py),
so the history is what the job would have produced on those days.

  python cai/jobs/backfill_history.py --weeks 8
  python cai/jobs/backfill_history.py --weeks 4 --backend parquet --stub
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
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
    p.add_argument("--weeks", type=int, default=8)
    p.add_argument("--backend", default=None)
    p.add_argument("--context-rows", type=int, default=None)
    p.add_argument("--stub", action="store_true")
    args = p.parse_args()

    storage = get_storage(args.backend)
    snap = storage.snapshot_id("collections_features")
    latest = storage.latest_snapshot_date(snap)
    fridays = sorted({d for d in storage.labelled_dates(snap) if d.weekday() == 4})
    # the latest snapshots are unlabelled but still scoreable; take them from the full date list
    all_dates = sorted(set(storage.features(date_from=fridays[-1], snapshot_id=snap)["snapshot_date"]))
    dates = sorted({*fridays, *[d for d in all_dates if d.weekday() == 4 and d < latest]})[-args.weeks:]
    factory = StubClassifier if args.stub else new_classifier
    for d in dates:
        out = run_daily(storage, factory=factory, run_date=d, triggered_by="backfill",
                        n_context=args.context_rows, save_context_file=False)
        s = out["summary"]
        print(f"{d}: {s['scored_loans']} loans, holdout AUC {s.get('holdout_auc')}, "
              f"top-10% capture {s.get('capture_top10')}")


if __name__ == "__main__":
    main()
