#!/usr/bin/env python3
"""
One-off: run the daily scoring for the last N weekly snapshot dates, oldest
first, so the app has a history of call lists and holdout results from day
one. Each run only uses labels known on its own run date (see coll/pipeline.py),
so the history is what the job would have produced on those days. Each date
runs cai/jobs/daily_score.py in its own process (16 GB engine on CPU).

  python cai/jobs/backfill_history.py --weeks 8
  python cai/jobs/backfill_history.py --weeks 4 --backend parquet --stub

As the CAI job rsingh-coll-dlq-backfill-history (ci/cai_jobs.py), a run ignores
arguments: set COLL_BACKFILL_WEEKS in the run's environment (default 8).
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from coll.storage import get_storage  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--weeks", type=int, default=int(os.environ.get("COLL_BACKFILL_WEEKS") or 8))
    p.add_argument("--backend", default=None)
    p.add_argument("--context-rows", type=int, default=None)
    p.add_argument("--stub", action="store_true")
    args, _ = p.parse_known_args()  # a Jupyter-kernel job runtime adds -f <kernel.json>

    storage = get_storage(args.backend)
    snap = storage.snapshot_id("collections_features")
    latest = storage.latest_snapshot_date(snap)
    fridays = sorted({d for d in storage.labelled_dates(snap) if d.weekday() == 4})
    # the latest snapshots are unlabelled but still scoreable; take them from the full date list
    all_dates = sorted(set(storage.features(date_from=fridays[-1], snapshot_id=snap)["snapshot_date"]))
    dates = sorted({*fridays, *[d for d in all_dates if d.weekday() == 4 and d < latest]})[-args.weeks:]
    print("backfill dates:", ", ".join(d.isoformat() for d in dates), flush=True)
    for d in dates:
        run_one(d, args)


def run_one(d, args) -> None:
    """One date in a fresh process: memory is released after every date, and a
    killed run (out of memory) fails this job instead of ending it silently."""
    cmd = [sys.executable, str(_repo_root() / "cai" / "jobs" / "daily_score.py"),
           "--run-date", d.isoformat(), "--triggered-by", "backfill", "--no-context-file"]
    if args.backend:
        cmd += ["--backend", args.backend]
    if args.context_rows:
        cmd += ["--context-rows", str(args.context_rows)]
    if args.stub:
        cmd.append("--stub")
    env = {k: v for k, v in os.environ.items() if k not in ("COLL_RUN_DATE", "COLL_DRY_RUN")}
    t = time.time()
    code = subprocess.run(cmd, env=env).returncode
    if code != 0:
        raise SystemExit(f"backfill {d}: daily_score exited {code}"
                         + (" (killed, likely out of memory)" if code < 0 else ""))
    print(f"backfill {d}: done in {time.time() - t:.0f}s", flush=True)


if __name__ == "__main__":
    main()
