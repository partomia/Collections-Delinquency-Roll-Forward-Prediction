#!/usr/bin/env python3
"""
Run a .sql file on CDW Impala statement by statement (the COLL_IMPALA_*
connection from config/collections.yaml and .env), printing each statement's
row count and time. Statements are split on ';' at line ends; '--' comment
lines are dropped.

  set -a; source .env; set +a
  python scripts/run_impala_sql.py sql/dataviz_views.sql
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def statements(text: str) -> list[str]:
    body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in body.split(";\n") if s.strip().rstrip(";").strip()]


def main() -> int:
    from coll.storage import get_storage

    if len(sys.argv) != 2:
        sys.exit(__doc__)
    conn = get_storage("impala")._connect()
    for sql in statements(Path(sys.argv[1]).read_text()):
        t0 = time.time()
        cur = conn.cursor()
        try:
            cur.execute(sql.rstrip(";"))
            rows = cur.fetchall() if cur.description else []
        finally:
            cur.close()
        print(f"{time.time() - t0:6.1f}s  {len(rows):6d} rows  {' '.join(sql.split())[:90]}", flush=True)
        if sql.lstrip().upper().startswith("SELECT"):
            for row in rows[:12]:
                print("        ", row)
    return 0


if __name__ == "__main__":
    sys.exit(main())
