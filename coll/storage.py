"""Read gold features and write the daily outputs, on CDW Impala or local parquet.

  impala   CDW Impala virtual warehouse over HTTPS (impyla). Feature reads can
           be pinned to an Iceberg snapshot (FOR SYSTEM_VERSION AS OF), so a
           run reads one consistent version even if a CDE load commits
           mid-run, and the endpoint can rebuild the exact same context later.
           Outputs are Iceberg v2 tables; a rerun for a run_date does
           DELETE + INSERT for that date, each committing a snapshot.
  parquet  one file per table under storage.parquet_dir, for a laptop, Docker
           or an offline demo. Same query and replace-by-run_date semantics.
"""

from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import pandas as pd

from coll.config import ROOT, settings, table
from coll.features import COLUMNS, LABEL
from coll.schema import COLLECTOR_OUTCOMES, OUTPUT_TABLES

logger = logging.getLogger(__name__)

TABLE_COLUMNS = {**OUTPUT_TABLES, "collector_outcomes": COLLECTOR_OUTCOMES}
PARTITION = {"collections_call_list": "run_date", "collections_holdout": "run_date",
             "collections_model_run": "run_date", "collector_outcomes": "month(contact_ts)"}


def get_storage(backend: str | None = None):
    backend = backend or settings()["storage"]["backend"]
    if backend == "impala":
        return ImpalaStorage(settings()["impala"])
    if backend == "parquet":
        return ParquetStorage(Path(settings()["storage"]["parquet_dir"]))
    raise ValueError(f"unknown storage backend {backend!r} (impala | parquet)")


def _cast(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    df = df.copy()
    for col, typ in columns:
        if col not in df.columns:
            continue
        if typ == "DATE":
            df[col] = pd.to_datetime(df[col]).dt.date
        elif typ == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col])
    return df


def _normalise_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    for c in COLUMNS[2:]:
        df[c] = pd.to_numeric(df[c])
    return df.reset_index(drop=True)


class ParquetStorage:
    name = "parquet"

    def __init__(self, directory: Path):
        self.dir = directory if directory.is_absolute() else ROOT / directory
        self._features = None

    def _path(self, key: str) -> Path:
        return self.dir / f"{table(key).split('.')[-1]}.parquet"

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def read(self, key: str, where_run_date: date | None = None) -> pd.DataFrame:
        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(f"{path} not found - run scripts/run_cde_local.py all, or copy an export")
        df = pd.read_parquet(path)
        if where_run_date is not None and "run_date" in df.columns:
            df = df[pd.to_datetime(df["run_date"]).dt.date == where_run_date]
        return df.reset_index(drop=True)

    def _all_features(self) -> pd.DataFrame:
        if self._features is None:
            self._features = _normalise_features(self.read("collections_features")[COLUMNS])
        return self._features

    def snapshot_id(self, key: str) -> str | None:
        return None

    def labelled_dates(self, snapshot_id: str | None = None) -> list[date]:
        df = self._all_features()
        return sorted(df.loc[df[LABEL].notna(), "snapshot_date"].unique())

    def latest_snapshot_date(self, snapshot_id: str | None = None) -> date:
        return self._all_features()["snapshot_date"].max()

    def features(self, *, labelled: bool | None = None, date_from: date | None = None,
                 date_to: date | None = None, limit: int | None = None,
                 snapshot_id: str | None = None) -> pd.DataFrame:
        df = self._all_features()
        if labelled is True:
            df = df[df[LABEL].notna()]
        elif labelled is False:
            df = df[df[LABEL].isna()]
        if date_from is not None:
            df = df[df["snapshot_date"] >= date_from]
        if date_to is not None:
            df = df[df["snapshot_date"] <= date_to]
        df = df.sort_values(["snapshot_date", "loan_id"], ascending=[False, True])
        if limit is not None:
            df = df.head(limit)
        return df.reset_index(drop=True)

    def book_trend(self) -> pd.DataFrame:
        df = self._all_features()
        g = (df.groupby(["snapshot_date", "product_code"])
             .agg(sma0_loans=("loan_id", "size"), rolled=(LABEL, "sum"), labelled=(LABEL, "count"),
                  overdue_amount=("overdue_amount", "sum"))
             .reset_index())
        return g

    def replace_run(self, key: str, df: pd.DataFrame, run_date: date) -> None:
        cols = OUTPUT_TABLES[key]
        df = _cast(df[[c for c, _ in cols]], cols)
        path = self._path(key)
        if path.exists():
            old = pd.read_parquet(path)
            old = old[pd.to_datetime(old["run_date"]).dt.date != run_date]
            df = pd.concat([_cast(old, cols), df], ignore_index=True)
        self.dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info("wrote %d rows for %s to %s", (df["run_date"] == run_date).sum(), run_date, path)

    def append(self, key: str, df: pd.DataFrame) -> None:
        cols = TABLE_COLUMNS[key]
        df = _cast(df[[c for c, _ in cols]], cols)
        path = self._path(key)
        if path.exists():
            df = pd.concat([_cast(pd.read_parquet(path), cols), df], ignore_index=True)
        self.dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)


def _sql_literal(value, typ: str) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NaT:
        return "NULL"
    if typ in ("DOUBLE", "INT", "BIGINT"):
        return repr(float(value)) if typ == "DOUBLE" else str(int(value))
    if typ == "BOOLEAN":
        return "true" if bool(value) else "false"
    if typ == "DATE":
        return f"DATE '{pd.Timestamp(value).date().isoformat()}'"
    if typ == "TIMESTAMP":
        return f"CAST('{pd.Timestamp(value).strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


class ImpalaStorage:
    name = "impala"
    INSERT_CHUNK = 500

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._conn = None
        self._ensured: set[str] = set()

    def _connect(self):
        if self._conn is None:
            from impala.dbapi import connect

            c = self.cfg
            if not c["host"]:
                raise RuntimeError("COLL_IMPALA_HOST is not set (CDW Impala virtual warehouse host)")
            kwargs = dict(host=c["host"], port=int(c["port"]), use_ssl=c["use_ssl"],
                          use_http_transport=c["use_http_transport"], http_path=c["http_path"],
                          auth_mechanism=c["auth_mechanism"])
            if c["auth_mechanism"].upper() == "GSSAPI":
                kwargs["kerberos_service_name"] = c["kerberos_service_name"]
            if c.get("user"):
                kwargs["user"] = c["user"]
            if c.get("password"):
                kwargs["password"] = c["password"]
            self._conn = connect(**kwargs)
        return self._conn

    def query(self, sql: str) -> pd.DataFrame:
        cur = self._connect().cursor()
        try:
            cur.execute(sql)
            if cur.description is None:
                return pd.DataFrame()
            cols = [d[0].split(".")[-1] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
        finally:
            cur.close()

    def execute(self, sql: str) -> None:
        cur = self._connect().cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def exists(self, key: str) -> bool:
        db, name = table(key).split(".")
        return not self.query(f"SHOW TABLES IN {db} LIKE '{name}'").empty

    def read(self, key: str, where_run_date: date | None = None) -> pd.DataFrame:
        sql = f"SELECT * FROM {table(key)}"
        if where_run_date is not None:
            sql += f" WHERE run_date = DATE '{where_run_date.isoformat()}'"
        return self.query(sql)

    def snapshot_id(self, key: str) -> str | None:
        """Latest Iceberg snapshot of a table, recorded for lineage."""
        try:
            hist = self.query(f"DESCRIBE HISTORY {table(key)}")
            return str(hist.iloc[-1]["snapshot_id"]) if not hist.empty else None
        except Exception as e:  # lineage is best-effort, never fail the run on it
            logger.warning("could not read snapshot history for %s: %s", key, e)
            return None

    @staticmethod
    def _source(snapshot_id: str | None) -> str:
        t = table("collections_features")
        return f"{t} FOR SYSTEM_VERSION AS OF {int(snapshot_id)}" if snapshot_id else t

    def labelled_dates(self, snapshot_id: str | None = None) -> list[date]:
        df = self.query(f"SELECT DISTINCT snapshot_date FROM {self._source(snapshot_id)} "
                        f"WHERE {LABEL} IS NOT NULL")
        return sorted(pd.to_datetime(df["snapshot_date"]).dt.date)

    def latest_snapshot_date(self, snapshot_id: str | None = None) -> date:
        df = self.query(f"SELECT MAX(snapshot_date) AS d FROM {self._source(snapshot_id)}")
        return pd.Timestamp(df.iloc[0]["d"]).date()

    def features(self, *, labelled: bool | None = None, date_from: date | None = None,
                 date_to: date | None = None, limit: int | None = None,
                 snapshot_id: str | None = None) -> pd.DataFrame:
        where = []
        if labelled is True:
            where.append(f"{LABEL} IS NOT NULL")
        elif labelled is False:
            where.append(f"{LABEL} IS NULL")
        if date_from is not None:
            where.append(f"snapshot_date >= DATE '{date_from.isoformat()}'")
        if date_to is not None:
            where.append(f"snapshot_date <= DATE '{date_to.isoformat()}'")
        sql = f"SELECT {', '.join(COLUMNS)} FROM {self._source(snapshot_id)}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY snapshot_date DESC, loan_id"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return _normalise_features(self.query(sql))

    def book_trend(self) -> pd.DataFrame:
        df = self.query(f"""
            SELECT snapshot_date, product_code, COUNT(*) AS sma0_loans, SUM({LABEL}) AS rolled,
                   COUNT({LABEL}) AS labelled, SUM(overdue_amount) AS overdue_amount
            FROM {table('collections_features')} GROUP BY snapshot_date, product_code""")
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
        return df

    def ensure_table(self, key: str) -> None:
        if key in self._ensured:
            return
        full = table(key)
        cols = ", ".join(f"{c} {t}" for c, t in TABLE_COLUMNS[key])
        self.execute(f"CREATE DATABASE IF NOT EXISTS {full.split('.')[0]}")
        self.execute(f"CREATE TABLE IF NOT EXISTS {full} ({cols}) PARTITIONED BY SPEC ({PARTITION[key]}) "
                     f"STORED AS ICEBERG TBLPROPERTIES ('format-version'='2')")
        self._ensured.add(key)

    def _insert(self, key: str, df: pd.DataFrame) -> None:
        cols = TABLE_COLUMNS[key]
        full = table(key)
        records = df[[c for c, _ in cols]].to_dict("records")
        for i in range(0, len(records), self.INSERT_CHUNK):
            values = ",\n".join(
                "(" + ", ".join(_sql_literal(r[c], t) for c, t in cols) + ")"
                for r in records[i:i + self.INSERT_CHUNK])
            self.execute(f"INSERT INTO {full} ({', '.join(c for c, _ in cols)}) VALUES {values}")

    def replace_run(self, key: str, df: pd.DataFrame, run_date: date) -> None:
        self.ensure_table(key)
        self.execute(f"DELETE FROM {table(key)} WHERE run_date = DATE '{run_date.isoformat()}'")
        self._insert(key, df)
        logger.info("wrote %d rows for %s to %s", len(df), run_date, table(key))

    def append(self, key: str, df: pd.DataFrame) -> None:
        self.ensure_table(key)
        self._insert(key, df)
