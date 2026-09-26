"""Cached reads of the gold tables for the Streamlit app."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from coll.storage import get_storage

DATE_COLS = ("run_date", "snapshot_date", "context_from", "context_to", "holdout_test_from", "holdout_test_to",
             "ptp_date")


@st.cache_resource
def storage():
    return get_storage()


def _dates(df: pd.DataFrame) -> pd.DataFrame:
    for c in DATE_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c]).dt.date
    return df


@st.cache_data(ttl=300, show_spinner="Reading gold tables...")
def read(key: str) -> pd.DataFrame:
    return _dates(storage().read(key))


def for_run(key: str, run_date) -> pd.DataFrame:
    df = read(key)
    return df[df["run_date"] == run_date].copy()


@st.cache_data(ttl=300, show_spinner="Reading features...")
def features_on(snapshot_date) -> pd.DataFrame:
    return storage().features(date_from=snapshot_date, date_to=snapshot_date)


@st.cache_data(ttl=600, show_spinner="Reading book trend...")
def book_trend() -> pd.DataFrame:
    return storage().book_trend()


def outcomes() -> pd.DataFrame:
    try:
        return _dates(storage().read("collector_outcomes"))
    except Exception:  # table not created until the first outcome is recorded
        return pd.DataFrame()
