"""Collections: delinquency roll-forward prediction. Collections head / collector demo app.

  streamlit run app/streamlit_app.py            # storage from config/collections.yaml
  COLL_STORAGE_BACKEND=parquet streamlit run app/streamlit_app.py
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    ROOT = Path(__file__).resolve().parent.parent
except NameError:
    ROOT = Path(os.getcwd())
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from app import data  # noqa: E402
from coll.client import endpoint_configured, score_loans  # noqa: E402
from coll.config import policy, settings, table  # noqa: E402
from coll.features import FEATURES, PRODUCTS  # noqa: E402
from coll.prioritise import assign_bands  # noqa: E402

st.set_page_config(page_title="Collections roll-forward", page_icon=":telephone_receiver:", layout="wide")

BAND_COLOR = {"AGENT_CALL_TODAY": "#e4572e", "AGENT_OR_IVR": "#f3a712", "SMS_REMINDER": "#2e86ab"}
BAND_LABEL = {"AGENT_CALL_TODAY": "Agent call today", "AGENT_OR_IVR": "Agent or IVR", "SMS_REMINDER": "SMS reminder"}
OUTCOMES = ["REACHED", "NO_ANSWER", "SWITCHED_OFF", "WRONG_NUMBER"]


def inr(x: float) -> str:
    if abs(x) >= 1e7:
        return f"₹{x / 1e7:,.2f} cr"
    if abs(x) >= 1e5:
        return f"₹{x / 1e5:,.1f} lakh"
    return f"₹{x:,.0f}"


def pct(x) -> str:
    return "n/a" if x is None or pd.isna(x) else f"{x:.0%}"


# ---------------------------------------------------------------- sidebar
try:
    runs = data.read("collections_model_run").sort_values("run_date", ascending=False)
except Exception as e:  # no data yet / connection problem
    st.error(f"Could not read {table('collections_model_run')} via {settings()['storage']['backend']}: {e}")
    st.info("Run the daily job first (cai/jobs/daily_score.py), or set COLL_STORAGE_BACKEND=parquet "
            "with exported tables in data/parquet.")
    st.stop()

st.sidebar.title("Scoring run")
run_date = st.sidebar.selectbox("Run date", runs["run_date"].tolist(), format_func=lambda d: d.strftime("%a %d %b %Y"))
run = runs[runs["run_date"] == run_date].iloc[0]
st.sidebar.caption(f"Run `{run.run_id}`  \nModel `{run.model_id}`  \nDevice `{run.device}` · "
                   f"context {int(run.context_rows):,} rows  \nStorage: {data.storage().name}")
if run.capture_top10 is not None and not pd.isna(run.capture_top10):
    st.sidebar.metric("Top 10% of calls catch", pct(run.capture_top10), f"of actual rolls (DPD alone "
                      f"{pct(run.dpd_only_capture_top10)})", delta_color="off",
                      help="Holdout on the latest labelled weeks, scored with a context from older weeks only.")

st.sidebar.divider()
st.sidebar.markdown("**Dialler capacity**")
bands_pol = policy()["treatment"]["bands"]
top = st.sidebar.slider("Agent call today (top %)", 1, 30, int(round(bands_pol[0]["upto_pct"] * 100)))
mid = st.sidebar.slider("Agent or IVR (next %)", 0, 60, int(round((bands_pol[1]["upto_pct"] - bands_pol[0]["upto_pct"]) * 100)))
bands = [{**bands_pol[0], "upto_pct": top / 100}, {**bands_pol[1], "upto_pct": (top + mid) / 100}, bands_pol[2]]
st.sidebar.caption("Cut-offs are business settings: moving them re-bands the book without re-scoring.")
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.divider()
st.sidebar.caption("Demo on synthetic data, not a validated credit model. The model only ranks accounts; contact "
                   "hours, conduct and frequency follow the RBI fair practices code for recovery agents.")

calls = data.for_run("collections_call_list", run_date).sort_values("priority_rank")
calls["treatment"], calls["action"] = assign_bands(calls["priority_pct"], bands)
calls["product"] = calls["product_code"].map(PRODUCTS)

st.title("Collections: SMA-0 roll-forward call list")
st.caption(f"Run {run_date:%d %b %Y}. TabICL v2 scores every SMA-0 loan (1-30 DPD) for the chance of reaching "
           "SMA-1 (31+ DPD) within 30 days, learning in context from labelled history in the gold Iceberg table. "
           "Priority = roll probability × overdue amount.")

tab_calls, tab_trust, tab_book, tab_whatif, tab_outcomes, tab_history = st.tabs(
    ["Today's call list", "Model trust", "Book trend", "Loan what-if", "Collector outcomes", "History & lineage"])

# ---------------------------------------------------------------- call list
with tab_calls:
    exp_rolls = calls["p_roll"].sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SMA-0 loans scored", f"{len(calls):,}")
    c2.metric("Overdue in SMA-0", inr(calls["overdue_amount"].sum()))
    c3.metric("Expected rolls to SMA-1", f"{exp_rolls:,.0f}", f"{exp_rolls / max(len(calls), 1):.0%} of the book",
              delta_color="off")
    c4.metric("Expected overdue at risk", inr(calls["priority_score"].sum()))

    left, right = st.columns([3, 2])
    with right:
        g = (calls.groupby("treatment").agg(loans=("loan_id", "size"), overdue=("overdue_amount", "sum"),
                                            at_risk=("priority_score", "sum"), exp_rolls=("p_roll", "sum"))
             .reindex(BAND_LABEL.keys()).fillna(0))
        fig = go.Figure(go.Bar(x=[BAND_LABEL[b] for b in g.index], y=g["at_risk"], marker_color=list(BAND_COLOR.values()),
                               text=[f"{int(n):,} loans<br>{r:,.0f} exp. rolls" for n, r in zip(g["loans"], g["exp_rolls"])],
                               textposition="outside"))
        fig.update_layout(title="Expected overdue at risk by treatment", height=340, yaxis_title="INR",
                          margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, width="stretch")
        share = g["at_risk"] / max(g["at_risk"].sum(), 1)
        st.info(f"**{BAND_LABEL['AGENT_CALL_TODAY']}** covers {top}% of loans and {share.iloc[0]:.0%} of the expected "
                f"overdue at risk.")
    with left:
        f1, f2, f3 = st.columns(3)
        band_f = f1.multiselect("Treatment", list(BAND_LABEL), default=list(BAND_LABEL), format_func=BAND_LABEL.get)
        prod_f = f2.multiselect("Product", sorted(calls["product"].dropna().unique()))
        search = f3.text_input("Loan id contains")
        view = calls[calls["treatment"].isin(band_f)]
        if prod_f:
            view = view[view["product"].isin(prod_f)]
        if search:
            view = view[view["loan_id"].str.contains(search.strip(), case=False)]
        st.dataframe(view.assign(treatment=view["treatment"].map(BAND_LABEL).fillna(view["treatment"]))[
                         ["priority_rank", "loan_id", "product", "dpd_now", "overdue_amount", "p_roll",
                          "priority_score", "treatment", "risk_signals"]],
                     hide_index=True, width="stretch", height=430, column_config={
                         "priority_rank": st.column_config.NumberColumn("#", format="%d"),
                         "loan_id": "Loan", "product": "Product", "dpd_now": st.column_config.NumberColumn("DPD"),
                         "overdue_amount": st.column_config.NumberColumn("Overdue ₹", format="%.0f"),
                         "p_roll": st.column_config.ProgressColumn("P(roll)", min_value=0, max_value=1, format="%.2f"),
                         "priority_score": st.column_config.NumberColumn("Priority ₹", format="%.0f"),
                         "treatment": st.column_config.TextColumn("Treatment"),
                         "risk_signals": st.column_config.TextColumn("Signals for the collector", width="large")})
        st.download_button("Download call list (CSV)", view.to_csv(index=False).encode(),
                           f"call_list_{run_date:%Y%m%d}.csv", "text/csv")
    st.caption("Signals are business rules on the inputs to help the collector open the call; they are not an "
               "explanation of the model's score.")

# ---------------------------------------------------------------- trust
with tab_trust:
    if run.holdout_auc is None or pd.isna(run.holdout_auc):
        st.warning("This run skipped the holdout check.")
    else:
        st.markdown(f"**Holdout**: loans in SMA-0 on the {run.holdout_test_from:%d %b} to {run.holdout_test_to:%d %b} "
                    f"snapshots ({int(run.holdout_rows):,} loans, {pct(run.holdout_roll_rate)} actually rolled), "
                    f"scored with a context that ends 30 days before the first test week "
                    f"({int(run.holdout_context_rows):,} rows), as if the model had been deployed then.")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Top 10% of calls catch", pct(run.capture_top10), f"DPD alone {pct(run.dpd_only_capture_top10)}",
                  delta_color="off")
        c2.metric("Agent bands (top 40%) catch", pct(run.capture_top40),
                  f"DPD alone {pct(getattr(run, 'dpd_only_capture_top40', None))}", delta_color="off")
        c3.metric("Top 10% by priority catch", pct(run.value_capture_top10), "of rolled overdue ₹", delta_color="off")
        c4.metric("AUC", f"{run.holdout_auc:.3f}")
        dec = data.for_run("collections_holdout", run_date).sort_values("decile")
        if not dec.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=dec["decile"], y=dec["roll_rate"], name="Actual roll rate", marker_color="#e4572e"))
            fig.add_trace(go.Scatter(x=dec["decile"], y=dec["mean_p_roll"], name="Mean predicted P(roll)",
                                     mode="lines+markers", line=dict(color="#1b1b1e", dash="dot")))
            fig.add_trace(go.Scatter(x=dec["decile"], y=dec["cum_capture"], name="Cumulative share of rolls caught",
                                     mode="lines+markers", line=dict(color="#2e86ab"), yaxis="y"))
            fig.update_layout(title="Holdout by decile of predicted roll probability (1 = highest)", height=400,
                              xaxis=dict(title="Decile", dtick=1), yaxis=dict(tickformat=".0%", range=[0, 1.05]),
                              legend=dict(orientation="h", y=-0.2), margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, width="stretch")
        st.caption("DPD alone = calling the most overdue loans first. A loan at 25 DPD only has to stay unpaid "
                   "6 more days, so DPD is a strong baseline; the model adds most by finding loans at low DPD "
                   "that will still roll (no salary credit, bounces, broken promises, unreachable).")

# ---------------------------------------------------------------- book
with tab_book:
    trend = data.book_trend()
    trend = trend[trend["snapshot_date"] <= run_date]
    agg = trend.groupby("snapshot_date")[["sma0_loans", "rolled", "labelled", "overdue_amount"]].sum().reset_index()
    agg["roll_rate"] = agg["rolled"] / agg["labelled"].where(agg["labelled"] > 0)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=agg["snapshot_date"], y=agg["sma0_loans"], name="SMA-0 loans", marker_color="#9fc5e8"))
    fig.add_trace(go.Scatter(x=agg["snapshot_date"], y=agg["roll_rate"], name="Rolled to SMA-1 within 30 days",
                             yaxis="y2", mode="lines+markers", line=dict(color="#e4572e")))
    fig.update_layout(title="Weekly SMA-0 book and its 30-day roll rate (latest 4 weeks not yet labelled)",
                      height=420, yaxis=dict(title="Loans"), legend=dict(orientation="h", y=-0.2),
                      yaxis2=dict(title="Roll rate", overlaying="y", side="right", tickformat=".0%"),
                      margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, width="stretch")
    by_prod = trend[trend["labelled"] > 0].groupby("product_code")[["sma0_loans", "rolled", "labelled"]].sum()
    by_prod["roll_rate"] = by_prod["rolled"] / by_prod["labelled"]
    by_prod.index = by_prod.index.map(PRODUCTS)
    st.dataframe(by_prod[["sma0_loans", "roll_rate"]].rename(columns={"sma0_loans": "SMA-0 loan-weeks",
                                                                     "roll_rate": "Roll rate"}),
                 width="stretch", column_config={"Roll rate": st.column_config.NumberColumn(format="percent")})

# ---------------------------------------------------------------- what-if
with tab_whatif:
    st.markdown("Score one loan live, then change what the collections team can influence. "
                + ("Calls the **CAI model endpoint**." if endpoint_configured()
                   else "No endpoint configured: scoring runs in this app."))
    feats = data.features_on(run.snapshot_date).set_index("loan_id")
    options = [x for x in calls["loan_id"] if x in feats.index]
    loan_id = st.selectbox("Loan (call list order)", options, key="whatif_loan")
    base = feats.loc[loan_id, FEATURES].to_dict()
    c1, c2, c3, c4 = st.columns(4)
    sal = c1.selectbox("Salary credit in last 30 days", [0, 1], index=int(base["salary_credit_last_30d"]),
                       format_func=lambda v: "Yes" if v else "No")
    reached = c2.slider("Contacts reached in 30 days", 0, 6, int(base["contacts_reached_30d"]))
    broken = c3.slider("Broken PTPs in 3 months", 0, 5, int(base["broken_ptp_3m"]))
    bureau = c4.slider("Bureau score", 300, 900, int(base["bureau_score"]))
    st.json({k: (int(v) if float(v).is_integer() else v) for k, v in base.items()}, expanded=False)
    if st.button("Score", type="primary", key="whatif_score"):
        req = {"loans": [{"loan_id": loan_id, **base}],
               "what_if": {"salary_credit_last_30d": sal, "contacts_reached_30d": reached,
                           "broken_ptp_3m": broken, "bureau_score": bureau}}
        with st.spinner("Scoring..."):
            try:
                resp, src = score_loans(req)
            except Exception as e:
                st.error(f"Scoring failed: {e}")
                st.stop()
        if "error" in resp:
            st.error(resp["error"])
            st.stop()
        s = resp["scores"][0]
        c1, c2, c3 = st.columns(3)
        c1.metric("P(roll) as is", f"{s['p_roll']:.1%}")
        c2.metric("P(roll) with changes", f"{s['p_roll_what_if']:.1%}",
                  f"{(s['p_roll_what_if'] - s['p_roll']) * 100:+.1f} pts", delta_color="inverse")
        c3.metric("Priority ₹", f"{s['priority_score_what_if']:,.0f}", f"{s['priority_score_what_if'] - s['priority_score']:+,.0f}",
                  delta_color="inverse")
        st.caption(f"Scored by: {src} · signals: {s['risk_signals'] or 'none'} · context: "
                   f"{resp.get('model', {}).get('context_rows', '?')} rows")

# ---------------------------------------------------------------- outcomes
with tab_outcomes:
    st.markdown(f"Record what happened on a call. Outcomes are appended to `{table('collector_outcomes')}`; the next "
                "CDE silver load unions them into the contact history, so tomorrow's features (reached contacts, "
                "broken promises) include them with no retraining.")
    with st.form("outcome"):
        c1, c2, c3 = st.columns(3)
        o_loan = c1.selectbox("Loan", calls["loan_id"].tolist())
        o_channel = c2.selectbox("Channel", ["CALL", "FIELD", "IVR", "WHATSAPP"])
        o_outcome = c3.selectbox("Outcome", OUTCOMES)
        c4, c5, c6 = st.columns(3)
        o_ptp = c4.checkbox("Promise to pay")
        o_ptp_date = c5.date_input("PTP date", value=date.today() + timedelta(days=3))
        default_amt = float(calls.set_index("loan_id").loc[o_loan, "overdue_amount"]) if o_loan else 0.0
        o_ptp_amt = c6.number_input("PTP amount ₹", min_value=0.0, value=default_amt, step=500.0)
        o_by = st.text_input("Recorded by", value=os.environ.get("HADOOP_USER_NAME", "collector"))
        o_notes = st.text_input("Notes")
        submitted = st.form_submit_button("Save outcome", type="primary")
    if submitted:
        now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
        keep_ptp = o_ptp and o_outcome == "REACHED"
        row = pd.DataFrame([{"contact_id": f"APP-{uuid.uuid4().hex[:12]}", "loan_id": o_loan, "contact_ts": now,
                             "channel": o_channel, "outcome": o_outcome, "ptp_date": o_ptp_date if keep_ptp else None,
                             "ptp_amount": o_ptp_amt if keep_ptp else None, "recorded_by": o_by,
                             "notes": o_notes, "ingested_at": now}])
        try:
            data.storage().append("collector_outcomes", row)
            st.success(f"Saved: {o_loan} {o_outcome}" + (f", PTP ₹{o_ptp_amt:,.0f} by {o_ptp_date:%d %b}" if keep_ptp else ""))
        except Exception as e:
            st.error(f"Could not save: {e}")
    recent = data.outcomes()
    if not recent.empty:
        st.dataframe(recent.sort_values("contact_ts", ascending=False).head(50), hide_index=True, width="stretch")

# ---------------------------------------------------------------- history
with tab_history:
    st.markdown("**Holdout capture by run**. Every daily run is kept, keyed by run date.")
    h = runs.sort_values("run_date")
    fig = go.Figure()
    for col, name, dash in (("capture_top10", "Top 10% catch", None), ("dpd_only_capture_top10", "Top 10% by DPD alone", "dot"),
                            ("capture_top40", "Top 40% catch", None), ("holdout_auc", "AUC", "dash")):
        if col in h:
            fig.add_trace(go.Scatter(x=h["run_date"], y=h[col], name=name, mode="lines+markers", line=dict(dash=dash)))
    fig.update_layout(height=340, yaxis=dict(tickformat=".0%", range=[0, 1]), margin=dict(l=10, r=10, t=20, b=10),
                      legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, width="stretch")

    st.markdown("**Lineage**: each run records the checkpoint, the TabICL version, the context window and the "
                "Iceberg snapshot of the gold table it read. TabICL has no trained weights of its own beyond the "
                "pinned checkpoint, so these fully define the model of the day.")
    st.dataframe(runs[["run_date", "run_id", "run_ts", "model_id", "tabicl_version", "device", "scored_loans",
                       "context_rows", "context_from", "context_to", "source_snapshot_id", "holdout_auc",
                       "capture_top10", "triggered_by", "duration_s"]], hide_index=True, width="stretch")
    snap = run.source_snapshot_id if isinstance(run.source_snapshot_id, str) and run.source_snapshot_id else "<snapshot_id>"
    st.markdown("Rebuild the exact context of this run in Hue (CDW Impala):")
    st.code(f"SELECT * FROM {table('collections_features')}\n  FOR SYSTEM_VERSION AS OF {snap}\n"
            f"  WHERE rolled_to_sma1_30d IS NOT NULL\n"
            f"    AND snapshot_date BETWEEN DATE '{run.context_from}' AND DATE '{run.context_to}'\n"
            f"  ORDER BY snapshot_date DESC, loan_id LIMIT {int(run.context_rows)};", language="sql")
