"""Column definitions for the tables written from CAI.

Gold outputs of the daily job are keyed by run_date: a rerun for the same day
replaces that day only, so yesterday's call list stays queryable.
bronze.collector_outcomes is appended to by the app and read by the CDE silver
job, which closes the loop into tomorrow's features.
"""

OUTPUT_TABLES = {
    "collections_call_list": [
        ("run_date", "DATE"), ("run_id", "STRING"), ("snapshot_date", "DATE"), ("loan_id", "STRING"),
        ("product_code", "INT"), ("dpd_now", "INT"), ("overdue_amount", "DOUBLE"), ("emi_amount", "DOUBLE"),
        ("p_roll", "DOUBLE"), ("priority_score", "DOUBLE"), ("priority_rank", "INT"), ("priority_pct", "DOUBLE"),
        ("treatment", "STRING"), ("action", "STRING"), ("risk_signals", "STRING"),
    ],
    "collections_holdout": [
        ("run_date", "DATE"), ("run_id", "STRING"), ("decile", "INT"), ("rows", "INT"), ("rolls", "INT"),
        ("roll_rate", "DOUBLE"), ("mean_p_roll", "DOUBLE"), ("cum_capture", "DOUBLE"),
    ],
    "collections_model_run": [
        ("run_date", "DATE"), ("run_id", "STRING"), ("run_ts", "TIMESTAMP"), ("model_id", "STRING"),
        ("tabicl_version", "STRING"), ("device", "STRING"), ("snapshot_date", "DATE"), ("scored_loans", "INT"),
        ("context_rows", "INT"), ("context_from", "DATE"), ("context_to", "DATE"),
        ("source_table", "STRING"), ("source_snapshot_id", "STRING"),
        ("holdout_test_from", "DATE"), ("holdout_test_to", "DATE"), ("holdout_context_rows", "INT"),
        ("holdout_rows", "INT"), ("holdout_roll_rate", "DOUBLE"), ("holdout_auc", "DOUBLE"),
        ("capture_top10", "DOUBLE"), ("capture_top40", "DOUBLE"), ("value_capture_top10", "DOUBLE"),
        ("dpd_only_capture_top10", "DOUBLE"), ("policy_json", "STRING"), ("triggered_by", "STRING"),
        ("duration_s", "DOUBLE"),
    ],
}

# Appended by the app; same contact columns as bronze.dialler_contacts.
COLLECTOR_OUTCOMES = [
    ("contact_id", "STRING"), ("loan_id", "STRING"), ("contact_ts", "TIMESTAMP"), ("channel", "STRING"),
    ("outcome", "STRING"), ("ptp_date", "DATE"), ("ptp_amount", "DOUBLE"), ("recorded_by", "STRING"),
    ("notes", "STRING"), ("ingested_at", "TIMESTAMP"),
]
