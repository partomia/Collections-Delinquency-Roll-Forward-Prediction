-- Reporting views for the Cloudera Data Visualization dashboards "Collections Roll-Forward -
-- Call List & Model Trust" and "Collections Roll-Forward - Data Health" (docs/DATAVIZ.md).
-- One flat view per dataset, so every visual reads one table. Views only: nothing is copied,
-- each visual reads the latest pipeline output, and nothing in the pipeline reads them.
-- Impala has no CREATE OR REPLACE VIEW, so each is dropped and recreated. Re-runnable:
--   python scripts/run_impala_sql.py sql/dataviz_views.sql

CREATE DATABASE IF NOT EXISTS rsingh_collections_delinquency_prediction_report;

-- Dataset "Call list": every published call list, one row per SMA-0 loan, with the product
-- name and a DPD bucket.
DROP VIEW IF EXISTS rsingh_collections_delinquency_prediction_report.v_call_list;
CREATE VIEW rsingh_collections_delinquency_prediction_report.v_call_list AS
SELECT c.run_date,
       CASE WHEN c.run_date = m.latest THEN 1 ELSE 0 END                AS is_latest,
       c.run_id, c.snapshot_date, c.loan_id, c.product_code,
       COALESCE(p.product_name, CAST(c.product_code AS STRING))         AS product_name,
       c.dpd_now,
       CASE WHEN c.dpd_now = 0 THEN '0'
            WHEN c.dpd_now <= 10 THEN '1-10'
            WHEN c.dpd_now <= 20 THEN '11-20' ELSE '21-30' END          AS dpd_bucket,
       c.overdue_amount, c.emi_amount, c.p_roll, c.p_roll * c.overdue_amount AS expected_roll_inr,
       c.priority_score, c.priority_rank, c.priority_pct, c.treatment, c.action, c.risk_signals
FROM rsingh_collections_delinquency_prediction_gold.collections_call_list c
CROSS JOIN (SELECT MAX(run_date) AS latest
            FROM rsingh_collections_delinquency_prediction_gold.collections_call_list) m
LEFT JOIN rsingh_collections_delinquency_prediction_ref.product_map p ON p.product_code = c.product_code;

-- Dataset "Book weekly": the SMA-0 book per weekly snapshot and product. Roll rate is
-- sum(rolls) / sum(labelled) in the visual; labels are NULL until 30 days have passed.
DROP VIEW IF EXISTS rsingh_collections_delinquency_prediction_report.v_book_weekly;
CREATE VIEW rsingh_collections_delinquency_prediction_report.v_book_weekly AS
SELECT f.snapshot_date,
       COALESCE(p.product_name, CAST(f.product_code AS STRING))         AS product_name,
       COUNT(*)                                                         AS loans,
       COUNT(f.rolled_to_sma1_30d)                                      AS labelled,
       SUM(f.rolled_to_sma1_30d)                                        AS rolls,
       SUM(f.overdue_amount)                                            AS overdue_inr,
       SUM(f.rolled_to_sma1_30d * f.overdue_amount)                     AS rolled_overdue_inr,
       SUM(f.nach_bounces_6m)                                           AS nach_bounces_6m,
       SUM(f.broken_ptp_3m)                                             AS broken_ptp_3m
FROM rsingh_collections_delinquency_prediction_gold.collections_features f
LEFT JOIN rsingh_collections_delinquency_prediction_ref.product_map p ON p.product_code = f.product_code
GROUP BY f.snapshot_date, COALESCE(p.product_name, CAST(f.product_code AS STRING));

-- Dataset "Model runs": one row per run date (a re-run replaces it), the holdout trust numbers
-- next to DPD alone.
DROP VIEW IF EXISTS rsingh_collections_delinquency_prediction_report.v_model_run;
CREATE VIEW rsingh_collections_delinquency_prediction_report.v_model_run AS
SELECT r.run_date, CASE WHEN r.run_date = m.latest THEN 1 ELSE 0 END AS is_latest,
       r.run_id, r.run_ts, r.device, r.tabicl_version, r.triggered_by, r.scored_loans,
       r.context_rows, r.holdout_rows, r.holdout_roll_rate, r.holdout_auc,
       r.capture_top10, r.dpd_only_capture_top10, r.capture_top10 - r.dpd_only_capture_top10 AS uplift_top10,
       r.capture_top40, r.dpd_only_capture_top40, r.capture_top40 - r.dpd_only_capture_top40 AS uplift_top40,
       r.value_capture_top10, r.duration_s
FROM rsingh_collections_delinquency_prediction_gold.collections_model_run r
CROSS JOIN (SELECT MAX(run_date) AS latest
            FROM rsingh_collections_delinquency_prediction_gold.collections_model_run) m;

-- Dataset "Holdout deciles": rolls caught by decile of p(roll), with a random-call baseline.
DROP VIEW IF EXISTS rsingh_collections_delinquency_prediction_report.v_holdout;
CREATE VIEW rsingh_collections_delinquency_prediction_report.v_holdout AS
SELECT h.run_date, CASE WHEN h.run_date = m.latest THEN 1 ELSE 0 END AS is_latest,
       h.decile, h.loans, h.rolls, h.roll_rate, h.mean_p_roll, h.cum_capture,
       h.decile / 10.0                                                  AS random_cum_capture
FROM rsingh_collections_delinquency_prediction_gold.collections_holdout h
CROSS JOIN (SELECT MAX(run_date) AS latest
            FROM rsingh_collections_delinquency_prediction_gold.collections_holdout) m;

-- Dataset "Data quality": every check result, flagged with the latest run of its layer.
-- A near miss passed but found unexpected rows (a "mostly" threshold absorbed them).
DROP VIEW IF EXISTS rsingh_collections_delinquency_prediction_report.v_dq;
CREATE VIEW rsingh_collections_delinquency_prediction_report.v_dq AS
-- '_' is a LIKE wildcard: escaped, or 'manual__%' would also match 'manual-2026...'.
SELECT d.pipeline_run,
       CASE WHEN d.pipeline_run LIKE 'scheduled\_\_%' THEN 'scheduled'
            WHEN d.pipeline_run LIKE 'manual\_\_%' THEN 'dag-manual'
            WHEN d.pipeline_run LIKE 'manual-%' THEN 'manual' ELSE 'other' END              AS run_type,
       CASE WHEN d.pipeline_run LIKE 'scheduled\_\_%'
            THEN CONCAT('DAG ', SUBSTR(d.pipeline_run, 12, 10)) ELSE d.pipeline_run END   AS run_label,
       d.run_ts, d.as_of, d.layer,
       CASE d.layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 ELSE 9 END AS layer_order,
       d.table_name, d.check_name, d.expectation_type,
       CASE WHEN d.expectation_type = 'expect_column_values_to_not_be_null' THEN 'Completeness'
            WHEN d.expectation_type LIKE '%to_be_unique' THEN 'Uniqueness'
            WHEN d.expectation_type = 'expect_table_row_count_to_be_between' THEN 'Volume'
            WHEN d.expectation_type IN ('expect_column_mean_to_be_between',
                                        'expect_column_max_to_be_between') THEN 'Distribution'
            WHEN d.expectation_type = 'expect_column_values_to_be_null' THEN 'Leakage guard'
            ELSE 'Validity' END                                                              AS category,
       d.column_name, d.severity, CAST(d.success AS INT) AS passed, 1 - CAST(d.success AS INT) AS failed,
       CASE WHEN d.success AND d.unexpected_count > 0 THEN 1 ELSE 0 END                     AS near_miss,
       d.observed_value, d.unexpected_count, d.unexpected_pct, d.element_count,
       CASE WHEN d.expectation_type = 'expect_table_row_count_to_be_between'
            THEN CAST(d.observed_value AS BIGINT) END                                        AS row_count,
       CASE WHEN d.pipeline_run = l.pipeline_run THEN 1 ELSE 0 END AS is_latest
FROM rsingh_collections_delinquency_prediction_ref.dq_results d
LEFT JOIN (
  SELECT layer, pipeline_run
  FROM (SELECT layer, pipeline_run,
               ROW_NUMBER() OVER (PARTITION BY layer ORDER BY MAX(run_ts) DESC) AS rn
        FROM rsingh_collections_delinquency_prediction_ref.dq_results GROUP BY layer, pipeline_run) x
  WHERE rn = 1
) l ON l.layer = d.layer;

-- Dataset "DQ runs": one row per pipeline run and layer, with when each gate ran relative
-- to the run's bronze gate.
DROP VIEW IF EXISTS rsingh_collections_delinquency_prediction_report.v_dq_run;
CREATE VIEW rsingh_collections_delinquency_prediction_report.v_dq_run AS
SELECT pipeline_run, run_type, run_label, as_of, layer, layer_order, MAX(is_latest) AS is_latest,
       MAX(run_ts)                                                   AS checked_at,
       ROUND((UNIX_TIMESTAMP(MAX(run_ts))
              - UNIX_TIMESTAMP(MIN(MIN(run_ts)) OVER (PARTITION BY pipeline_run))) / 60, 1) AS minutes_after_bronze,
       COUNT(*)                                                      AS checks,
       SUM(passed)                                                   AS passed,
       SUM(failed)                                                   AS failed,
       SUM(CASE WHEN severity = 'critical' THEN failed ELSE 0 END)   AS critical_failed,
       SUM(CASE WHEN severity = 'warning' THEN failed ELSE 0 END)    AS warnings_failed,
       SUM(near_miss)                                                AS near_misses,
       COUNT(DISTINCT table_name)                                    AS table_count,
       SUM(row_count)                                                AS rows_checked
FROM rsingh_collections_delinquency_prediction_report.v_dq
GROUP BY pipeline_run, run_type, run_label, as_of, layer, layer_order;

-- Checks: the numbers the dashboards' KPI tiles should show

SELECT COUNT(*) AS loans, ROUND(SUM(overdue_amount) / 1e5, 2) AS overdue_lakh,
       SUM(CASE WHEN treatment = 'AGENT_CALL_TODAY' THEN 1 ELSE 0 END) AS agent_calls,
       ROUND(SUM(p_roll), 0) AS expected_rolls
FROM rsingh_collections_delinquency_prediction_report.v_call_list WHERE is_latest = 1;

SELECT snapshot_date, SUM(loans) AS loans, ROUND(SUM(rolls) / SUM(labelled), 4) AS roll_rate
FROM rsingh_collections_delinquency_prediction_report.v_book_weekly
WHERE labelled > 0 GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 5;

SELECT run_date, triggered_by, holdout_auc, capture_top10, dpd_only_capture_top10, uplift_top10
FROM rsingh_collections_delinquency_prediction_report.v_model_run ORDER BY run_date;

SELECT decile, cum_capture, random_cum_capture
FROM rsingh_collections_delinquency_prediction_report.v_holdout WHERE is_latest = 1 ORDER BY decile;

SELECT layer, COUNT(*) AS checks, SUM(failed) AS failed, SUM(near_miss) AS near_misses
FROM rsingh_collections_delinquency_prediction_report.v_dq WHERE is_latest = 1 GROUP BY layer ORDER BY layer;

SELECT run_label, layer, checked_at, minutes_after_bronze, checks, failed, near_misses, table_count, rows_checked
FROM rsingh_collections_delinquency_prediction_report.v_dq_run ORDER BY checked_at;
