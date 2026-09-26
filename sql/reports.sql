-- Collections roll-forward: report queries for Hue (CDW Impala).
-- Tables written by Spark (CDE) or another engine may need a metadata refresh first.
INVALIDATE METADATA rsingh_collections_delinquency_prediction_gold.collections_features;
REFRESH rsingh_collections_delinquency_prediction_gold.collections_call_list;

-- 1. Today's call list: loans and expected overdue at risk by treatment band
WITH latest AS (SELECT MAX(run_date) AS d FROM rsingh_collections_delinquency_prediction_gold.collections_call_list)
SELECT treatment,
       COUNT(*)                              AS loans,
       ROUND(SUM(overdue_amount) / 1e5, 1)   AS overdue_lakh,
       ROUND(SUM(p_roll), 0)                 AS expected_rolls,
       ROUND(SUM(priority_score) / 1e5, 1)   AS overdue_at_risk_lakh
FROM rsingh_collections_delinquency_prediction_gold.collections_call_list c, latest
WHERE c.run_date = latest.d
GROUP BY treatment
ORDER BY MIN(priority_rank);

-- 2. The morning's top 25 calls with the signals a collector opens with
SELECT priority_rank, loan_id, product_code, dpd_now, overdue_amount, p_roll, priority_score, treatment, risk_signals
FROM rsingh_collections_delinquency_prediction_gold.collections_call_list
WHERE run_date = (SELECT MAX(run_date) FROM rsingh_collections_delinquency_prediction_gold.collections_call_list)
ORDER BY priority_rank
LIMIT 25;

-- 3. Weekly SMA-0 book and its 30-day roll rate (NULL until labels mature)
SELECT snapshot_date,
       COUNT(*)                                        AS sma0_loans,
       ROUND(SUM(overdue_amount) / 1e7, 2)             AS overdue_cr,
       ROUND(AVG(rolled_to_sma1_30d), 3)               AS roll_rate
FROM rsingh_collections_delinquency_prediction_gold.collections_features
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 20;

-- 4. Roll rate by DPD band and salary credit: why a salary credit matters
SELECT CASE WHEN dpd_now <= 7 THEN '01-07' WHEN dpd_now <= 15 THEN '08-15'
            WHEN dpd_now <= 23 THEN '16-23' ELSE '24-30' END AS dpd_band,
       salary_credit_last_30d,
       COUNT(*)                         AS loan_weeks,
       ROUND(AVG(rolled_to_sma1_30d), 3) AS roll_rate
FROM rsingh_collections_delinquency_prediction_gold.collections_features
WHERE rolled_to_sma1_30d IS NOT NULL AND salary_account_flag = 1
GROUP BY 1, 2
ORDER BY 1, 2;

-- 5. Trust number and lineage for every daily run
SELECT run_date, run_id, run_ts, model_id, tabicl_version, device, scored_loans, context_rows,
       context_from, context_to, holdout_auc, capture_top10, dpd_only_capture_top10,
       capture_top40, dpd_only_capture_top40, source_snapshot_id, triggered_by
FROM rsingh_collections_delinquency_prediction_gold.collections_model_run
ORDER BY run_date DESC;

-- 6. Did yesterday's top band roll? Join a past call list to the labels that have matured since
SELECT c.run_date, c.treatment,
       COUNT(*)                            AS loans,
       SUM(f.rolled_to_sma1_30d)           AS rolled,
       ROUND(AVG(f.rolled_to_sma1_30d), 3) AS roll_rate,
       ROUND(AVG(c.p_roll), 3)             AS mean_p_roll
FROM rsingh_collections_delinquency_prediction_gold.collections_call_list c
JOIN rsingh_collections_delinquency_prediction_gold.collections_features f
  ON f.loan_id = c.loan_id AND f.snapshot_date = c.snapshot_date
WHERE f.rolled_to_sma1_30d IS NOT NULL
GROUP BY c.run_date, c.treatment
ORDER BY c.run_date, MIN(c.priority_rank);

-- 7. Collector outcomes captured in the app (fed into tomorrow's features by the CDE silver job)
SELECT contact_ts, loan_id, channel, outcome, ptp_date, ptp_amount, recorded_by, notes
FROM rsingh_collections_delinquency_prediction_bronze.collector_outcomes
ORDER BY contact_ts DESC
LIMIT 50;

-- 8. Iceberg time travel: the exact context a past run learned from
DESCRIBE HISTORY rsingh_collections_delinquency_prediction_gold.collections_features;

-- use collections_model_run.source_snapshot_id, context_from, context_to and context_rows of the run
SELECT COUNT(*) AS context_rows, ROUND(AVG(rolled_to_sma1_30d), 3) AS context_roll_rate
FROM (
  SELECT rolled_to_sma1_30d
  FROM rsingh_collections_delinquency_prediction_gold.collections_features FOR SYSTEM_VERSION AS OF 1234567890123456789
  WHERE rolled_to_sma1_30d IS NOT NULL
    AND snapshot_date BETWEEN DATE '2025-08-22' AND DATE '2026-08-21'
  ORDER BY snapshot_date DESC, loan_id
  LIMIT 50000
) ctx;

-- labels maturing: how a snapshot's rows looked before the latest load
SELECT snapshot_date, COUNT(*) AS rows_, COUNT(rolled_to_sma1_30d) AS labelled
FROM rsingh_collections_delinquency_prediction_gold.collections_features FOR SYSTEM_TIME AS OF now() - INTERVAL 1 DAYS
GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 6;
