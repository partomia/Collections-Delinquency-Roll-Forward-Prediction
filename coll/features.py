"""Model inputs: the numeric columns of gold.collections_features.

Must match FEATURES / LABEL in cde/jobs/build_gold_features.py (the CDE jobs
are self-contained, so the list lives in both places; a test checks they agree).
"""

FEATURES = [
    "product_code",            # 1 personal, 2 two-wheeler, 3 credit card, 4 home, 5 MSME term loan
    "dpd_now",                 # 1..30
    "overdue_amount",          # INR, unpaid EMIs + bounce charges
    "emi_amount",
    "overdue_to_emi",
    "months_on_book",
    "max_dpd_12m",
    "nach_bounces_6m",
    "broken_ptp_3m",           # promises-to-pay not kept
    "contacts_reached_30d",
    "salary_credit_last_30d",  # 1 if a salary credit landed in the account
    "salary_account_flag",     # 1 if the customer's salary account is with the bank
    "bureau_score",
]
LABEL = "rolled_to_sma1_30d"
KEYS = ["loan_id", "snapshot_date"]
COLUMNS = [*KEYS, *FEATURES, LABEL]

PRODUCTS = {1: "Personal loan", 2: "Two-wheeler", 3: "Credit card", 4: "Home loan", 5: "MSME term loan"}
