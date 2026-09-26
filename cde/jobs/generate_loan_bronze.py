"""
Stage 1 - Generate (bronze)

Synthesises the source extracts a collections team works from, for a retail
loan book, and lands them raw in bronze:

  loan_master          loan x static attributes (product, EMI, tenor, repayment mode)
  loan_instalments     loan x instalment: due date, EMI, paid date (NULL = unpaid as of the load)
  nach_presentations   loan x NACH auto-debit presentation: SUCCESS / BOUNCED + reason
  dialler_contacts     loan x collection attempt: channel, outcome, promise-to-pay (PTP) date + amount
  casa_credits         customer x salary credit landing in the customer's account with the bank
  bureau_snapshot      customer x monthly bureau score pull
  ref.product_map      product_code -> name, tenor and rate bands

In production this job would read the LMS / NACH / dialler / CBS extracts
instead.

How the synthetic book behaves: each loan has a hidden monthly stress level
(an AR(1) around a loan-level risk set by product and origination bureau score,
plus occasional multi-month shocks such as a job loss, plus a small book-wide
macro factor). Stress drives everything the collector can see:

  * the chance of missing an EMI (NACH bounce on the due date)
  * a missed salary credit during a shock, and the bureau score drifting down
  * contactability, and whether a promise-to-pay is kept

Once an EMI is missed, the loan is simulated day by day until it cures: a
salary credit, a reached contact or a NACH re-presentation on day 10 raise the
chance of paying that day. A loan that is not cured by day 120 becomes NPA and
stops. Later EMIs falling due while in arrears are paid on the cure date, so
days past due (DPD) always counts from the oldest unpaid due date.

History is prefix-stable: the world is simulated from a fixed seed up to
WORLD_END and --as-of only truncates it (events after the as-of date are
dropped, payments after it become NULL). Running with a later as-of adds new
days without changing any earlier fact, so daily runs behave like real loads.

Writes (drop + recreate every run):
  <prefix>_bronze.{loan_master, loan_instalments, nach_presentations,
                   dialler_contacts, casa_credits, bureau_snapshot}
  <prefix>_ref.product_map

Usage:
  spark-submit generate_loan_bronze.py [--as-of YYYY-MM-DD] [--db-prefix P]
                                       [--loans N] [--seed S]
"""

from __future__ import annotations

import argparse
import logging
import math
import random
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DateType, DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_collections_delinquency_prediction"
DEFAULT_SEED = 20240401
DEFAULT_LOANS = 150_000
HISTORY_START = date(2024, 4, 1)   # first event in the extracts; features need 12 months before the first snapshot
DISB_START = date(2019, 1, 1)
DISB_END = date(2027, 6, 30)
WORLD_END = date(2027, 12, 31)
CARE_DAYS = 120                    # not cured by then -> NPA, loan leaves the book
REPRESENT_DAY = 10                 # NACH re-presentation after a bounce
DUPLICATE_RATE = 0.0002            # re-sent records, removed in silver
BOUNCE_REASONS = [("INSUFFICIENT_FUNDS", 0.82), ("ACCOUNT_FROZEN", 0.05),
                  ("MANDATE_CANCELLED", 0.06), ("PAYMENT_STOPPED", 0.07)]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]

# share: of loans; tenors in months (None = revolving card, minimum amount due);
# emi: median in INR; risk: offset to the loan's stress; salaried / nach: shares.
PRODUCTS = {
    1: dict(name="PERSONAL_LOAN", prefix="PL", share=0.30, tenors=[12, 24, 36, 48, 60], emi=9000,
            rate=(11.0, 16.0), risk=0.25, salaried=0.65, nach=0.92),
    2: dict(name="TWO_WHEELER", prefix="TW", share=0.22, tenors=[12, 18, 24, 36], emi=3500,
            rate=(9.0, 14.0), risk=0.45, salaried=0.35, nach=0.85),
    3: dict(name="CREDIT_CARD", prefix="CC", share=0.18, tenors=[None], emi=2500,
            rate=(36.0, 42.0), risk=0.55, salaried=0.50, nach=0.55),
    4: dict(name="HOME_LOAN", prefix="HL", share=0.12, tenors=[180, 240], emi=28000,
            rate=(8.4, 9.6), risk=-0.70, salaried=0.70, nach=0.97),
    5: dict(name="MSME_TERM_LOAN", prefix="ML", share=0.18, tenors=[24, 36, 48, 60], emi=18000,
            rate=(10.0, 14.0), risk=0.15, salaried=0.0, nach=0.88),
}

# Book-wide stress by month: post-festive overspend and a weak monsoon quarter.
MACRO = {(2025, 11): 0.20, (2025, 12): 0.35, (2026, 1): 0.35, (2026, 2): 0.20,
         (2026, 6): 0.15, (2026, 7): 0.25, (2026, 8): 0.15}


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def add_months(d: date, k: int, day: int) -> date:
    y, m = divmod(d.month - 1 + k, 12)
    y, m = d.year + y, m + 1
    nxt = date(y + (m == 12), m % 12 + 1, 1)
    return date(y, m, min(day, (nxt - timedelta(days=1)).day))


def month_index(d: date) -> int:
    return (d.year - HISTORY_START.year) * 12 + d.month - HISTORY_START.month


N_MONTHS = month_index(WORLD_END) + 1


def _pick(rng: random.Random, weighted):
    x, acc = rng.random(), 0.0
    for value, w in weighted:
        acc += w
        if x < acc:
            return value
    return weighted[-1][0]


def simulate_loan(idx: int, seed: int) -> dict | None:
    """Whole life of one loan up to WORLD_END, from a per-loan RNG.

    Returns None for loans that matured before HISTORY_START. Pure Python, so
    the behaviour can be unit tested without Spark.
    """
    rng = random.Random(seed * 1_000_003 + idx)
    code = _pick(rng, [(c, p["share"]) for c, p in PRODUCTS.items()])
    prod = PRODUCTS[code]
    disb = DISB_START + timedelta(days=rng.randint(0, (DISB_END - DISB_START).days))
    tenor = rng.choice(prod["tenors"])
    last_due = add_months(disb, tenor, 28) if tenor else WORLD_END
    if last_due < HISTORY_START:
        return None

    due_day = rng.choice([2, 5, 7, 10, 15])
    emi = round(prod["emi"] * math.exp(rng.gauss(0.0, 0.45)), -1)
    rate = round(rng.uniform(*prod["rate"]), 2)
    salaried = rng.random() < prod["salaried"]
    nach = rng.random() < prod["nach"]
    bureau0 = int(min(880, max(560, rng.gauss(725, 55))))
    mu = (720 - bureau0) / 70.0 + prod["risk"] + rng.gauss(0.0, 0.4)
    unreachable = rng.gauss(0.0, 1.0)
    sal_day = rng.randint(1, 5)
    sal_amount = round(emi * rng.uniform(2.5, 6.0), -2)

    # Monthly hidden stress, salary credits and shocks for every month of the world.
    z, shock_left = mu + rng.gauss(0.0, 0.6), 0
    stress, shocked, salary_dates = [], [], []
    for i in range(N_MONTHS):
        y, m = divmod(HISTORY_START.month - 1 + i, 12)
        y, m = HISTORY_START.year + y, m + 1
        if shock_left == 0 and rng.random() < 0.008:
            shock_left = rng.randint(2, 7)
        in_shock = shock_left > 0
        shock_left = max(0, shock_left - 1)
        z = mu + 0.7 * (z - mu) + 0.45 * rng.gauss(0.0, 1.0)
        stress.append(z + 1.8 * in_shock + MACRO.get((y, m), 0.0))
        shocked.append(in_shock)
        sal_missed = rng.random() < sigmoid(-4.2 + 3.8 * in_shock + 0.4 * (z - mu))
        if salaried and not sal_missed:
            salary_dates.append(date(y, m, sal_day))

    def stress_on(d: date) -> float:
        return stress[min(N_MONTHS - 1, max(0, month_index(d)))]

    def shocked_on(d: date) -> bool:
        return shocked[min(N_MONTHS - 1, max(0, month_index(d)))]

    salary_set = set(salary_dates)

    def salary_recent(d: date) -> bool:
        return any((d - timedelta(days=k)) in salary_set for k in range(5))

    loan_id = f"{prod['prefix']}-{idx:07d}"
    instalments, presentations, contacts = [], [], []
    episode_end: date | None = None      # cure date of the arrears in progress
    npa = False
    serious_months = set()               # months with DPD > 30, for the bureau score
    k = 0
    while True:
        k += 1
        due = add_months(disb, k, due_day)
        if due > last_due or due > WORLD_END or npa:
            break
        if due < HISTORY_START:
            continue
        z = stress_on(due)

        if episode_end is not None and episode_end > due:
            # Still in arrears: this EMI bounces and is paid on the cure date.
            if nach:
                presentations.append((due, emi, "BOUNCED", "INSUFFICIENT_FUNDS", 1))
            instalments.append((k, due, emi, episode_end, emi, "UPI"))
            continue

        sal_missed_now = salaried and date(due.year, due.month, sal_day) not in salary_set
        if rng.random() >= sigmoid(-2.4 + 1.05 * z + 0.9 * sal_missed_now):
            if nach:
                presentations.append((due, emi, "SUCCESS", None, 1))
                instalments.append((k, due, emi, due, emi, "NACH"))
            else:
                instalments.append((k, due, emi, due - timedelta(days=rng.randint(0, 3)), emi,
                                    rng.choice(["UPI", "UPI", "NETBANKING", "CASH"])))
            episode_end = None
            continue

        # Missed EMI: simulate the arrears day by day until cured or NPA.
        if nach:
            presentations.append((due, emi, "BOUNCED", _pick(rng, BOUNCE_REASONS), 1))
        cured, channel, ptp_date, reached_on = None, None, None, None
        for t in range(1, CARE_DAYS + 1):
            day = due + timedelta(days=t)
            z = stress_on(day)
            sal = salary_recent(day)
            shock = shocked_on(day)
            if nach and t == REPRESENT_DAY:
                if rng.random() < sigmoid(-0.4 - 0.8 * z + 2.0 * sal - 2.0 * shock):
                    presentations.append((day, emi, "SUCCESS", None, 2))
                    cured, channel = t, "NACH"
                    break
                presentations.append((day, emi, "BOUNCED", "INSUFFICIENT_FUNDS", 2))
            if ptp_date == day:
                ptp_date = None
                if rng.random() < sigmoid(0.4 - 0.9 * z + 1.0 * sal - 2.0 * shock):
                    cured, channel = t, "UPI"
                    break
            reached_recently = reached_on is not None and (day - reached_on).days <= 7
            if rng.random() < sigmoid(-1.2 - 0.85 * z + 1.3 * sal + 1.2 * reached_recently - 2.0 * shock):
                cured, channel = t, rng.choice(["UPI", "NETBANKING", "CASH", "BRANCH"])
                break
            if t >= 2 and (t - 2) % 3 == 0 and ptp_date is None and t <= 90:
                ch = "IVR" if t <= 7 and rng.random() < 0.5 else ("FIELD" if t > 60 else "CALL")
                hour = rng.randint(9, 18)
                if rng.random() < sigmoid(0.4 - 0.45 * z - 0.9 * unreachable - 1.5 * shock):
                    reached_on = day
                    if rng.random() < 0.6:
                        ptp_date = day + timedelta(days=rng.randint(2, 7))
                        ptp_amount = emi * (1 + t // 30)
                        contacts.append((day, hour, ch, "REACHED", ptp_date, float(ptp_amount)))
                    else:
                        contacts.append((day, hour, ch, "REACHED", None, None))
                else:
                    contacts.append((day, hour, ch, _pick(rng, [("NO_ANSWER", 0.7), ("SWITCHED_OFF", 0.2),
                                                                ("WRONG_NUMBER", 0.1)]), None, None))
        if cured is None:
            npa = True
            episode_end = None
            instalments.append((k, due, emi, None, None, None))
        else:
            episode_end = due + timedelta(days=cured)
            instalments.append((k, due, emi, episode_end, emi, channel))
        if cured is None or cured > 30:
            end = episode_end or due + timedelta(days=CARE_DAYS)
            d = due + timedelta(days=31)
            while d <= end:
                serious_months.add(month_index(d))
                d += timedelta(days=28)

    active_to = WORLD_END if npa or not tenor else last_due
    salary = [(d, sal_amount) for d in salary_dates if max(disb, HISTORY_START) <= d <= active_to]
    bureau = []
    for i in range(N_MONTHS):
        pull = add_months(HISTORY_START, i, 1)
        if pull < disb or pull > active_to:
            continue
        recent = any(j in serious_months for j in range(i - 6, i))
        score = bureau0 - 28 * (stress[i] - mu) - 30 * shocked[i] - 45 * recent + rng.gauss(0.0, 8.0)
        bureau.append((pull, int(min(900, max(300, score)))))

    return dict(
        loan_id=loan_id, customer_id=f"C{idx:08d}", product_code=code, branch_region=rng.choice(REGIONS),
        disbursal_date=disb, tenor_months=tenor or 0, emi_amount=float(emi),
        sanction_amount=float(round(emi * (tenor or 24) * 0.8, -3)), interest_rate=rate,
        repayment_mode="NACH" if nach else "SELF_PAY", salary_account_flag=int(salaried),
        bureau_score_at_origination=bureau0, due_day=due_day,
        instalments=instalments, presentations=presentations, contacts=contacts,
        salary=salary, bureau=bureau,
    )


SIM_SCHEMA = StructType([
    StructField("loan_id", StringType()), StructField("customer_id", StringType()),
    StructField("product_code", IntegerType()), StructField("branch_region", StringType()),
    StructField("disbursal_date", DateType()), StructField("tenor_months", IntegerType()),
    StructField("emi_amount", DoubleType()), StructField("sanction_amount", DoubleType()),
    StructField("interest_rate", DoubleType()), StructField("repayment_mode", StringType()),
    StructField("salary_account_flag", IntegerType()), StructField("bureau_score_at_origination", IntegerType()),
    StructField("due_day", IntegerType()),
    StructField("instalments", ArrayType(StructType([
        StructField("instalment_no", IntegerType()), StructField("due_date", DateType()),
        StructField("emi_amount", DoubleType()), StructField("paid_date", DateType()),
        StructField("paid_amount", DoubleType()), StructField("payment_channel", StringType())]))),
    StructField("presentations", ArrayType(StructType([
        StructField("presentation_date", DateType()), StructField("amount", DoubleType()),
        StructField("status", StringType()), StructField("bounce_reason", StringType()),
        StructField("presentation_seq", IntegerType())]))),
    StructField("contacts", ArrayType(StructType([
        StructField("contact_date", DateType()), StructField("contact_hour", IntegerType()),
        StructField("channel", StringType()), StructField("outcome", StringType()),
        StructField("ptp_date", DateType()), StructField("ptp_amount", DoubleType())]))),
    StructField("salary", ArrayType(StructType([
        StructField("credit_date", DateType()), StructField("amount", DoubleType())]))),
    StructField("bureau", ArrayType(StructType([
        StructField("pull_date", DateType()), StructField("bureau_score", IntegerType())]))),
])
_FIELDS = [f.name for f in SIM_SCHEMA.fields]


def _simulate_partition(seed: int):
    def run(rows):
        for row in rows:
            loan = simulate_loan(int(row.id), seed)
            if loan is not None:
                yield tuple(loan[f] for f in _FIELDS)
    return run


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--as-of", default=None, help="last business date in the extracts, YYYY-MM-DD (default: yesterday UTC)")
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    p.add_argument("--loans", type=int, default=DEFAULT_LOANS, help="loans in the simulated world (not all active)")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args, _ = p.parse_known_args(argv)
    return args


def resolve_as_of(value: str | None) -> date:
    if value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _with_dupes(df, *keys):
    dupes = df.where((F.abs(F.xxhash64(*keys)) % 1_000_000) < DUPLICATE_RATE * 1_000_000)
    return df.unionByName(dupes)


def _write(df, name: str, partition_col=None) -> None:
    w = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    if partition_col is not None:
        w = w.partitionedBy(partition_col)
    w.createOrReplace()


def run(spark: SparkSession, args: argparse.Namespace) -> None:
    as_of = resolve_as_of(args.as_of)
    bronze_db, ref_db = f"{args.db_prefix}_bronze", f"{args.db_prefix}_ref"
    logger.info("Simulating %d loans, events %s -> %s (world to %s), seed %d",
                args.loans, HISTORY_START, as_of, WORLD_END, args.seed)

    parts = max(8, args.loans // 5000)
    sim = (spark.range(args.loans, numPartitions=parts).rdd
           .mapPartitions(_simulate_partition(args.seed)))
    world = spark.createDataFrame(sim, SIM_SCHEMA).where(F.col("disbursal_date") <= F.lit(as_of)).cache()
    cut = F.lit(as_of).cast("date")
    batch = [F.lit(as_of).cast("date").alias("batch_as_of"),
             F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast("timestamp").alias("ingested_at")]

    master = world.select(*[c for c in _FIELDS if c not in ("instalments", "presentations", "contacts",
                                                             "salary", "bureau")], *batch)
    inst = (world.select("loan_id", F.explode("instalments").alias("i")).select("loan_id", "i.*")
            .where(F.col("due_date") <= cut))
    unpaid_yet = F.col("paid_date").isNull() | (F.col("paid_date") > cut)
    inst = (inst.withColumn("paid_amount", F.when(unpaid_yet, None).otherwise(F.col("paid_amount")))
            .withColumn("payment_channel", F.when(unpaid_yet, None).otherwise(F.col("payment_channel")))
            .withColumn("paid_date", F.when(unpaid_yet, None).otherwise(F.col("paid_date")))
            .select("loan_id", "instalment_no", "due_date", "emi_amount", "paid_date", "paid_amount",
                    "payment_channel", *batch))
    nach = (world.select("loan_id", F.explode("presentations").alias("p")).select("loan_id", "p.*")
            .where(F.col("presentation_date") <= cut).select("*", *batch))
    contacts = (world.select("loan_id", F.posexplode("contacts").alias("n", "c"))
                .select("loan_id", "n", "c.*")
                .where(F.col("contact_date") <= cut)
                .select(F.concat_ws("-", "loan_id", F.lpad(F.col("n").cast("string"), 4, "0")).alias("contact_id"),
                        "loan_id",
                        F.to_timestamp(F.concat_ws(" ", F.col("contact_date").cast("string"),
                                                   F.format_string("%02d:00:00", "contact_hour"))).alias("contact_ts"),
                        "channel", "outcome", "ptp_date", "ptp_amount", *batch))
    salary = (world.select("customer_id", F.explode("salary").alias("s")).select("customer_id", "s.*")
              .where(F.col("credit_date") <= cut)
              .select("customer_id", "credit_date", "amount", F.lit("SALARY").alias("credit_type"), *batch))
    bureau = (world.select("customer_id", F.explode("bureau").alias("b")).select("customer_id", "b.*")
              .where(F.col("pull_date") <= cut).select("*", *batch))
    products = spark.createDataFrame(
        [(c, p["name"], min(t or 0 for t in p["tenors"]), max(t or 0 for t in p["tenors"]),
          p["rate"][0], p["rate"][1], p["tenors"] == [None]) for c, p in PRODUCTS.items()],
        "product_code int, product_name string, min_tenor_months int, max_tenor_months int, "
        "min_rate double, max_rate double, revolving boolean")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ref_db}")
    _write(products.withColumn("ingested_at", F.current_timestamp()), f"{ref_db}.product_map")
    _write(master, f"{bronze_db}.loan_master")
    _write(_with_dupes(inst, "loan_id", "instalment_no"), f"{bronze_db}.loan_instalments", F.years("due_date"))
    _write(nach, f"{bronze_db}.nach_presentations", F.years("presentation_date"))
    _write(_with_dupes(contacts, "contact_id"), f"{bronze_db}.dialler_contacts", F.years("contact_ts"))
    _write(salary, f"{bronze_db}.casa_credits", F.years("credit_date"))
    _write(bureau, f"{bronze_db}.bureau_snapshot", F.years("pull_date"))
    world.unpersist()

    for t in ("loan_master", "loan_instalments", "nach_presentations", "dialler_contacts",
              "casa_credits", "bureau_snapshot"):
        logger.info("%s.%s: %d rows", bronze_db, t, spark.table(f"{bronze_db}.{t}").count())
    logger.info("Bronze load as of %s complete", as_of)


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("coll-generate-loan-bronze").getOrCreate()
    try:
        run(spark, args)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()
