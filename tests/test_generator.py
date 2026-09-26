from datetime import date, timedelta


def _loans(generator, n=400):
    return [x for x in (generator.simulate_loan(i, generator.DEFAULT_SEED) for i in range(n)) if x]


def test_deterministic(generator):
    a = generator.simulate_loan(7, 1)
    b = generator.simulate_loan(7, 1)
    assert a == b


def test_instalments_consistent(generator):
    for loan in _loans(generator):
        ins = loan["instalments"]
        dues = [i[1] for i in ins]
        assert dues == sorted(dues)
        assert all(d >= generator.HISTORY_START for d in dues)
        for k, due, emi, paid, amount, channel in ins:
            assert emi > 0
            if paid is not None:
                assert paid >= due - timedelta(days=3)
                assert (paid - due).days <= generator.CARE_DAYS + 400  # later EMIs in arrears pay on the cure date
        # once an EMI is never paid (NPA) no later EMI exists
        unpaid = [i for i in ins if i[3] is None]
        assert len(unpaid) <= 1 or all(i[3] is None for i in ins[ins.index(unpaid[0]):])


def test_arrears_paid_together(generator):
    """An EMI falling due while the loan is in arrears is paid on the cure date, so DPD
    always counts from the oldest unpaid due date."""
    for loan in _loans(generator):
        ins = loan["instalments"]
        for prev, cur in zip(ins, ins[1:]):
            if prev[3] is not None and prev[3] > cur[1]:
                assert cur[3] == prev[3]


def test_book_shape(generator):
    loans = _loans(generator, 2000)
    products = {x["product_code"] for x in loans}
    assert products == {1, 2, 3, 4, 5}
    missed = sum(1 for x in loans for i in x["instalments"] if i[3] is None or i[3] > i[1])
    total = sum(len(x["instalments"]) for x in loans)
    assert 0.03 < missed / total < 0.25


def test_add_months_clamps_day(generator):
    assert generator.add_months(date(2025, 1, 31), 1, 31) == date(2025, 2, 28)
    assert generator.add_months(date(2025, 11, 15), 3, 15) == date(2026, 2, 15)
