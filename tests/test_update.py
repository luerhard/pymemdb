from pymemdb import Table

def test_update_single(big_table):
    t = Table()

    row1 = dict(vorname="rainer", nachname="greiff")
    row2 = dict(vorname="rainer", nachname="greiff")
    row3 = dict(vorname="rainer", nachname="erhard")

    for row in [row1, row2, row3]:
        t.insert(row)

    row_vals_before = set(t["vorname"].values["rainer"])
    t.update(dict(vorname="rainer", nachname="erhard"), n_values=45)

    row_vals_after = set(t["vorname"].values["rainer"])
    result = list(t.find(n_values=45))
    assert len(result) == 1
    assert row_vals_before == row_vals_after