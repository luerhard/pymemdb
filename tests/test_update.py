from pymemdb import Table

def test_update_single():
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


def test_update_replace():
    t = Table()

    row1 = dict(vorname="rainer", nachname="greiff")
    row2 = dict(vorname="rainer", nachname="smith")
    row3 = dict(vorname="rainer", nachname="erhard")

    for row in [row1, row2, row3]:
        t.insert(row)

    t.update_replace(where={"nachname": "smith"}, nachname="greiff")

    assert len(t) == 2
    assert len(list(t.find(nachname="greiff"))) == 1