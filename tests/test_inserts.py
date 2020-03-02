from pymemdb import Table

def test_single_insert_auto_primary():
    table = Table(primary_id="pk")
    row = {"a": 1, "b": 2, "c": 0}
    table.insert(row)
    assert list(table.all()) == [{"pk": 0, **row}]


def test_single_insert_given_primary():
    table = Table(primary_id="pk")
    row = {"pk": 1, "a": 1, "b": 2, "c": 0}
    table.insert(row)
    assert list(table.all()) == [{**row}]



def test_no_insert_columns_on_find():
    t = Table(primary_id="a")
    t.insert({"a": 1})
    t.insert({"a": 2})

    assert list(t.find(a=1, b=2, ignore_errors=True)) == [{"a": 1}]
    assert t.columns == ["a"]
