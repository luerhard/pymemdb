from pymemdb import Table, ColumnDoesNotExist


def test_add_columns():
    t = Table(primary_id="id")
    t.create_column("a")
    t.create_column("b")

    assert t.columns == ["id", "a", "b"]


def test_delete_columns():
    t = Table(primary_id="primary_key")
    t.create_column("a")
    t.create_column("b")
    del t["b"]

    assert t.columns == ["primary_key", "a"]


def test_len_of_column():
    t = Table()
    t.insert({"a": 1})
    t.insert({"a": 1})
    t.insert({"a": 1})

    assert len(t["a"]) == 3

