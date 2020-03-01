import pytest

from pymemdb import Table, ColumnDoesNotExist


def test_add_columns():
    t = Table()
    t.create_column("a")
    t.create_column("b")

    assert t.columns == ["a", "b"]


def test_delete_columns():
    t = Table()
    t.create_column("a")
    t.create_column("b")
    del t["b"]

    assert t.columns == ["a"]


def test_delete_invalid_columns():
    t = Table()
    t.create_column("a")
    t.create_column("b")

    with pytest.raises(ColumnDoesNotExist):
        del t["c"]


def test_len_of_column():
    t = Table()
    t.insert({"a": 1})
    t.insert({"a": 1})
    t.insert({"a": 1})

    assert len(t["a"]) == 3


def test_no_insert_columns_on_find():
    t = Table(primary_id="a")
    t.insert({"a": 1})
    t.insert({"a": 2})

    assert list(t.find(a=1, b=2, ignore_errors=True)) == [{"a": 1}]
    assert t.columns == ["a"]
