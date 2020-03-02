import pytest

from pymemdb import Table, UniqueConstraintError, ColumnDoesNotExist

def test_unique_constraint():
    t = Table()
    t.create_column("a", unique=True)
    t.insert({"a": 1, "b": 2})
    t.insert({"a": 2})

    with pytest.raises(UniqueConstraintError):
        t.insert({"a": 1})


def test_find_col_not_exists(big_table):
    with pytest.raises(KeyError):
        result = list(big_table.find(quadratic=17, ignore_errors=False))

    result = list(big_table.find(quadratic=17, ignore_errors=True))
    assert result == []


def test_delete_invalid_columns():
    t = Table()
    t.create_column("a")
    t.create_column("b")

    with pytest.raises(ColumnDoesNotExist):
        del t["c"]


def test_invalid_delete():
    t = Table(primary_id="a")
    row = {"a": 1, "foo": "bar"}
    t.insert(row)
    t.delete(a=5, ignore_errors=True)
    assert list(t.all()) == [row]

    with pytest.raises(KeyError):
        t.delete(a=5, ignore_errors=False)

    assert t.delete(a=1, ignore_errors=False) == 1
    assert len(t) == 0


def test_invalid_colname():
    t1 = Table()
    with pytest.raises(ColumnDoesNotExist):
        t1["g"]


def test_insert_pk_twice():
    t = Table(primary_id="pk")
    t.insert(dict(pk=1, b=2))
    with pytest.raises(UniqueConstraintError):
        t.insert(dict(pk=1, b=3))


def test_invalid_arg_ordered():
    t = Table(primary_id="a")
    t.insert({"a": 1})

    with pytest.raises(ValueError):
        list(t.all(ordered="foo"))
