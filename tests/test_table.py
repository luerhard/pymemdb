import pytest

from pymemdb import Table, UniqueConstraintError, Database, ColumnDoesNotExist


@pytest.fixture(scope="session")
def big_table():
    t = Table(primary_id="normal")
    for i in range(100):
        t.insert({"normal": i, "squared": i**2, "cubed": i**3})

    return t


def test_single_insert_auto_primary():
    table = Table(primary_id="pk")
    row = {"a": 1, "b": 2, "c": 0}
    table.insert(row)
    assert list(table.all()) == [{"pk": 1, **row}]


def test_single_insert_given_primary():
    table = Table(primary_id="pk")
    row = {"pk": 1, "a": 1, "b": 2, "c": 0}
    table.insert(row)
    assert list(table.all()) == [{**row}]


def test_number_of_rows():
    table = Table(primary_id="pk")
    rows = []
    for i in range(167):
        row = {"pk": i, "a": 1, "b": 2, "c": 0}
        table.insert(row)
        rows.append(row)

    assert list(table.all()) == rows


def test_unique_constraint():
    t = Table()
    t.create_column("a", unique=True)
    t.insert({"a": 1, "b": 2})
    t.insert({"a": 2})

    with pytest.raises(UniqueConstraintError):
        t.insert({"a": 1})


def test_find_rows(big_table):
    result = next(big_table.find(normal=8))
    assert result["squared"] == 64


def test_find_col_not_exists(big_table):
    with pytest.raises(KeyError):
        result = list(big_table.find(quadratic=17, ignore_errors=False))

    result = list(big_table.find(quadratic=17, ignore_errors=True))
    assert result == []


def test_find_in(big_table):
    result = list(big_table.find(normal=[7, 8]))
    assert {row["squared"] for row in result} == {49, 64}


def test_find_multiple_keys(big_table):
    result = list(big_table.find(normal=3, squared=9))
    assert len(result) == 1
    assert result[0]["cubed"] == 27


def test_no_find_multiple_keys(big_table):
    result = list(big_table.find(normal=3, squared=10))
    assert len(result) == 0


def test_find_with_default_val():
    t = Table(primary_id="b")
    t.insert(dict(b=1))
    t.insert(dict(b=2))
    t.insert(dict(a=1, b=3))
    results = list(t.find(a=None))
    assert results == [dict(b=1, a=None), dict(b=2, a=None)]


def test_find_with_multiple_default_vals():
    t = Table(primary_id="b")
    t.create_column("firstname", default="John")
    t.create_column("lastname", default="Smith")
    t.insert(dict(b=1))
    t.insert(dict(b=2))
    t.insert(dict(b=3, lastname="Doe"))

    result = t.find(lastname="Smith")
    result2 = t.find(b=2)
    result3 = t.find(firstname="John", lastname="Smith")


    assert [r["b"] for r in result] == [1, 2]
    assert list(result2) == [dict(b=2, firstname="John", lastname="Smith")]
    assert [r["b"] for r in result3] == [1, 2]


def test_delete_find_delete():
    t = Table(primary_id="a")
    t.insert({"a": 1, "b": 2})
    t.insert({"a": 2, "b": 5})
    t.insert({"a": 3, "b": 6})

    result = list(t.find(a=1))
    assert len(result) == 1
    assert len(t) == 3

    n_delete = t.delete(a=1)

    result = list(t.find(a=1))
    print(result)
    assert n_delete == 1
    assert len(result) == 0
    assert len(t) == 2
    assert list(t.all(ordered="ascending")) == [{"a": 2, "b": 5},
                                                {"a": 3, "b": 6}]


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


def test_update():
    t = Table(primary_id="a")
    t.insert({"a": 1, "b": 2})
    t.insert({"a": 2, "b": 5})
    t.insert({"a": 3, "b": 6})
    t.insert({"a": 3, "b": 7})
    t.insert({"a": 3, "b": 8})

    t.update(where={"a": 2}, b=5)
    result = next(t.find(a=2))
    assert result["b"] == 5
    t.update(dict(a=3), b=10)

    result = list(t.find(a=3))
    assert all(row["b"] == 10 for row in result)


def test_update_no_vals():
    t = Table(primary_id="a")
    row = {"a": 1, "b": 2}
    t.insert({"a": 1, "b": 2})
    t.update(where={"a": 2}, b=5)
    assert list(t.all()) == [row]


def test_insert_ignore(big_table):
    big_table.insert(dict(normal=101, squared=102))

    assert next(big_table.find(normal=101))["squared"] == 102
    assert len(big_table) == 101

    for _ in range(3):
        big_table.insert_ignore(dict(normal=102, squared=102),
                                keys=["normal", "squared"])

    assert len(big_table) == 102
    assert len(list(big_table.find(normal=102))) == 1
    assert len(list(big_table.find(squared=102))) == 2


def test_get_all_ordered_descending():
    t = Table(primary_id="a")
    t.insert({"a": 1, "b": 2})
    t.insert({"a": 2, "b": 5})
    t.insert({"a": 3, "b": 6})

    results = t.all(ordered="descending")

    assert [r["b"] for r in results] == [6, 5, 2]


def test_invalid_arg_ordered():
    t = Table(primary_id="a")
    t.insert({"a": 1})

    with pytest.raises(ValueError):
        list(t.all(ordered="foo"))


def test_equal_tables():
    t1 = Table(name="foo")
    t2 = Table(name="bar")
    t3 = Table(name="foo")

    assert t1 == t3
    assert t1 != t2

def test_invalid_colname():
    t1 = Table()
    with pytest.raises(ColumnDoesNotExist):
        t1["g"]
