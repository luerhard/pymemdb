import pytest

from pymemdb import Table, UniqueConstraintError, Database


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


def test_delete_find_delete():
    t = Table(primary_id="a")
    t.insert({"a": 1, "b": 2})
    t.insert({"a": 2, "b": 5})
    t.insert({"a": 3, "b": 6})

    result = list(t.find(a=1))
    assert len(result) == 1
    assert len(t) == 3

    t.delete(a=1)

    result = list(t.find(a=1))
    print(result)
    assert len(result) == 0
    assert len(t) == 2
    assert list(t.all(ordered="ascending")) == [{"a": 2, "b": 5},
                                                {"a": 3, "b": 6}]


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
