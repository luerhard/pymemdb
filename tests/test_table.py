from pymemdb import Table, UniqueConstraintError, ColumnDoesNotExist


def test_number_of_rows():
    table = Table(primary_id="pk")
    rows = []
    for i in range(167):
        row = {"pk": i, "a": 1, "b": 2, "c": 0}
        table.insert(row)
        rows.append(row)

    assert list(table.all()) == rows


def test_delete_simple():
    t = Table()
    t.insert(dict(b=2))
    t.insert(dict(a="John"))
    t.insert(dict(a="John"))
    t.delete(a="John")

    assert list(t.find(a="John")) == []
    assert len(list(t.all())) == 1


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


def test_update():
    t = Table(primary_id="id")
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


def test_equal_tables():
    t1 = Table(name="foo")
    t2 = Table(name="bar")
    t3 = Table(name="foo")

    assert t1 == t3
    assert t1 != t2
