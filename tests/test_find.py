from pymemdb import Table


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