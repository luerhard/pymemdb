from pymemdb import Table

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