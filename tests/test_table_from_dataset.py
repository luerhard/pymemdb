import pymemdb
import dataset


def test_simple_table():
    db = dataset.connect("sqlite:///:memory:", row_type=dict)
    simple_table = db.create_table("my_table")
    simple_table.insert_many([dict(a=i, b=1) for i in range(10)])

    pytable = pymemdb.Table.from_dataset(simple_table)

    assert pytable.name == "my_table"

    assert list(pytable.all()) == list(simple_table.all())

def test_simple_table_non_default_pk():
    db = dataset.connect("sqlite:///:memory:", row_type=dict)
    simple_table = db.create_table("my_table", primary_id="a")
    simple_table.insert_many([dict(a=i, b=1) for i in range(10)])

    pytable = pymemdb.Table.from_dataset(simple_table)

    testrow = dict(a=21, b=2)
    simple_table.insert(testrow)
    pytable.insert(testrow)

    assert list(pytable.all()) == list(simple_table.all())
