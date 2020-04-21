import pytest

import dataset

import pymemdb

@pytest.fixture(scope="function")
def simple_dataset_db():
    db = dataset.connect("sqlite:///:memory:", row_type=dict)
    simple_table = db.create_table("my_table")
    simple_table.insert_many([dict(a=i, b=1) for i in range(10)])

    return db


def test_simple_table(simple_dataset_db):
    simple_table = simple_dataset_db["my_table"]
    pytable = pymemdb.Table.from_dataset(simple_table)

    assert pytable.name == "my_table"
    assert list(pytable.all()) == list(simple_table.all())

def test_simple_table_non_default_pk():
   # simple_table = simple_dataset_db["my_table"]

    db = dataset.connect("sqlite:///:memory:", row_type=dict)
    simple_table = db.create_table("my_table", primary_id="a")
    simple_table.insert_many([dict(a=i, b=1) for i in range(10)])
    pytable = pymemdb.Table.from_dataset(simple_table)

    testrow = dict(a=21, b=2)
    simple_table.insert(testrow)
    pytable.insert(testrow)

    assert list(pytable.all()) == list(simple_table.all())


def test_to_dataset_with_drop(simple_dataset_db):
    rows = [{"b": i, "c": 3} for i in range(1, 6)]
    t = pymemdb.Table("my_table", primary_id="id")
    for r in rows:
        t.insert(r)

    t.to_dataset(simple_dataset_db, drop=True)
    assert list(simple_dataset_db["my_table"].all()) == list(t.all())

@pytest.mark.xfail
def test_to_dataset_with_drop_change_pk(simple_dataset_db):
    """known strange behavior addressd in
        https://github.com/pudo/dataset/issues/329
    """
    rows = [{"b": i, "c": 3} for i in range(1, 6)]
    t = pymemdb.Table("my_table", primary_id="b")
    for r in rows:
        t.insert(r)

    t.to_dataset(simple_dataset_db, drop=True)
    assert list(simple_dataset_db["my_table"].all()) == list(t.all())


def test_to_dataset_no_drop(simple_dataset_db):
    rows = [{"b": i, "c": 3} for i in range(1, 6)]
    t = pymemdb.Table("my_table2", primary_id="b")
    for r in rows:
        t.insert(r)

    t.to_dataset(simple_dataset_db, drop=False)

    assert list(simple_dataset_db["my_table2"].all()) == list(t.all())
