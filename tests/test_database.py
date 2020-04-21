
import os
import pytest

import dataset

from pymemdb import Database, Table, TableAlreadyExists


def test_database_create():
    db = Database()
    db.create_table("mytable", primary_id="test")

    assert db.tables == ["mytable"]
    assert db["mytable"].idx_name == "test"


def test_drop_table():
    db = Database()
    db.create_table("mytable", primary_id="test")
    db.create_table("mytable2", primary_id="test")
    db.create_table("mytable3", primary_id="test")

    db.drop_table("mytable2")

    assert db.tables == ["mytable", "mytable3"]


def test_delete_invalid_table():
    db = Database()

    with pytest.raises(KeyError):
        db.drop_table("foo")


def test_access_table():
    db = Database()
    assert isinstance(db["mytable"], Table)
    assert db["mytable"].name == "mytable"


def test_assign_new_table():
    t = Table()
    db = Database()
    db["test"] = t

    assert "test" in db.tables


def test_assign_wrong_type():
    db = Database()
    with pytest.raises(TypeError):
        db["mytable"] = "Hello World"


def test_error_on_overwrite():
    t1 = Table()
    t2 = Table()
    db = Database()
    db["mytable"] = t1
    exp_msg = "Table 'mytable' already exists in the Database!"
    with pytest.raises(TableAlreadyExists, match=exp_msg):
        db["mytable"] = t2


def test_sqlitedb_create(tmpdir):
    db = Database()
    db.create_table("a", primary_id="col1")
    db.create_table("b", primary_id="col1b")
    db["a"].insert({"col1": 1, "col2": "stringcol"})
    db["b"].insert({"col1b": 1, "col2b": 3.5})

    dataset_db = dataset.connect(f"sqlite:///{tmpdir / 'test.db'}")
    sqlitedb = db.to_dataset(dataset_db, chunk_size=1)

    assert list(sqlitedb["a"].all()) == list(db["a"].all())
    assert list(sqlitedb["b"].all()) == list(db["b"].all())
    assert os.path.isfile(tmpdir/"test.db")


def test_raise_table_already_exists():
    db = Database()
    db.create_table("table")

    with pytest.raises(TableAlreadyExists):
        db.create_table("table")
