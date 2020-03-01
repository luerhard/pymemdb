
from pymemdb import Database, Table


def test_database_create():
    db = Database()
    db.create_table("mytable", primary_id="test")

    assert db.tables == ["mytable"]
    assert db["mytable"].pk_id == "test"


def test_drop_table():
    db = Database()
    db.create_table("mytable", primary_id="test")
    db.create_table("mytable2", primary_id="test")
    db.create_table("mytable3", primary_id="test")

    db.drop_table("mytable2")

    assert db.tables == ["mytable", "mytable3"]


def test_access_table():
    db = Database()
    assert isinstance(db["mytable"], Table)
    assert db["mytable"].name == "mytable"
