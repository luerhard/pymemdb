import dataset

import pymemdb


def test_simple_db():
    db = dataset.Database("sqlite:///:memory:")

    table1 = db.create_table("table1", primary_id="a")
    table1.insert(dict(a=1, b="2"))

    table2 = db.create_table("table2")
    table2.insert(dict(name="John", lastname="Doe"))

    assert db.tables == ["table1", "table2"]
    pymem_db = pymemdb.Database.from_dataset(db)

    assert list(pymem_db["table1"].all()) == list(table1.all())
    assert list(pymem_db["table2"].all()) == list(table2.all())
