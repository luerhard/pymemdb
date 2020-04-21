from __future__ import annotations
from typing import List, Optional

import dataset

from pymemdb import TableAlreadyExists
from pymemdb import Table


class Database:

    def __init__(self) -> None:
        self._tables: dict = dict()

    def create_table(self, name: str, primary_id: str = "id") -> Table:
        if name in self._tables:
            raise TableAlreadyExists(name)

        self._tables[name] = Table(name, primary_id=primary_id)

        return self._tables[name]

    def drop_table(self, name: str) -> None:
        self._tables[name].drop()
        del self._tables[name]

    def __getitem__(self, name: str):
        if name not in self._tables:
            table = self.create_table(name)
            return table
        return self._tables[name]

    def __setitem__(self, key, item):
        if key in self._tables:
            raise TableAlreadyExists(key)
        if not isinstance(item, Table):
            raise TypeError(f"{item} not an instance of 'Table'!")
        self._tables[key] = item

    @property
    def tables(self) -> List[Optional[str]]:
        return list(self._tables)

    def to_dataset(self, db: dataset.Database, chunk_size: int = 1000) -> dataset.Database:
        for tablename in self.tables:
            pk_id = self._tables[tablename].idx_name
            tableobject = db.create_table(tablename, primary_id=pk_id)
            tableobject.insert_many(list(self._tables[tablename].all()),
                                    chunk_size=chunk_size)

        return db

    @classmethod
    def from_dataset(cls, db: dataset.Database) -> Database:
        pymemdb_database = cls()

        for tablename in db.tables:
            pymemdb_database[tablename] = Table.from_dataset(db[tablename])

        return pymemdb_database
