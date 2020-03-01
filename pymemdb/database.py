import dataset
from .errors import TableAlreadyExists
from .table import Table


class Database:

    def __init__(self):
        self._tables = dict()

    def create_table(self, name, primary_id="pk"):
        if name in self._tables:
            raise TableAlreadyExists(f"Table with Name {name} already exists in the Database")

        self._tables[name] = Table(name, primary_id=primary_id)

        return self._tables[name]

    def drop_table(self, name):
        self._tables[name].drop()
        del self._tables[name]

    def __getitem__(self, name):
        if name not in self._tables:
            table = self.create_table(name)
            return table
        return self._tables[name]

    @property
    def tables(self):
        return list(self._tables)

    def to_sqlite(self, file, chunk_size=1_000):

        db = dataset.connect(f"sqlite:///{file}")
        for tablename in self.tables:
            pk_id = self._tables[tablename].pk_id
            tableobject = db.create_table(tablename, primary_id=pk_id)
            tableobject.insert_many(list(self._tables[tablename].all()),
                                    chunk_size=chunk_size)

        return db