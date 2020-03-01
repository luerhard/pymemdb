from collections import defaultdict
from collections.abc import Iterable
from pymemdb.column import Column
from pymemdb.errors import RowDoesNotExist


class Table:

    def __init__(self, name=None, primary_id="pk"):
        self.name = name
        self.pk_id = primary_id
        self._columns = defaultdict(Column)
        self.pk = 0
        self.keys = set()

    def all(self, ordered=False):
        if ordered is False:
            for i in self.keys:
                yield self._get_row(i)
        elif ordered == "ascending":
            for i in sorted(self.keys):
                yield self._get_row(i)
        elif ordered == "descending":
            for i in sorted(self.keys, reverse=True):
                yield self._get_row(i)
        else:
            raise ValueError("Value for kwarg 'ordered' not in [False, "
                             "ascending, descending] !")

    def create_column(self, name, default=None, unique=False):
        self._columns[name] = Column(default=default, unique=unique)

    @property
    def columns(self):
        return list(self._columns)

    def drop(self):
        del self

    def insert(self, row):
        self.pk += 1
        self.keys.add(self.pk)
        for key, val in row.items():
            self._columns[key].insert(self.pk, val)
        return self.pk

    def insert_ignore(self, row, keys):
        results = self.find(**{key: row[key] for key in keys})
        try:
            next(results)
        except StopIteration:
            return self.insert(row)
        return None

    def find(self, **kwargs):
        results = self._find_rows(**kwargs)
        if not results:
            return None
        for p in results:
            yield {self.pk_id: p, **self._get_row(p)}

    def delete(self, ignore_errors=False, **kwargs):
        pks = {row[self.pk_id] for row in self.find(**kwargs)}
        try:
            for pk in pks:
                self.keys.remove(pk)
        except KeyError as e:
            if ignore_errors:
                pass
            else:
                raise e
        for col in self._columns.values():
            for pk in pks:
                try:
                    col.drop(pk)
                except KeyError:
                    pass

    def update(self, where, **kwargs):
        pks = self._find_rows(**where)
        if not pks:
            return None

        for col, val in kwargs.items():
            cell_dict = self._columns[col].cells
            val_dict = self._columns[col].values
            to_remove = defaultdict(set)
            for pk in pks:
                prev_val = self._columns[col].find_value(pk)
                cell_dict[pk] = val
                val_dict[val].add(pk)
                to_remove[prev_val].add(pk)

    def _get_row(self, pk):
        if pk not in self.keys:
            raise RowDoesNotExist(f"Row with {self.pk_id}={pk} does not exist")
        row = {col: self._columns[col].find_value(pk) for col in self.columns}
        row = {self.pk_id: pk, **row}
        return row

    def _find(self, col, val):
        if col not in self._columns:
            self.create_column(col)
        if isinstance(val, Iterable) and not isinstance(val, str):
            results = set()
            for v in val:
                results.update(self._columns[col].find(v))
            return results
        return self._columns[col].find(val)

    def _find_rows(self, **kwargs):
        results = None
        for col, val in kwargs.items():
            if results is None:
                results = self._find(col, val)
                if val == self._columns[col].default:
                    results.update(self.keys.symmetric_difference(
                        set(self._columns[col].cells)))
            else:
                pk = self._find(col, val)
                results.intersection_update(pk)
                if val == self._columns[col].default:
                    results.update(self.keys.symmetric_difference(
                        set(self._columns[col].cells)))
            if not results:
                return None
        return results

    def __eq__(self, other):
        return self.name == other.name

    def __getitem__(self, col):
        if col not in self._columns:
            raise KeyError
        return self._columns[col]

    def __delitem__(self, col):
       if col not in self._columns:
            raise KeyError
       del self._columns[col]

    def __hash__(self):
        return hash(self.name)

    def __len__(self):
        return len(self.keys)
