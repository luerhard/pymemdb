from collections import defaultdict
from collections.abc import Iterable
from pymemdb import Column, ColumnDoesNotExist


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

    def find(self, ignore_errors=False, **kwargs):
        results = self._find_rows(**kwargs, ignore_errors=ignore_errors)
        if not results:
            return None
        for p in results:
            yield {self.pk_id: p, **self._get_row(p)}

    def delete(self, ignore_errors=False, **kwargs):
        pks = {row[self.pk_id] for row in self.find(**kwargs)}
        if len(pks) == 0 and not ignore_errors:
            raise KeyError(f"No matching rows found for {kwargs}")
        for pk in pks:
            self.keys.remove(pk)
        for col in self._columns.values():
            for pk in pks:
                col.drop(pk)
        return len(pks)

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
        row = {col: self._columns[col].find_value(pk) for col in self.columns}
        row = {self.pk_id: pk, **row}
        return row

    def _find(self, col, val):
        if isinstance(val, Iterable) and not isinstance(val, str):
            results = set()
            for v in val:
                results.update(self._columns[col].find(v))
            return results
        return self._columns[col].find(val)

    def _find_rows(self, ignore_errors=True, **kwargs):
        results = None
        for col, val in kwargs.items():
            if col not in self._columns:
                if ignore_errors:
                    continue
                else:
                    raise KeyError(f"Column {col} not in Table!")
            if results is None:
                results = self._find(col, val)
                if val == self._columns[col].default:
                    column_cells = set(self._columns[col].cells)
                    results.update(self.keys.symmetric_difference(column_cells))
            else:
                pk = self._find(col, val)
                results.intersection_update(pk)
                print(f"ON COL {col}")
                if val == self._columns[col].default:
                    column_cells = set(self._columns[col].cells)
                    results.update(self.keys.symmetric_difference(column_cells))
            if not results:
                return None
        return results

    def __eq__(self, other):
        return self.name == other.name

    def __getitem__(self, col):
        if col not in self._columns:
            raise ColumnDoesNotExist("Column {col} does not exist!")
        return self._columns[col]

    def __delitem__(self, col):
        if col not in self._columns:
            raise ColumnDoesNotExist(f"Column {col} does not exist!")
        del self._columns[col]

    def __len__(self):
        return len(self.keys)
