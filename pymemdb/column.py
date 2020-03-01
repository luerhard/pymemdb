from collections import defaultdict
from .errors import UniqueConstraintError


class Column:

    def __init__(self, default=None, unique=False):
        self.cells = dict()
        self.values = defaultdict(set)
        self.default = default
        self.unique = unique

    def insert(self, pk, val):
        if self.unique and val in self.values:
            raise UniqueConstraintError(f"{val} already present in column (row {self.values[val]})")
        self.cells[pk] = val
        self.values[val].add(pk)

    def drop(self, pk):
        val = self.cells[pk]
        del self.cells[pk]
        self.values[val].remove(pk)
        if not self.values[val]:
            del self.values[val]

    def find(self, val):
        return self.values[val]

    def find_value(self, pk):
        return self.cells.get(pk, self.default)

    def __len__(self):
        return len(self.cells)
