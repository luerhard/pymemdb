from typing import Hashable, Set

from collections import defaultdict
from .errors import UniqueConstraintError


class Column:

    def __init__(self, default: Hashable = None, unique: bool = False):
        self.cells: dict = dict()
        self.values: defaultdict = defaultdict(set)
        self.default = default
        self.unique = unique

    def insert(self, pk: int, val: Hashable) -> None:
        if self.unique and val in self.values:
            raise UniqueConstraintError(f"{val} already present in column "
                                        f"(row {self.values[val]})")
        self.cells[pk] = val
        self.values[val].add(pk)

    def drop(self, pk: int) -> None:
        if pk in self.cells:
            val = self.cells[pk]
            del self.cells[pk]
            self.values[val].remove(pk)
            if not self.values[val]:
                del self.values[val]

    def find(self, val: Hashable) -> Set:
        return self.values.get(val, set())

    def find_value(self, pk: int) -> Hashable:
        return self.cells.get(pk, self.default)

    def __len__(self):
        return len(self.cells)
