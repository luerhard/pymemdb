# pymemdb
[![CircleCI](https://circleci.com/gh/luerhard/pymemdb.svg?style=svg)](https://circleci.com/gh/luerhard/pymemdb) [![codecov](https://codecov.io/gh/luerhard/pymemdb/branch/master/graph/badge.svg)](https://codecov.io/gh/luerhard/pymemdb)

will soon be available with 
```
pip install pymemdb
```

# Description
Very simple RDMBS that is supposed to serve as a drop-in replacement for a conventional DB during build-up. It is very fast, completely written in python und relies heavily on dictionaries. It features a `to_sqlite` export method - more DBs will follow.

# Usage

## Insert into a table

```
from pymemdb import Table

table = Table()
row1 = dict(firstname="John", lastname="Smith")
row2 = dict(firstname="Jane", lastname="Smith")
row3 = dict(firstname="John", lastname="Doe")

for row in [row1, row2, row3]:
    table.insert(row)
```
## iterate over the entire table
```
print(list(table.all()))

[{'id': 0, 'firstname': 'John', 'lastname': 'Smith'},
 {'id': 1, 'firstname': 'Jane', 'lastname': 'Smith'},
 {'id': 2, 'firstname': 'John', 'lastname': 'Doe'}]
```
## update rows
```
table.update(where={"firstname": "Jane"}, firstname="Joanne")
print(list(table.all()))

[{'id': 0, 'firstname': 'John', 'lastname': 'Smith'},
 {'id': 1, 'firstname': 'Joanne', 'lastname': 'Smith'},
 {'id': 2, 'firstname': 'John', 'lastname': 'Doe'}]
```
## search for rows
```
print(list(table.find(firstname="John")))

[{'id': 0, 'firstname': 'John', 'lastname': 'Smith'},
{'id': 2, 'firstname': 'John', 'lastname': 'Doe'}]
```

## search for values in iterable
```
print(list(table.find(firstname=["John", "Joanne"])))

[{'id': 0, 'firstname': 'John', 'lastname': 'Smith'},
{'id': 1, 'firstname': 'Joanne', 'lastname': 'Smith'},
{'id': 2, 'firstname': 'John', 'lastname': 'Doe'}]

```
## delete rows
```
table.delete(firstname="John", lastname="Smith")
print(list(table.all()))

[{'id': 1, 'firstname': 'Joanne', 'lastname': 'Smith'},
 {'id': 2, 'firstname': 'John', 'lastname': 'Doe'}]
```