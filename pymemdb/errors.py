class TableAlreadyExists(Exception):

    def __init__(self, tablename):
        self.tablename = tablename
        super(TableAlreadyExists, self).__init__()

    def __str__(self):
        return f"Table '{self.tablename}' already exists in the Database!"


class ColumnDoesNotExist(Exception):
    pass


class UniqueConstraintError(Exception):
    pass
