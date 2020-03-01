class TableAlreadyExists(Exception):
    pass


class ColumnDoesNotExist(Exception):
    pass


class RowDoesNotExist(Exception):
    pass


class UniqueConstraintError(Exception):
    pass
