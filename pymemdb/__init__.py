"""A very fast in-memory database with export to sqlite written purely in python"""

from .errors import *
from .column import Column
from .database import Database
from .table import Table

__version__ = "1.1.0"
