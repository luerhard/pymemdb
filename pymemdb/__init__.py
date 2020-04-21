"""A very fast in-memory database with export to sqlite written purely in python"""

from .errors import *
from .column import Column
from .table import Table
from .database import Database


__version__ = "1.3.0"
