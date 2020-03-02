import pytest

from pymemdb import Table

@pytest.fixture(scope="session")
def big_table():
    t = Table(primary_id="normal")
    for i in range(100):
        t.insert({"normal": i, "squared": i**2, "cubed": i**3})
    return t