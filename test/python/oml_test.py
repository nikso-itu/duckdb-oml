import duckdb
import os
import pytest

# Get a fresh connection to DuckDB with the oml extension binary loaded
@pytest.fixture
def duckdb_conn():
    extension_binary = os.getenv('OML_EXTENSION_BINARY_PATH')
    if (extension_binary == ''):
        raise Exception('Please make sure the `OML_EXTENSION_BINARY_PATHH` is set to run the python tests')
    conn = duckdb.connect('', config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"load '{extension_binary}'")
    return conn

def test_oml(duckdb_conn):
    duckdb_conn.execute("SELECT * from OmlGen('data/oml_testing/st_lrwan1_11.oml');");
    res = duckdb_conn.fetchall()
    assert(res[0][0] == 67725);