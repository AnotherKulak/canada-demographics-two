"""
Shared DuckDB connection helper.
All pipeline modules import get_conn() from here.
"""
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent / "warehouse" / "canada_demographics.duckdb"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a connection to the warehouse, initialising schema on first run."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    sql = SCHEMA_PATH.read_text()
    conn.execute(sql)
