"""Recompute the headline indicators directly from Bronze with DuckDB."""
import math
from pathlib import Path

import duckdb

QUERY = Path(__file__).resolve().parents[1] / "analytics" / "compare_raw.sql"


def open_reference():
    return duckdb.connect(config={"threads": 2})


def raw_comparison(connection, source):
    cursor = connection.execute(QUERY.read_text(encoding="utf-8"), [str(source)])
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, record)) for record in cursor.fetchall()]


def verify(spark_rows, duckdb_rows):
    if [r["status"] for r in spark_rows] != [r["status"] for r in duckdb_rows]:
        raise ValueError("Catégories différentes entre Spark et DuckDB")
    for observed, expected in zip(spark_rows, duckdb_rows):
        for metric, value in expected.items():
            other = observed[metric]
            agrees = (
                isinstance(other, (float, int)) and math.isclose(other, value, rel_tol=1e-9)
                if isinstance(value, float) else other == value
            )
            if not agrees:
                raise ValueError(f"{expected['status']} / {metric} : Spark={other}, DuckDB={value}")
