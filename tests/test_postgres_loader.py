from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from canopy.core.loader.postgres import PostgresLoader
from canopy.models.schema import ColumnSchema, TargetSchema


class TestPostgresLoader:
    def test_ensure_table_creates_table(self):
        loader = PostgresLoader("sqlite:///:memory:")
        schema = TargetSchema(
            table_name="test_table",
            columns=[
                ColumnSchema(name="id", type="INTEGER", nullable=False, primary_key=True),
                ColumnSchema(name="name", type="VARCHAR(100)"),
                ColumnSchema(name="score", type="NUMERIC(5,2)"),
                ColumnSchema(name="active", type="BOOLEAN"),
            ],
        )
        loader.ensure_table(schema)

        # Verify table exists
        result = loader.get_target_schema("test_table")
        assert result is not None
        assert len(result.columns) == 4
        loader.finalize()

    def test_ensure_table_idempotent(self):
        loader = PostgresLoader("sqlite:///:memory:")
        schema = TargetSchema(
            table_name="idem",
            columns=[ColumnSchema(name="id", type="INTEGER", primary_key=True)],
        )
        loader.ensure_table(schema)
        loader.ensure_table(schema)  # should not raise
        loader.finalize()

    def test_load_batch_inserts_rows(self):
        loader = PostgresLoader("sqlite:///:memory:")
        schema = TargetSchema(
            table_name="employees",
            columns=[
                ColumnSchema(name="id", type="INTEGER", primary_key=True),
                ColumnSchema(name="name", type="VARCHAR(100)"),
                ColumnSchema(name="salary", type="FLOAT"),
            ],
        )
        loader.ensure_table(schema)

        rows = [
            {"id": 1, "name": "Alice", "salary": 75000.0},
            {"id": 2, "name": "Bob", "salary": 62000.0},
        ]
        count = loader.load_batch("employees", rows)
        assert count == 2

        summary = loader.finalize()
        assert summary.rows_loaded == 2
        assert summary.rows_failed == 0

    def test_load_batch_empty_rows(self):
        loader = PostgresLoader("sqlite:///:memory:")
        count = loader.load_batch("whatever", [])
        assert count == 0
        loader.finalize()

    def test_get_target_schema_nonexistent(self):
        loader = PostgresLoader("sqlite:///:memory:")
        assert loader.get_target_schema("nope") is None
        loader.finalize()

    def test_finalize_returns_summary(self):
        loader = PostgresLoader("sqlite:///:memory:")
        schema = TargetSchema(
            table_name="t",
            columns=[ColumnSchema(name="id", type="INTEGER", primary_key=True)],
        )
        loader.ensure_table(schema)
        loader.load_batch("t", [{"id": 1}, {"id": 2}, {"id": 3}])

        summary = loader.finalize()
        assert summary.rows_loaded == 3
        assert summary.duration_seconds >= 0
