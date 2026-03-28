from __future__ import annotations

import time
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from canopy.core.context.schema_inspector import SchemaInspector
from canopy.core.loader.base import BaseLoader
from canopy.models.execution import LoadSummary
from canopy.models.schema import ColumnSchema, TargetSchema

# Map SQL type strings from LLM proposals to SQLAlchemy types.
# Keys are uppercase for case-insensitive matching.
_TYPE_MAP: dict[str, type[sa.types.TypeEngine]] = {
    "VARCHAR": sa.String,
    "TEXT": sa.Text,
    "INTEGER": sa.Integer,
    "BIGINT": sa.BigInteger,
    "SMALLINT": sa.SmallInteger,
    "NUMERIC": sa.Numeric,
    "DECIMAL": sa.Numeric,
    "FLOAT": sa.Float,
    "DOUBLE": sa.Float,
    "REAL": sa.Float,
    "BOOLEAN": sa.Boolean,
    "DATE": sa.Date,
    "TIMESTAMP": sa.DateTime,
    "DATETIME": sa.DateTime,
    "JSON": sa.JSON,
    "SERIAL": sa.Integer,
}


def _resolve_sa_type(type_str: str) -> sa.types.TypeEngine:
    """Convert a SQL type string like 'VARCHAR(255)' to a SQLAlchemy type instance."""
    upper = type_str.upper().strip()

    # Extract base type and optional params: "VARCHAR(255)" -> ("VARCHAR", "255")
    base = upper.split("(")[0].strip()
    params_str = ""
    if "(" in upper and ")" in upper:
        params_str = upper[upper.index("(") + 1 : upper.index(")")]

    sa_type_cls = _TYPE_MAP.get(base)
    if sa_type_cls is None:
        return sa.Text()  # fallback

    # Handle parameterized types
    if base in ("VARCHAR", "CHAR") and params_str:
        try:
            return sa.String(int(params_str))
        except ValueError:
            return sa.String()

    if base in ("NUMERIC", "DECIMAL") and params_str:
        parts = [p.strip() for p in params_str.split(",")]
        try:
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            return sa.Numeric(precision=precision, scale=scale)
        except (ValueError, IndexError):
            return sa.Numeric()

    return sa_type_cls()


class PostgresLoader(BaseLoader):
    """Target loader for PostgreSQL (also works with SQLite via SQLAlchemy)."""

    def __init__(self, connection_string: str) -> None:
        self._engine: Engine = sa.create_engine(connection_string)
        self._metadata = sa.MetaData()
        self._inspector = SchemaInspector(self._engine)
        self._rows_loaded = 0
        self._rows_failed = 0
        self._start_time = time.monotonic()

    def get_target_schema(self, table_name: str) -> TargetSchema | None:
        return self._inspector.inspect(table_name)

    def ensure_table(self, schema: TargetSchema) -> None:
        if self._inspector.inspect(schema.table_name) is not None:
            return  # table already exists

        columns: list[sa.Column] = []
        for col in schema.columns:
            sa_type = _resolve_sa_type(col.type)
            columns.append(
                sa.Column(
                    col.name,
                    sa_type,
                    primary_key=col.primary_key,
                    nullable=col.nullable,
                    autoincrement=col.type.upper().startswith("SERIAL"),
                )
            )

        table = sa.Table(schema.table_name, self._metadata, *columns)
        table.create(self._engine, checkfirst=True)

    def load_batch(self, table_name: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        table = sa.Table(table_name, self._metadata, autoload_with=self._engine)
        with self._engine.begin() as conn:
            try:
                conn.execute(table.insert(), rows)
                self._rows_loaded += len(rows)
                return len(rows)
            except Exception:
                self._rows_failed += len(rows)
                raise

    def finalize(self) -> LoadSummary:
        duration = time.monotonic() - self._start_time
        self._engine.dispose()
        return LoadSummary(
            rows_loaded=self._rows_loaded,
            rows_failed=self._rows_failed,
            duration_seconds=round(duration, 2),
        )
