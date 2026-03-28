from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from canopy.models.schema import ColumnSchema, TargetSchema


class SchemaInspector:
    """Introspects an existing database table and returns a TargetSchema."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def inspect(self, table_name: str, schema: str | None = None) -> TargetSchema | None:
        """Reflect the table. Returns None if the table doesn't exist."""
        inspector = sa.inspect(self.engine)

        if not inspector.has_table(table_name, schema=schema):
            return None

        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
        pk_columns = set(pk_constraint.get("constrained_columns", []))

        columns: list[ColumnSchema] = []
        for col in inspector.get_columns(table_name, schema=schema):
            columns.append(
                ColumnSchema(
                    name=col["name"],
                    type=str(col["type"]),
                    nullable=col.get("nullable", True),
                    primary_key=col["name"] in pk_columns,
                    default=str(col["default"]) if col.get("default") else None,
                )
            )

        return TargetSchema(table_name=table_name, columns=columns)
