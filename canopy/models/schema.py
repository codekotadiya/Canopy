from __future__ import annotations

from pydantic import BaseModel


class ColumnSchema(BaseModel):
    name: str
    type: str  # SQL type string, e.g. "VARCHAR(255)", "INTEGER", "TIMESTAMP"
    nullable: bool = True
    primary_key: bool = False
    default: str | None = None


class TargetSchema(BaseModel):
    table_name: str
    columns: list[ColumnSchema]
