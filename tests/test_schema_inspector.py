from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from canopy.core.context.schema_inspector import SchemaInspector


class TestSchemaInspector:
    def test_inspect_existing_table(self, db_engine: Engine):
        meta = sa.MetaData()
        sa.Table(
            "users",
            meta,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.String(100), nullable=False),
            sa.Column("email", sa.String(255)),
            sa.Column("active", sa.Boolean, default=True),
        )
        meta.create_all(db_engine)

        inspector = SchemaInspector(db_engine)
        schema = inspector.inspect("users")

        assert schema is not None
        assert schema.table_name == "users"
        assert len(schema.columns) == 4

        id_col = next(c for c in schema.columns if c.name == "id")
        assert id_col.primary_key is True

        name_col = next(c for c in schema.columns if c.name == "name")
        assert name_col.nullable is False

    def test_inspect_nonexistent_table(self, db_engine: Engine):
        inspector = SchemaInspector(db_engine)
        result = inspector.inspect("nonexistent_table")
        assert result is None

    def test_inspect_returns_all_columns(self, db_engine: Engine):
        meta = sa.MetaData()
        sa.Table(
            "products",
            meta,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.Text),
            sa.Column("price", sa.Numeric(10, 2)),
            sa.Column("created_at", sa.DateTime),
        )
        meta.create_all(db_engine)

        inspector = SchemaInspector(db_engine)
        schema = inspector.inspect("products")

        assert schema is not None
        col_names = [c.name for c in schema.columns]
        assert col_names == ["id", "name", "price", "created_at"]
