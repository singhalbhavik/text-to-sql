"""Loads database schema information from a SQLite database."""

from sqlalchemy import create_engine, inspect, text


def load_schema(db_path: str, sample_rows: int = 3) -> str:
    """Return a human-readable DDL-style schema with optional sample rows per table."""
    engine = create_engine(f"sqlite:///{db_path}")
    inspector = inspect(engine)
    sections = []

    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        fks = inspector.get_foreign_keys(table_name)

        col_defs = []
        for col in columns:
            col_type = str(col["type"])
            nullable = "" if col.get("nullable", True) else " NOT NULL"
            pk = " PRIMARY KEY" if col.get("primary_key") else ""
            col_defs.append(f"  {col['name']} {col_type}{pk}{nullable}")

        for fk in fks:
            ref_cols = ", ".join(fk["referred_columns"])
            local_cols = ", ".join(fk["constrained_columns"])
            col_defs.append(
                f"  FOREIGN KEY ({local_cols}) REFERENCES {fk['referred_table']}({ref_cols})"
            )

        table_def = f"CREATE TABLE {table_name} (\n" + ",\n".join(col_defs) + "\n);"

        if sample_rows > 0:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM {table_name} LIMIT :n"), {"n": sample_rows}
                )
                rows = [dict(row._mapping) for row in result]
            if rows:
                sample_lines = "\n".join(f"  {row}" for row in rows)
                table_def += f"\n/* sample rows:\n{sample_lines}\n*/"

        sections.append(table_def)

    engine.dispose()
    return "\n\n".join(sections)


def get_sample_rows(db_path: str, table_name: str, n: int = 3) -> list[dict]:
    """Return up to n sample rows from the given table."""
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT :n"), {"n": n})
        rows = [dict(row._mapping) for row in result]
    engine.dispose()
    return rows
