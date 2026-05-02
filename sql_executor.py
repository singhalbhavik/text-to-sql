"""Executes a SQL query against a SQLite database and returns results."""

import re

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class QueryError(Exception):
    pass


class SchemaError(QueryError):
    """Raised when the query references columns or tables not in the schema."""
    pass


def validate_sql(sql: str) -> None:
    """Raise QueryError if sql is not a SELECT statement."""
    cleaned = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    cleaned = re.sub(r"--[^\n]*", "", cleaned).strip().upper()
    if not cleaned.startswith("SELECT"):
        raise QueryError("Only SELECT queries are allowed.")


def execute_query(db_path: str, sql: str) -> tuple[list[str], list[tuple]]:
    """Run a SELECT query and return (column_names, rows). Raises QueryError on SQL errors."""
    validate_sql(sql)
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = list(result.keys())
            rows = result.fetchall()
            return columns, [tuple(row) for row in rows]
    except SQLAlchemyError as e:
        msg = str(e)
        m = re.search(r"no such (column|table): (\S+)", msg, re.IGNORECASE)
        if m:
            raise SchemaError(f"no {m.group(1)} '{m.group(2)}' in this database") from e
        raise QueryError(msg) from e
    finally:
        engine.dispose()


def format_results(columns: list[str], rows: list[tuple], max_rows: int = 50) -> str:
    if not rows:
        return "(no rows returned)"

    col_widths = [max(len(c), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(columns)]
    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header = "| " + " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(columns)) + " |"

    lines = [sep, header, sep]
    for row in rows[:max_rows]:
        lines.append("| " + " | ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)) + " |")
    lines.append(sep)

    if len(rows) > max_rows:
        lines.append(f"  ... and {len(rows) - max_rows} more rows")

    return "\n".join(lines)
