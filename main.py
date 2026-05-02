"""CLI entry point for the text-to-SQL app."""

import argparse
import os
import re
import sys

import anthropic

from prompt_builder import build_messages, build_system_prompt
from schema_loader import load_schema
from sql_executor import QueryError, SchemaError, execute_query, format_results, validate_sql


def _call_claude(client: anthropic.Anthropic, system: str, messages: list[dict]) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=system,
        messages=messages,
    )
    return response.content[0].text.strip()


def generate_sql(
    client: anthropic.Anthropic,
    system: str,
    db_path: str,
    history: list[tuple[str, str]],
    question: str,
    max_retries: int = 2,
) -> tuple[str, list[str], list[tuple]]:
    """Generate SQL for question, auto-retrying up to max_retries times on failure."""
    messages = build_messages(history, question)

    for attempt in range(max_retries + 1):
        if attempt > 0:
            print(f"  Retrying (attempt {attempt + 1})...")

        sql = _call_claude(client, system, messages)

        try:
            validate_sql(sql)
            columns, rows = execute_query(db_path, sql)
            return sql, columns, rows
        except SchemaError:
            raise  # data doesn't exist in the schema — retrying won't help
        except QueryError as e:
            if attempt < max_retries:
                messages = messages + [
                    {"role": "assistant", "content": sql},
                    {"role": "user", "content": f"That query failed with error: {e}. Please provide a corrected SELECT query."},
                ]
            else:
                raise


def run_interactive(db_path: str, client: anthropic.Anthropic) -> None:
    print(f"Loading schema from '{db_path}'...")
    schema = load_schema(db_path)
    system = build_system_prompt(schema)
    print("Schema loaded. Type your question (or 'quit' to exit).\n")

    history: list[tuple[str, str]] = []

    while True:
        try:
            question = input("Question> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            print("Goodbye.")
            break

        print("Generating SQL...")
        try:
            sql, columns, rows = generate_sql(client, system, db_path, history, question)
            print(f"\nSQL:\n{sql}\n")
            print(format_results(columns, rows))
            history.append((question, sql))
        except QueryError as e:
            print(f"Error: {e}")
        print()


def import_csv(csv_path: str, db_path: str) -> None:
    """Import a CSV file into a SQLite database as a table named after the file."""
    import pandas as pd
    from sqlalchemy import create_engine

    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", os.path.splitext(os.path.basename(csv_path))[0])
    df = pd.read_csv(csv_path)
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    engine.dispose()
    print(f"Imported {len(df)} rows × {len(df.columns)} columns into table '{table_name}'.")


def seed_demo_db(db_path: str) -> None:
    """Create a small demo database for quick testing."""
    from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
    from sqlalchemy.orm import DeclarativeBase, Session

    class Base(DeclarativeBase):
        pass

    class Department(Base):
        __tablename__ = "departments"
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)

    class Employee(Base):
        __tablename__ = "employees"
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        role = Column(String)
        department_id = Column(Integer, ForeignKey("departments.id"))

    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        if session.query(Department).count() == 0:
            eng = Department(id=1, name="Engineering")
            mkt = Department(id=2, name="Marketing")
            session.add_all([eng, mkt])
            session.add_all([
                Employee(name="Alice", role="Engineer", department_id=1),
                Employee(name="Bob", role="Senior Engineer", department_id=1),
                Employee(name="Carol", role="Manager", department_id=2),
                Employee(name="Dave", role="Analyst", department_id=2),
            ])
            session.commit()
    engine.dispose()
    print(f"Demo database created at '{db_path}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Text-to-SQL CLI powered by Claude")
    parser.add_argument("--db", default=None, help="Path to SQLite database (default: demo.db, or <csv>.db with --csv)")
    parser.add_argument("--csv", help="Import a CSV file and query it")
    parser.add_argument("--seed", action="store_true", help="Seed a demo database and exit")
    parser.add_argument("--question", "-q", help="Run a single question non-interactively")
    args = parser.parse_args()

    if args.seed:
        seed_demo_db(args.db or "demo.db")
        return

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    if args.csv:
        if not os.path.exists(args.csv):
            print(f"Error: CSV file '{args.csv}' not found.", file=sys.stderr)
            sys.exit(1)
        db_path = args.db or os.path.splitext(args.csv)[0] + ".db"
        import_csv(args.csv, db_path)
    else:
        db_path = args.db or "demo.db"
        if not os.path.exists(db_path):
            print(f"Database '{db_path}' not found. Run with --seed to create a demo database.", file=sys.stderr)
            sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    if args.question:
        schema = load_schema(db_path)
        system = build_system_prompt(schema)
        try:
            sql, columns, rows = generate_sql(client, system, db_path, [], args.question)
            print(f"SQL:\n{sql}\n")
            print(format_results(columns, rows))
        except QueryError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        run_interactive(db_path, client)


if __name__ == "__main__":
    main()
