"""CLI entry point for the text-to-SQL app."""

import argparse
import os
import sys

import anthropic

from prompt_builder import SYSTEM_PROMPT, build_messages
from schema_loader import load_schema
from sql_executor import QueryError, execute_query, format_results


def generate_sql(client: anthropic.Anthropic, schema: str, question: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=build_messages(schema, question),
    )
    return response.content[0].text.strip()


def run_interactive(db_path: str, client: anthropic.Anthropic) -> None:
    print(f"Loading schema from '{db_path}'...")
    schema = load_schema(db_path)
    print("Schema loaded. Type your question (or 'quit' to exit).\n")

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
        sql = generate_sql(client, schema, question)
        print(f"\nSQL:\n{sql}\n")

        try:
            columns, rows = execute_query(db_path, sql)
            print(format_results(columns, rows))
        except QueryError as e:
            print(f"Execution error: {e}")
        print()


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
    parser.add_argument("--db", default="demo.db", help="Path to SQLite database (default: demo.db)")
    parser.add_argument("--seed", action="store_true", help="Seed a demo database and exit")
    parser.add_argument("--question", "-q", help="Run a single question non-interactively")
    args = parser.parse_args()

    if args.seed:
        seed_demo_db(args.db)
        return

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(args.db):
        print(f"Database '{args.db}' not found. Run with --seed to create a demo database.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    if args.question:
        schema = load_schema(args.db)
        sql = generate_sql(client, schema, args.question)
        print(f"SQL:\n{sql}\n")
        try:
            columns, rows = execute_query(args.db, sql)
            print(format_results(columns, rows))
        except QueryError as e:
            print(f"Execution error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        run_interactive(args.db, client)


if __name__ == "__main__":
    main()
