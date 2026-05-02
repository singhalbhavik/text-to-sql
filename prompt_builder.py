"""Builds the prompt sent to Claude for text-to-SQL generation."""

_SYSTEM_TEMPLATE = """\
You are an expert SQL assistant. Given a database schema and a natural language question, \
produce a single valid SQLite query that answers the question.

Rules:
- Output ONLY the raw SQL query, no explanation, no markdown fences.
- Use only tables and columns that exist in the schema.
- Prefer readable aliases and JOINs over subqueries where practical.
- Never use DROP, DELETE, INSERT, UPDATE, or any DDL/DML other than SELECT.

Database schema:
{schema}"""


def build_system_prompt(schema: str) -> str:
    return _SYSTEM_TEMPLATE.format(schema=schema)


def build_messages(history: list[tuple[str, str]], question: str) -> list[dict]:
    """Build messages list from conversation history and the new question.

    history is a list of (question, sql) pairs from prior successful turns.
    """
    messages = []
    for q, sql in history:
        messages.append({"role": "user", "content": q})
        messages.append({"role": "assistant", "content": sql})
    messages.append({"role": "user", "content": question})
    return messages
