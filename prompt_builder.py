"""Builds the prompt sent to Claude for text-to-SQL generation."""

SYSTEM_PROMPT = """\
You are an expert SQL assistant. Given a database schema and a natural language question, \
produce a single valid SQLite query that answers the question.

Rules:
- Output ONLY the raw SQL query, no explanation, no markdown fences.
- Use only tables and columns that exist in the schema.
- Prefer readable aliases and JOINs over subqueries where practical.
- Never use DROP, DELETE, INSERT, UPDATE, or any DDL/DML other than SELECT.\
"""


def build_user_message(schema: str, question: str) -> str:
    return f"""Database schema:
{schema}

Question: {question}

SQL query:"""


def build_messages(schema: str, question: str) -> list[dict]:
    return [{"role": "user", "content": build_user_message(schema, question)}]
