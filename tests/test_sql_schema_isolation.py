import re

import app.sql as sql_module


def _all_sql_text() -> str:
    sql_fragments: list[str] = []
    for value in vars(sql_module).values():
        if isinstance(value, str):
            sql_fragments.append(value)
        elif isinstance(value, list) and all(isinstance(item, str) for item in value):
            sql_fragments.extend(value)
    return "\n".join(sql_fragments)


def test_schema_statements_create_heroku_schema_first() -> None:
    assert sql_module.SCHEMA_STATEMENTS[0].strip() == "CREATE SCHEMA IF NOT EXISTS heroku"


def test_sql_references_heroku_schema_tables() -> None:
    all_sql = _all_sql_text()
    assert "heroku.jobs" in all_sql
    assert "heroku.job_invoices" in all_sql


def test_sql_has_no_unqualified_jobs_or_job_invoices_table_references() -> None:
    all_sql = _all_sql_text()
    disallowed_patterns = [
        r"\bFROM\s+jobs\b",
        r"\bFROM\s+job_invoices\b",
        r"\bUPDATE\s+jobs\b",
        r"\bUPDATE\s+job_invoices\b",
        r"\bINSERT\s+INTO\s+jobs\b",
        r"\bINSERT\s+INTO\s+job_invoices\b",
        r"\bALTER\s+TABLE\s+jobs\b",
        r"\bALTER\s+TABLE\s+job_invoices\b",
        r"\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+jobs\b",
        r"\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+job_invoices\b",
        r"\bREFERENCES\s+jobs\b",
    ]
    for pattern in disallowed_patterns:
        assert re.search(pattern, all_sql, flags=re.IGNORECASE) is None


def test_sql_has_no_schema_qualified_index_name_with_if_not_exists() -> None:
    all_sql = _all_sql_text()
    assert re.search(r"CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+heroku\.", all_sql, re.IGNORECASE) is None
    assert (
        re.search(r"CREATE\s+UNIQUE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+heroku\.", all_sql, re.IGNORECASE)
        is None
    )
