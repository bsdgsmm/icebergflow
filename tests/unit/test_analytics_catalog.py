from pathlib import Path

import pytest

from icebergflow.analytics.queries import get_query, list_queries

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def test_every_query_states_its_business_question():
    queries = list_queries(SQL_DIR)
    assert len(queries) >= 8
    for query in queries:
        assert query.question.endswith("?"), query.name
        assert "SELECT" in query.sql.upper()


def test_unknown_query_lists_alternatives():
    with pytest.raises(KeyError, match="revenue_trend"):
        get_query(SQL_DIR, "does_not_exist")


def test_query_without_question_header_is_rejected(tmp_path):
    (tmp_path / "analytics").mkdir()
    (tmp_path / "analytics" / "bad.sql").write_text("SELECT 1\n")
    with pytest.raises(ValueError, match="Question"):
        list_queries(tmp_path)
