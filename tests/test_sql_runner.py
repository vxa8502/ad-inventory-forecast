"""Tests for SQL runner utilities."""

import pytest

from src.sql_runner import load_sql, render_sql


class TestRenderSql:
    """Tests for SQL placeholder substitution."""

    def test_single_placeholder(self):
        sql = "SELECT * FROM `{project_id}.dataset.table`"
        result = render_sql(sql, project_id="my-project")
        assert result == "SELECT * FROM `my-project.dataset.table`"

    def test_multiple_placeholders(self):
        sql = "SELECT * FROM `{project_id}.{dataset}.{table}`"
        result = render_sql(sql, project_id="proj", dataset="data", table="tbl")
        assert result == "SELECT * FROM `proj.data.tbl`"

    def test_repeated_placeholder(self):
        sql = "{val} AND {val}"
        result = render_sql(sql, val="test")
        assert result == "test AND test"

    def test_date_placeholders(self):
        sql = "WHERE date BETWEEN '{date_start}' AND '{date_end}'"
        result = render_sql(sql, date_start="2022-01-01", date_end="2023-12-31")
        assert result == "WHERE date BETWEEN '2022-01-01' AND '2023-12-31'"

    def test_no_placeholders(self):
        sql = "SELECT 1"
        result = render_sql(sql)
        assert result == "SELECT 1"


class TestLoadSql:
    """Tests for SQL file loading."""

    def test_load_existing_file(self, tmp_path):
        sql_content = "SELECT * FROM test_table"
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(sql_content)

        result = load_sql(sql_file)
        assert result == sql_content

    def test_load_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            load_sql("/nonexistent/path/to/file.sql")

    def test_load_preserves_whitespace(self, tmp_path):
        sql_content = "SELECT *\nFROM table\nWHERE x = 1"
        sql_file = tmp_path / "multiline.sql"
        sql_file.write_text(sql_content)

        result = load_sql(sql_file)
        assert result == sql_content
