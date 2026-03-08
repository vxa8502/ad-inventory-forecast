"""Tests for data validation functions."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_bq_result():
    """Factory fixture to create mock BQ result rows with specified attributes."""
    def _create(**attrs):
        row = MagicMock()
        for key, value in attrs.items():
            setattr(row, key, value)
        return [row]
    return _create


class TestValidateRowCounts:
    """Tests for row count validation."""

    @patch("src.validators.bq_client.run_query")
    def test_passes_when_above_minimum(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(row_count=100)

        from src.validators import validate_row_counts

        result = validate_row_counts("project.dataset.table", expected_min=50)

        assert result["status"] == "PASS"
        assert result["actual"] == 100

    @patch("src.validators.bq_client.run_query")
    def test_fails_when_below_minimum(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(row_count=10)

        from src.validators import validate_row_counts

        result = validate_row_counts("project.dataset.table", expected_min=50)

        assert result["status"] == "FAIL"
        assert result["actual"] == 10


class TestValidateNoNulls:
    """Tests for null value validation."""

    @patch("src.validators.bq_client.run_query")
    def test_passes_when_no_nulls(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(null_count=0)

        from src.validators import validate_no_nulls

        result = validate_no_nulls("project.dataset.table", ["col1", "col2"])

        assert result["status"] == "PASS"
        assert result["null_count"] == 0

    @patch("src.validators.bq_client.run_query")
    def test_fails_when_nulls_found(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(null_count=5)

        from src.validators import validate_no_nulls

        result = validate_no_nulls("project.dataset.table", ["col1"])

        assert result["status"] == "FAIL"
        assert result["null_count"] == 5


class TestValidateDateContinuity:
    """Tests for date continuity validation."""

    @patch("src.validators.bq_client.run_query")
    def test_passes_when_no_gaps(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(gap_count=0)

        from src.validators import validate_date_continuity

        result = validate_date_continuity("project.dataset.table", "date", "ad_unit")

        assert result["status"] == "PASS"
        assert result["gap_count"] == 0

    @patch("src.validators.bq_client.run_query")
    def test_fails_when_gaps_found(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(gap_count=3)

        from src.validators import validate_date_continuity

        result = validate_date_continuity("project.dataset.table", "date", "ad_unit")

        assert result["status"] == "FAIL"
        assert result["gap_count"] == 3


class TestValidateHolidayJoin:
    """Tests for holiday join validation."""

    @patch("src.validators.bq_client.run_query")
    def test_passes_when_holidays_found(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(
            total_rows=100, holiday_rows=10, null_days_count=0, edge_case_violations=0
        )

        from src.validators import validate_holiday_join

        result = validate_holiday_join("project.dataset.table")

        assert result["status"] == "PASS"
        assert result["populated"] == 10

    @patch("src.validators.bq_client.run_query")
    def test_fails_when_no_holidays_found(self, mock_run, mock_bq_result):
        mock_run.return_value = mock_bq_result(
            total_rows=100, holiday_rows=0, null_days_count=0, edge_case_violations=0
        )

        from src.validators import validate_holiday_join

        result = validate_holiday_join("project.dataset.table")

        assert result["status"] == "FAIL"
        assert result["populated"] == 0
