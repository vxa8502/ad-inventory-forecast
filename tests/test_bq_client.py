"""Tests for BigQuery client wrapper functions."""

from unittest.mock import MagicMock, mock_open, patch

import pytest


@pytest.fixture
def mock_settings():
    """Mock settings module with default values."""
    with patch("src.bq_client.settings") as mock:
        mock.PROJECT_ID = "test-project"
        mock.LOCATION = "US"
        mock.MAX_BYTES_BILLED = 250 * 1024**3
        yield mock


@pytest.fixture
def mock_bq_client_class():
    """Mock BigQuery Client class."""
    with patch("src.bq_client.bigquery.Client") as mock:
        yield mock


@pytest.fixture
def mock_query_job():
    """Create a mock query job with configurable bytes processed."""
    job = MagicMock()
    job.total_bytes_processed = 1024**3  # 1 GB default
    return job


class TestGetClient:
    """Tests for BigQuery client initialization."""

    def test_creates_client_with_project_settings(
        self, mock_settings, mock_bq_client_class
    ):
        from src.bq_client import get_client

        get_client()

        mock_bq_client_class.assert_called_once_with(
            project="test-project", location="US"
        )

    def test_returns_client_instance(self, mock_settings, mock_bq_client_class):
        mock_client_instance = MagicMock()
        mock_bq_client_class.return_value = mock_client_instance

        from src.bq_client import get_client

        result = get_client()

        assert result == mock_client_instance


class TestEstimateQueryCost:
    """Tests for query cost estimation."""

    @patch("src.bq_client.get_client")
    def test_returns_cost_estimate_dict(self, mock_get_client, mock_query_job):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.query.return_value = mock_query_job

        from src.bq_client import estimate_query_cost

        result = estimate_query_cost("SELECT * FROM table")

        assert "bytes_processed" in result
        assert "gb_processed" in result
        assert "estimated_cost_usd" in result

    @patch("src.bq_client.get_client")
    def test_calculates_gb_correctly(self, mock_get_client, mock_query_job):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_query_job.total_bytes_processed = 5 * 1024**3  # 5 GB
        mock_client.query.return_value = mock_query_job

        from src.bq_client import estimate_query_cost

        result = estimate_query_cost("SELECT 1")

        assert result["gb_processed"] == 5.0

    @patch("src.bq_client.get_client")
    def test_calculates_cost_at_5_per_tb(self, mock_get_client, mock_query_job):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_query_job.total_bytes_processed = 1024**4  # 1 TB
        mock_client.query.return_value = mock_query_job

        from src.bq_client import estimate_query_cost

        result = estimate_query_cost("SELECT 1")

        assert result["estimated_cost_usd"] == 5.0

    @patch("src.bq_client.get_client")
    def test_uses_dry_run_config(self, mock_get_client, mock_query_job):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_query_job.total_bytes_processed = 0
        mock_client.query.return_value = mock_query_job

        from src.bq_client import estimate_query_cost

        estimate_query_cost("SELECT 1")

        call_args = mock_client.query.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.dry_run is True
        assert job_config.use_query_cache is False


class TestRunQuery:
    """Tests for query execution."""

    @patch("src.bq_client.estimate_query_cost")
    def test_dry_run_returns_cost_estimate(self, mock_estimate):
        mock_estimate.return_value = {"gb_processed": 1.0, "estimated_cost_usd": 0.005}

        from src.bq_client import run_query

        result = run_query("SELECT 1", dry_run=True)

        assert result == {"gb_processed": 1.0, "estimated_cost_usd": 0.005}
        mock_estimate.assert_called_once_with("SELECT 1")

    @patch("src.bq_client.get_client")
    def test_execution_uses_max_bytes_billed(
        self, mock_get_client, mock_settings, mock_query_job
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_result = MagicMock()
        mock_result.total_rows = 10
        mock_query_job.result.return_value = mock_result
        mock_client.query.return_value = mock_query_job

        from src.bq_client import run_query

        run_query("SELECT 1", dry_run=False)

        call_args = mock_client.query.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.maximum_bytes_billed == 250 * 1024**3

    @patch("src.bq_client.get_client")
    def test_returns_row_iterator(
        self, mock_get_client, mock_settings, mock_query_job
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_result = MagicMock()
        mock_result.total_rows = 42
        mock_query_job.result.return_value = mock_result
        mock_client.query.return_value = mock_query_job

        from src.bq_client import run_query

        result = run_query("SELECT 1")

        assert result.total_rows == 42


class TestLoadCsvToTable:
    """Tests for CSV loading."""

    @pytest.fixture
    def mock_csv_file(self):
        """Mock open for CSV file reading."""
        with patch("builtins.open", mock_open(read_data=b"col1,col2\n1,2\n")) as mock:
            yield mock

    @pytest.fixture
    def mock_load_job(self):
        """Create a mock load job."""
        job = MagicMock()
        job.output_rows = 1
        return job

    @patch("src.bq_client.get_client")
    def test_loads_csv_with_correct_config(
        self, mock_get_client, mock_csv_file, mock_load_job
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.load_table_from_file.return_value = mock_load_job

        from src.bq_client import load_csv_to_table

        load_csv_to_table("/path/to/file.csv", "project.dataset.table")

        mock_csv_file.assert_called_once_with("/path/to/file.csv", "rb")
        mock_client.load_table_from_file.assert_called_once()

    @patch("src.bq_client.get_client")
    def test_waits_for_job_completion(
        self, mock_get_client, mock_csv_file, mock_load_job
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.load_table_from_file.return_value = mock_load_job

        from src.bq_client import load_csv_to_table

        load_csv_to_table("/path/to/file.csv", "project.dataset.table")

        mock_load_job.result.assert_called_once()

    @patch("src.bq_client.get_client")
    def test_returns_load_job(self, mock_get_client, mock_csv_file, mock_load_job):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_load_job.output_rows = 10
        mock_client.load_table_from_file.return_value = mock_load_job

        from src.bq_client import load_csv_to_table

        result = load_csv_to_table("/path/to/file.csv", "project.dataset.table")

        assert result == mock_load_job

    @patch("src.bq_client.get_client")
    def test_uses_write_truncate_disposition(
        self, mock_get_client, mock_csv_file, mock_load_job
    ):
        from google.cloud import bigquery

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.load_table_from_file.return_value = mock_load_job

        from src.bq_client import load_csv_to_table

        load_csv_to_table("/path/to/file.csv", "project.dataset.table")

        call_args = mock_client.load_table_from_file.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
