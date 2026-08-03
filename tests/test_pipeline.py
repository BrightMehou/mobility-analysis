from unittest.mock import MagicMock

import pytest
import requests

from src.pipeline import Url, fetch_and_store_data, pipeline


@pytest.fixture
def mock_db_client(mocker):
    """Faux client de base de données injecté dans le pipeline."""
    return mocker.MagicMock()


def test_fetch_and_store_data_success(mock_db_client, mocker):
    mock_get = mocker.patch("src.pipeline.requests.get")
    mock_response = MagicMock()
    mock_response.text = '{"station": "Bordeaux", "velos": 15}'
    mock_get.return_value = mock_response

    fetch_and_store_data("http://fake.url", "bordeaux", mock_db_client)

    mock_get.assert_called_once_with("http://fake.url", timeout=30)
    mock_response.raise_for_status.assert_called_once()
    mock_db_client.store_json.assert_called_once_with(
        "bordeaux.json", '{"station": "Bordeaux", "velos": 15}'
    )


def test_fetch_and_store_data_empty_content(mock_db_client, mocker):
    mock_get = mocker.patch("src.pipeline.requests.get")
    mock_response = MagicMock()
    mock_response.text = "   "
    mock_get.return_value = mock_response

    fetch_and_store_data("http://fake.url", "nantes", mock_db_client)

    mock_db_client.store_json.assert_called_once_with("nantes.json", "[]")


def test_fetch_and_store_data_http_error(mock_db_client, mocker):
    mock_get = mocker.patch("src.pipeline.requests.get")
    mock_get.side_effect = requests.exceptions.Timeout("Délai d'attente dépassé")

    fetch_and_store_data("http://fake.url", "paris", mock_db_client)

    mock_db_client.store_json.assert_called_once_with("paris.json", "[]")


def test_pipeline_execution(mock_db_client, mocker):
    mock_fetch = mocker.patch("src.pipeline.fetch_and_store_data")
    mock_dbt_runner_class = mocker.patch("src.pipeline.dbtRunner")
    mock_dbt_instance = MagicMock()
    mock_dbt_runner_class.return_value = mock_dbt_instance

    pipeline(mock_db_client)

    # Vérification
    assert mock_fetch.call_count == len(Url)
    mock_dbt_instance.invoke.assert_called_once_with(
        [
            "run",
            "--project-dir",
            "dbt-transformation",
            "--profiles-dir",
            "dbt-transformation",
        ]
    )
