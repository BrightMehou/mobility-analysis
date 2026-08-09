from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from sqlalchemy.exc import SQLAlchemyError

from src.db import DB_URL, DatabaseClient, create_database_client


@pytest.fixture
def mock_engine() -> MagicMock:
    """Faux moteur de base de données."""
    return MagicMock()


@pytest.fixture
def db_client(mock_engine: MagicMock) -> DatabaseClient:
    """Client prêt à être testé."""
    return DatabaseClient(mock_engine)


def test_init_db(
    db_client: DatabaseClient, mock_engine: MagicMock, mocker: MockerFixture
) -> None:
    mock_conn = mocker.MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    db_client.init_db()

    assert mock_conn.execute.called


def test_query_to_df_success(db_client: DatabaseClient, mocker: MockerFixture) -> None:
    mock_read_sql = mocker.patch("src.db.pd.read_sql_query")
    mock_read_sql.return_value = pd.DataFrame({"A": [1]})

    df = db_client.query_to_df("SELECT * FROM table")

    assert not df.empty
    assert "A" in df.columns


def test_query_to_df_exception(
    db_client: DatabaseClient, mocker: MockerFixture
) -> None:
    mock_read_sql = mocker.patch("src.db.pd.read_sql_query")
    mock_read_sql.side_effect = SQLAlchemyError("Erreur base de données simulée")

    df = db_client.query_to_df("SELECT * FROM mauvaise_table")

    assert df.empty


def test_store_json(
    db_client: DatabaseClient, mock_engine: MagicMock, mocker: MockerFixture
) -> None:
    mock_conn = mocker.MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    db_client.store_json("fichier1", '{"donnee": 123}')

    assert mock_conn.execute.called


def test_create_database_client(mocker: MockerFixture) -> None:
    mock_create_engine = mocker.patch("src.db.create_engine")
    mock_engine = mocker.MagicMock()
    mock_create_engine.return_value = mock_engine

    client = create_database_client()

    mock_create_engine.assert_called_once_with(DB_URL, pool_pre_ping=True)
    assert isinstance(client, DatabaseClient)
    assert client._engine == mock_engine  # noqa: SLF001
