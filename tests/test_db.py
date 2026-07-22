import pandas as pd
import pytest

from src.db import DatabaseClient


@pytest.fixture
def mock_engine(mocker):
    """Faux moteur de base de données."""
    return mocker.MagicMock()


@pytest.fixture
def db_client(mock_engine):
    """Client prêt à être testé."""
    return DatabaseClient(mock_engine)


def test_init_db(db_client, mock_engine, mocker):
    # Préparation
    mock_conn = mocker.MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Action
    db_client.init_db()

    # Vérification simple : la méthode execute a-t-elle été appelée au moins une fois ?
    assert mock_conn.execute.called


def test_query_to_df_success(db_client, mocker):
    # Préparation : on force pandas à renvoyer un DataFrame basique
    mock_read_sql = mocker.patch("src.db.pd.read_sql_query")
    mock_read_sql.return_value = pd.DataFrame({"A": [1]})

    # Action
    df = db_client.query_to_df("SELECT * FROM table")

    # Vérification : on a bien récupéré notre DataFrame, il n'est pas vide
    assert not df.empty
    assert "A" in df.columns


def test_query_to_df_exception(db_client, mocker):
    # Préparation : on force pandas à simuler une erreur
    mock_read_sql = mocker.patch("src.db.pd.read_sql_query")
    mock_read_sql.side_effect = Exception("Erreur base de données")

    # Action
    df = db_client.query_to_df("SELECT * FROM mauvaise_table")

    # Vérification : le DataFrame doit être vide grâce au bloc try/except
    assert df.empty


def test_store_json(db_client, mock_engine, mocker):
    # Préparation
    mock_conn = mocker.MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Action
    db_client.store_json("fichier1", '{"donnee": 123}')

    # Vérification simple : la méthode execute a-t-elle été appelée ?
    assert mock_conn.execute.called
