"""Fonctions réutilisables pour :
- Exécution de requêtes SQL sur PostgreSQL
- Envoi de fichiers JSON vers PostgreSQL
- Exécute les transformations ELT via `dbt run`.
"""

import logging
import os


from sqlalchemy import create_engine, text
from dbt.cli.main import dbtRunner, dbtRunnerResult

logger = logging.getLogger(__name__)

DB_NAME: str = os.getenv("DB_NAME", "postgres")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: str = os.getenv("DB_PORT", "5432")


def get_db_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, future=True)


def exec_sql(query: str, log_message: str) -> None:
    """
    Exécute une requête SQL passée directement en chaîne de caractères.

    Args:
        query (str): La requête SQL à exécuter.
        log_message (str): Message à afficher dans les logs après l'exécution.
    """

    engine = get_db_engine()
    with engine.begin() as connection:
        connection.execute(text(query))
    logger.info(log_message)


def store_json(name: str, raw_json: str) -> None:
    """
    Envoie les données JSON dans la table staging_raw de PostgreSQL.

    Args:
        name (str): Le nom du fichier source
        raw_json (str): Les données brutes en format JSON
    """

    engine = get_db_engine()
    query = text(
        """
        INSERT INTO staging_raw (nom, data)
        VALUES (:name, :data)
        ON CONFLICT (nom) DO UPDATE SET data = EXCLUDED.data
        """
    )
    with engine.begin() as connection:
        connection.execute(query, {"name": name, "data": raw_json})
    logger.info("Données JSON insérées dans la table staging_raw de PostgreSQL.")


def data_transformation() -> bool:
    """
    Exécute la commande `dbt run`.
    """

    logger.info("🚀 Démarrage de la commande dbt run")

    dbt = dbtRunner()
    cli_args = [
        "run",
        "--project-dir",
        "dbt-transformation",
        "--profiles-dir",
        "dbt-transformation",
    ]
    res: dbtRunnerResult = dbt.invoke(cli_args)
    return res.success
