"""
Script d'initialisation des tables de base pour l'analyse de mobilité.

Fonctionnalités principales
- Création des tables de staging dans PostgreSQL.
"""

import logging

from utils import exec_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Initialisation de la base de données PostgreSQL.")

    staging_table_query = """
    CREATE TABLE IF NOT EXISTS staging_raw (
        nom varchar PRIMARY KEY,
        data jsonb NOT NULL
    );
    """

    exec_sql(staging_table_query, "Création des tables de staging")
