"""
Script d'initialisation des tables de base pour l'analyse de mobilité.

Fonctionnalités principales
- Création des tables de staging dans PostgreSQL.
"""

import logging

from utils import exec_sql_from_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Initialisation de la base de données PostgreSQL.")

    exec_sql_from_file("create_staging_tables.sql", "Création des tables de staging")
