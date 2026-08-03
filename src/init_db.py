"""
Script d'initialisation des tables de base pour l'analyse de mobilité.

Fonctionnalités principales
- Création des tables de staging dans PostgreSQL.
"""

import logging

from db import create_database_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    db = create_database_client()
    logger.info("Initialisation de la base de données PostgreSQL.")
    db.init_db()
