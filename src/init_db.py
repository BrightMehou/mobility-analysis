"""
Script d'initialisation des tables de base pour l'analyse de mobilité.

Fonctionnalités principales
- Création des tables de staging dans PostgreSQL.
"""

import logging

from db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Initialisation de la base de données PostgreSQL.")
    init_db()
