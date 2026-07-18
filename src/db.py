"""Fonctions liées à la base de données PostgreSQL."""

import logging
import os

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DB_NAME: str = os.getenv("DB_NAME", "postgres")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: str = os.getenv("DB_PORT", "5432")

engine = create_engine(url=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def init_db() -> None:
    """Initialise la base de données PostgreSQL en créant les tables nécessaires."""
    staging_table_query = """
    CREATE TABLE IF NOT EXISTS staging_raw (
        nom varchar PRIMARY KEY,
        data jsonb NOT NULL
    );
    """
    with engine.begin() as connection:
        connection.execute(text(staging_table_query))
    logger.info("Tables de staging initialisées.")
