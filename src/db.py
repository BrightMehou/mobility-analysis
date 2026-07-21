"""Accès à PostgreSQL pour l'ingestion et le dashboard."""

import logging
import os
from typing import Any, Mapping

import pandas as pd
from sqlalchemy import URL, Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

DB_URL = URL.create(
    drivername="postgresql",
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "postgres"),
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    database=os.getenv("DB_NAME", "postgres"),
)


class DatabaseClient:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def init_db(self) -> None:
        statement = text("""
            CREATE TABLE IF NOT EXISTS staging_raw (
                nom varchar PRIMARY KEY,
                data jsonb NOT NULL
            )
        """)
        with self._engine.begin() as connection:
            connection.execute(statement)

    def query_to_df(
        self,
        query: str,
        params: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        try:
            with self._engine.connect() as connection:
                return pd.read_sql_query(text(query), connection, params=params)
        except (SQLAlchemyError, pd.errors.DatabaseError, Exception) as exc:
            logger.warning("Erreur lors du chargement du DataFrame : %s", exc)
        return pd.DataFrame()

    def store_json(self, name: str, raw_json: str) -> None:
        """Envoie des données JSON dans la table staging_raw."""
        statement = text("""
            INSERT INTO staging_raw (nom, data)
            VALUES (:name, CAST(:data AS jsonb))
            ON CONFLICT (nom)
            DO UPDATE SET data = EXCLUDED.data
        """)
        with self._engine.begin() as connection:
            connection.execute(statement, {"name": name, "data": raw_json})


engine = create_engine(DB_URL)
db = DatabaseClient(engine)
