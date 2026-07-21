"""
Script d'ingestion des données en temps réel pour l'analyse de mobilité.

Fonctionnalités principales :
- Récupération des données vélo en temps réel des stations de vélo.
- Récupération des données des communes françaises via l'API geo.gouv.fr.
- Stockage des données dans PostgreSQL via je format JSONB.
- Exécution de la commande `dbt run` pour transformer les données.
"""

import logging
from enum import StrEnum

import requests
from dbt.cli.main import dbtRunner

from db import db

logger = logging.getLogger(__name__)


class Url(StrEnum):
    BORDEAUX = "https://datahub.bordeaux-metropole.fr/api/explore/v2.1/catalog/datasets/ci_vcub_p/exports/json?lang=fr&timezone=Europe%2FBerlin"
    COMMUNES = "https://geo.api.gouv.fr/communes"
    MARSEILLE = "https://data.ampmetropole.fr/api/explore/v2.1/catalog/datasets/gbfs-extract-station-information/exports/json?lang=fr&timezone=Europe%2FBerlin"
    NANTES = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    PARIS = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    STRASBOURG = "https://opendata.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json?lang=fr&timezone=Europe%2FBerlin"
    TOULOUSE = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"


def fetch_and_store_data(url: str, label: str) -> None:
    data_to_store = "[]"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        if response.text.strip():
            data_to_store = response.text
            logger.info(f"✅ Données {label} récupérées avec succès")
        else:
            logger.warning(f"⚠️ {label} a renvoyé un contenu vide")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur réseau ou HTTP pour {label}: {e}")
    except Exception as e:
        logger.error(f"❌ Erreur imprévue pour {label}: {e}")
    finally:
        db.store_json(f"{label}.json", data_to_store)
        if data_to_store == "[]":
            logger.warning(f"ℹ️ Fichier vide créé pour {label}")


def pipeline() -> None:
    """
    Récupère les données en temps réel des stations de vélo et des communes françaises.
    Si une source échoue, crée un fichier JSON vide ([]) pour éviter un crash dbt.
    Exécute la commande `dbt run`.
    """
    for url in Url:
        label = url.name.lower()
        fetch_and_store_data(url, label)

    logger.info("🚀 Démarrage de la commande dbt run")

    dbt = dbtRunner()
    cli_args = [
        "run",
        "--project-dir",
        "dbt-transformation",
        "--profiles-dir",
        "dbt-transformation",
    ]
    dbt.invoke(cli_args)
