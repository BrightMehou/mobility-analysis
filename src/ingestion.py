"""
Script d'ingestion des données en temps réel pour l'analyse de mobilité.

Fonctionnalités principales :
- Récupération des données vélo en temps réel des stations de vélo.
- Récupération des données des communes françaises via l'API geo.gouv.fr.
- Stockage des données dans PostgreSQL via je format JSONB.
"""

import logging
from enum import StrEnum

import requests

from utils import store_json

logger = logging.getLogger(__name__)


class Url(StrEnum):
    PARIS = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    NANTES = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    TOULOUSE = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"
    STRASBOURG = "https://opendata.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json?lang=fr&timezone=Europe%2FBerlin"
    COMMUNES = "https://geo.api.gouv.fr/communes"


def fetch_and_store_data(url: str, file_name: str, label: str) -> None:
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
        store_json(file_name, data_to_store)
        if data_to_store == "[]":
            logger.warning(f"ℹ️ Fichier vide créé pour {label}")


def data_ingestion() -> None:
    """
    Récupère les données en temps réel des stations de vélo et des communes françaises.
    Si une source échoue, crée un fichier JSON vide ([]) pour éviter un crash dbt.
    """
    for url in Url:
        label = url.name.lower()
        file_name = f"{label}.json"
        fetch_and_store_data(url, file_name, label)
