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


class CityUrl(StrEnum):
    PARIS = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    NANTES = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    TOULOUSE = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"
    STRASBOURG = "https://opendata.strasbourg.eu/api/explore/v2.1/catalog/datasets/stations-velhop/exports/json?lang=fr&timezone=Europe%2FBerlin"


URL_COMMUNES: str = "https://geo.api.gouv.fr/communes"


def get_realtime_bicycle_data() -> None:
    """
    Récupère les données en temps réel des stations de vélo.
    Si une ville échoue, crée un fichier JSON vide ([]) pour éviter un crash dbt.
    """
    for url in CityUrl:
        file_name = f"{url.name.lower()}_realtime_bicycle_data.json"
        data_to_store = "[]"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            if response.text.strip():
                data_to_store = response.text
                logger.info(f"✅ Données {url.name} récupérées avec succès")        
            else:
                logger.warning(f"⚠️ {url.name} a renvoyé un contenu vide")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur réseau ou HTTP pour {url.name}: {e}")
        except Exception as e:
            logger.error(f"❌ Erreur imprévue pour {url.name}: {e}")
        finally:
            store_json(file_name, data_to_store)
            if data_to_store == "[]":
                logger.warning(f"ℹ️ Fichier vide créé pour {url.name}")


def get_commune_data() -> None:
    """Récupère les données des communes françaises et les stocke dans PostgreSQL."""
    data_to_store = "[]"
    try:
        response = requests.get(URL_COMMUNES, timeout=30)
        response.raise_for_status()

        if response.text.strip():
            data_to_store = response.text
            logger.info("✅ Données communes récupérées avec succès")
        else:
            logger.warning("⚠️ API communes indisponible (contenu vide)")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur réseau ou HTTP pour {URL_COMMUNES}: {e}")
    except Exception as e:
        logger.error(f"❌ Erreur imprévue pour {URL_COMMUNES}: {e}")
    finally:
        store_json("commune_data.json", data_to_store)
        if data_to_store == "[]":
            logger.warning(f"⚠️ Fichier vide créé pour {URL_COMMUNES}")


def data_ingestion() -> None:
    """
    Ingérer les données en temps réel des vélos et des communes.
    """
    get_realtime_bicycle_data()
    get_commune_data()
