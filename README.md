## 🚴 MOBILITY ANALYSIS

Ce projet collecte des données en temps réel sur les stations de vélos en libre-service de plusieurs villes françaises, puis les transforme avec dbt et les expose via un tableau de bord Streamlit.

L’application complète comprend :
- une ingestion de données depuis des API publiques,
- un stockage dans PostgreSQL,
- des modèles dbt pour la transformation et l’agrégation,
- une interface Streamlit pour explorer les métriques par ville, département et station.

---

## 📥 **Sources des Données**

- [API Paris](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/api/)  
- [API Nantes](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)  
- [API Toulouse](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)  
- [API Strasbourg](https://data.strasbourg.eu/explore/dataset/stations-velhop/api/)  
- [API Bordeaux](https://datahub.bordeaux-metropole.fr/explore/dataset/ci_vcub_p/information/)
- [API Marseille](https://data.ampmetropole.fr/explore/dataset/gbfs-extract-station-information/information/)
- [API Open Data Communes](https://geo.api.gouv.fr/communes)  

---

## 🗂️ Structure du projet

```plaintext
├── dbt-transformation/        # Projet dbt : staging, consolidate, analytics
├── src/                       # Code Python principal
│   ├── db.py                  # Connexion PostgreSQL et helpers DB
│   ├── init_db.py             # Initialisation des tables de base
│   ├── pipeline.py            # Ingestion des données et lancement de dbt run
│   └── ui.py                  # Tableau de bord Streamlit
├── docker-compose.yml         # Orchestration des services PostgreSQL et app
├── Dockerfile                 # Image applicative Python + dbt + Streamlit
├── pyproject.toml             # Dépendances du projet
├── README.md                  # Documentation
└── uv.lock                    # Verrouillage des dépendances
```

---

## ⚙️ Fonctionnement du pipeline

1. Ingestion des données
   - Le script [src/pipeline.py](src/pipeline.py) interroge les API publiques.
   - Les réponses brutes sont stockées dans la table PostgreSQL `staging_raw` sous forme de JSON.

2. Transformation dbt
   - Le projet dbt dans [dbt-transformation](dbt-transformation) crée des modèles de type staging, consolidate et analytics.
   - Les vues analytiques servent ensuite de base au tableau de bord.

3. Visualisation
   - L’interface Streamlit dans [src/ui.py](src/ui.py) permet de consulter :
     - des indicateurs globaux,
     - des métriques par ville,
     - des métriques par département,
     - une carte interactive des stations.

---

## 🚀 Démarrage rapide avec Docker

Prérequis : Docker et Docker Compose installés.

### Lancer l’application

```bash
docker compose up -d --build
```

### Accéder aux services

- Tableau de bord Streamlit : http://localhost:8501
- Documentation dbt : http://localhost:8080

### Arrêter l’application

```bash
docker compose down
```
