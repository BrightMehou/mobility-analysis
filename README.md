## 🚴 ETL-Velo

Ce projet propose la mise en place d’un pipeline pour collecter, transformer et analyser les données des systèmes de vélos en libre-service de Paris, Nantes, Toulouse et Strasbourg.
Les données sont stockées dans Postgres, transformées à l’aide de DBT (Data Build Tool) pour assurer la qualité, la modularité et la traçabilité des modèles de données. Enfin, elles sont présentées via Streamlit pour faciliter l’exploration et la visualisation des résultats.

---

## 📥 **Sources des Données**

- [API Paris](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/api/)  
- [API Nantes](https://data.nantesmetropole.fr/explore/dataset/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/api/)  
- [API Toulouse](https://data.toulouse-metropole.fr/explore/dataset/api-velo-toulouse-temps-reel/api/)  
- [API Strasbourg](https://data.strasbourg.eu/explore/dataset/stations-velhop/api/)  
- [API Open Data Communes](https://geo.api.gouv.fr/communes)  

---

## 🗂️ **Structure du Projet**

```plaintext
├── dbt-transformation/            # Projet DBT pour la transformation des données
├── src/                       # Code source principal
│   ├── sql_statements/        # Requêtes SQL réutilisables
│   ├── ingestion.py           # Ingestion des données en temps réel
│   ├── init_db.py             # Fichier d'initialisation de la base de données
│   ├── ui.py                  # Interface utilisateur
│   └── utils.py               # Fonctions utilitaires
├── docker-compose.yml         # Orchestration des services 
├── Dockerfile                 # Configuration Docker
├── init_app.sh                # Script pour initialiser l'application dans docker
├── pyproject.toml             # Configuration du projet uv
├── README.md                  # Documentation du projet
└── uv.lock                    # Verrouillage des dépendances uv
```

---

## ⚙️ **Workflow du Projet**

### **1. Ingestion des données**
**Objectif** : Récupérer des données brutes depuis des sources externes.
#### Étapes : 
Dans le fichier Python `ingestion.py`
- **`get_realtime_bicycle_data`** : 
  - Récupère les données en temps réel sur les vélos disponibles des villes (Paris, Nantes, Toulouse, Strasbourg).
- **`get_commune_data`** : 
  - Récupère des données sur les communes.

#### Produits :
- Les données brutes sont enregistrées dans les fichiers JSON dans la table de staging dédiée.


### **2. Transformation des données avec DBT**  
**Objectif** : Organiser, nettoyer et structurer les données brutes issues des API pour les rendre exploitables.

#### Étapes :  
La transformation des données est orchestrée via **DBT**, selon une architecture modulaire :

- 📁 **Staging**  
  - Création de tables temporaires à partir des fichiers bruts stockés dans la table staging_raw.  
  - Ces modèles permettent de normaliser les formats et de préparer les données pour les étapes suivantes.

- 📁 **Consolidate**  
  - Construction de tables consolidées, alimentées en **mode incrémental**, pour intégrer les nouvelles données sans retraiter l’ensemble du dataset.  
  - Les données des communes et des stations sont nettoyées, enrichies et structurées pour l’analyse.

#### Produits :  
- Les tables consolidées sont alimentées et servent de base aux modèles analytiques et aux vues agrégées.

---

### **3. Modélisation analytique**  
**Objectif** : Synthétiser les données consolidées pour produire des modèles analytiques et des vues prêtes à l’exploration.

#### Étapes :  
La modélisation suit une logique en étoile et se décompose en deux niveaux :

- 📁 **Star_model**  
  - Création des **tables dimensionnelles** (ex. : `dim_city`, `dim_station`) et de la **table factuelle** (`fact_station_statement`) en associant les données consolidées.  
  - Ces modèles facilitent les jointures et les analyses multi-axes.

- 📁 **Analytics**  
  - Génération de **vues analytiques** prêtes à être exposées dans **Streamlit**.  
  - Ces vues permettent d’explorer les métriques clés et les tendances du système de vélos en libre-service.

#### Produits :  
- Les vues finales créées et intégrées à l’interface Streamlit pour la visualisation interactive.

---

## 🚀 **Installation et Exécution**


1. **Construire les images Docker et lancer les containeurs :**  
   ```bash
   docker-compose up -d
   ```
   
2. **Accéder à l'interface streamlit :**  
   Rendez-vous sur [http://localhost:8501](http://localhost:8501) 

3. **Accéder à la documentation DBT :**  
   Rendez-vous sur [http://localhost:8080](http://localhost:8080) 