"""
Tableau de bord Streamlit pour l’analyse de mobilité urbaine 🚲.

Fonctionnalités principales :
- Lancement du pipeline (ingestion + transformation) via un bouton.
- Carte interactive des stations avec Plotly.
- Indicateurs clés par ville et par station.
"""

import logging
from collections.abc import Callable

import pandas as pd
import plotly.express as px
import streamlit as st
from plotly.graph_objects import Figure
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ingestion import data_ingestion
from utils import data_transformation, get_db_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Tableau de bord mobilité", page_icon="🚲", layout="wide")
st.logo("🚲")


st.title("📊 Tableau de bord des stations de vélos 🚲")

if st.button("🔄 Actualiser"):
    try:
        with st.status("🚀 Lancement du pipeline...", expanded=True) as status:
            steps: list[tuple[str, Callable[[], None]]] = [
                ("Ingestion des données", data_ingestion),
                ("Transformation des données", data_transformation),
            ]

            for label, func in steps:
                status.update(label=label, state="running")
                logger.info(label)
                func()
                status.update(label=f"✅ {label}", state="complete")

            status.update(label="✅ Pipeline terminé avec succès !", state="complete")
            st.success("Données alimentées et prêtes à l’affichage !")

    except Exception as e:
        logger.exception("Erreur pipeline")
        st.error(f"❌ Échec du pipeline à l'étape '{label}' : {e}")


def load_dataframe(con, query: str) -> pd.DataFrame:
    try:
        return pd.read_sql_query(text(query), con)
    except (SQLAlchemyError, pd.errors.DatabaseError) as exc:
        logger.warning("Erreur lors du chargement du DataFrame : %s", exc)
        return pd.DataFrame()

tab_global, tab_department, tab_city, tab_station = st.tabs(
    [
        "🌐 Global",
        "🏛️ Département",
        "🏙️ City",
        "🗺️ Station",
    ]
)

engine = get_db_engine()
with engine.connect() as con:
    with tab_global:
        st.subheader("🌐 Indicateurs globaux")
        query_global_metrics = "select * from global_metrics;"
        df_global_metrics = load_dataframe(con, query_global_metrics)

        if df_global_metrics.empty:
            st.warning("Aucune donnée globale disponible pour le moment.")
        else:
            display_cols = df_global_metrics.columns.tolist()
            cols_layout = st.columns(len(display_cols))
            first_row = df_global_metrics.iloc[0]
            for col_name, col_place in zip(display_cols, cols_layout):
                val = first_row[col_name]
                col_place.metric(col_name.replace("_", " ").title(), f"{val}", border=True)

        logger.info("Données globales chargées.")

        query_global_status = "select * from global_status;"
        df_global_status = load_dataframe(con, query_global_status)
        if df_global_status.empty:
            st.info("Aucun statut global disponible.")
        else:
            fig_status = px.pie(
                df_global_status,
                names="status",
                values="nb",
                title="Répartition des statuts des stations",
                color="status",
                color_discrete_map={
                    "open": "green",
                    "closed": "red",
                    "unknown": "yellow",
                },
            )
            fig_status.update_traces(textinfo="percent+label")
            st.plotly_chart(fig_status, use_container_width=True)

    with tab_city:
        st.subheader("🏙️ Indicateurs par ville")
        queries_city: list[tuple[str, str]] = [
            ("Emplacements dispo par ville", "select * from city_available_emplacement;"),
            ("Capacité totale par ville", "select * from city_total_capacity;"),
            ("Statuts des stations par ville", "select * from city_station_status;"),
        ]

        for title, query in queries_city:
            st.markdown(f"**{title}**")
            df = load_dataframe(con, query)
            if df.empty:
                st.warning("Aucune donnée disponible pour cette vue.")
            st.dataframe(df, width="stretch")
            logger.info(f"Données pour '{title}' chargées.")

    with tab_department:
        st.subheader("🏛️ Indicateurs par département")
        queries_department: list[tuple[str, str]] = [
            ("Emplacements dispo par département", "select * from department_available_emplacement;"),
            ("Capacité totale par département", "select * from department_total_capacity;"),
            ("Statuts des stations par département", "select * from department_station_status;"),
        ]

        for title, query in queries_department:
            st.markdown(f"**{title}**")
            df = load_dataframe(con, query)
            if df.empty:
                st.warning("Aucune donnée disponible pour cette vue.")
            st.dataframe(df, width="stretch")
            logger.info(f"Données pour '{title}' chargées.")

    with tab_station:
        st.subheader("🗺️ Carte interactive des stations")
        query_map: str = "select * from map_station;"
        df_map: pd.DataFrame = load_dataframe(con, query_map)

        if df_map.empty:
            st.warning("Aucune donnée disponible pour la carte.")
            logger.warning("DataFrame pour la carte est vide.")
        else:
            fig: Figure = px.scatter_map(
                df_map,
                lat="latitude",
                lon="longitude",
                hover_name="name",
                hover_data=[
                    "id",
                    "code",
                    "address",
                    "status",
                    "capacity",
                    "bicycle_docks_available",
                    "bicycle_available",
                    "last_statement_date",
                ],
                color="bicycle_available",
                color_continuous_scale=px.colors.sequential.Plasma,
                center=dict(lat=48.8566, lon=2.3522),  # Paris
                size_max=15,
                height=600,
                zoom=11,
            )
            st.plotly_chart(fig, config={"width": "stretch", "height": 600})
            logger.info("Données pour la carte chargées.")

st.caption("Données issues des API publiques des stations de vélos.")
