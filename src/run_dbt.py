"""Fonctions réutilisables pour les transformations ELT via `dbt run`."""

import logging

from dbt.cli.main import dbtRunner, dbtRunnerResult

logger = logging.getLogger(__name__)


def data_transformation() -> bool:
    """
    Exécute la commande `dbt run`.
    """

    logger.info("🚀 Démarrage de la commande dbt run")

    dbt = dbtRunner()
    cli_args = [
        "run",
        "--project-dir",
        "dbt-transformation",
        "--profiles-dir",
        "dbt-transformation",
    ]
    res: dbtRunnerResult = dbt.invoke(cli_args)
    return res.success
