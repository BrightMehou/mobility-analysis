{% docs __overview__ %}

# Mobility Analysis — transformations dbt

Ce projet dbt (`transformation`) structure les données de vélos en libre-service
(Paris, Nantes, Toulouse, Strasbourg) ingérées dans PostgreSQL.

## Couches

| Couche | Dossier | Matérialisation | Rôle |
|--------|---------|-----------------|------|
| **Staging** | `models/staging/` | `ephemeral` | Parse JSON depuis `staging_raw`, unifie les schémas par ville |
| **Consolidate** | `models/consolidate/` | `incremental` (merge) | Tables historisées stations, villes, relevés |
| **Analytics** | `models/analytics/` | `view` | Métriques exposées à Streamlit |

## Variables projet

Codes ville préfixant les identifiants station (`dbt_project.yml`) :

- `PARIS_CITY_CODE` (défaut : 1)
- `NANTES_CITY_CODE` (défaut : 2)
- `TOULOUSE_CITY_CODE` (défaut : 3)
- `STRASBOURG_CITY_CODE` (défaut : 4)
{% enddocs %}
