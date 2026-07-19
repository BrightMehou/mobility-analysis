{% docs __overview__ %}

# Mobility Analysis

Ce projet dbt (`transformation`) structure les données de vélos en libre-service

## Couches

| Couche | Dossier | Matérialisation | Rôle |
|--------|---------|-----------------|------|
| **Staging** | `models/staging/` | `ephemeral` | Parse JSON depuis `staging_raw`, unifie les schémas par ville |
| **Consolidate** | `models/consolidate/` | `incremental` (merge) | Tables historisées stations, villes, relevés |
| **Analytics** | `models/analytics/` | `view` | Métriques exposées à Streamlit |
{% enddocs %}
