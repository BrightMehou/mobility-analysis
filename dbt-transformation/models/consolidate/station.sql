{{ config(unique_key = ['id'],) }}
SELECT
    source.id,
    source.code,
    source.name,
    source.city_name,
    source.city_code,
    source.address,
    source.longitude,
    source.latitude,
    source.status,
    source.capacity,
    source.bicycle_docks_available,
    source.bicycle_available,
    source.last_statement_date :: TIMESTAMP AS last_statement_date,
    source.created_date :: DATE
FROM
    {{ ref('stg_station') }} as source {% if is_incremental() %}
WHERE
    source.created_date >= (
    SELECT
      COALESCE(MAX(created_date), '1900-01-01')
    FROM
      {{ this }}
  ) {% endif %}