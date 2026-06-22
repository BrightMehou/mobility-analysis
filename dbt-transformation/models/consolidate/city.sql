{{ config(unique_key = ['id'],) }}
SELECT
  id,
  NAME,
  code_departement,
  nb_inhabitants,
  created_date :: DATE AS created_date
FROM
  {{ ref('stg_city') }} {% if is_incremental() %}
WHERE
  created_date >= (
    SELECT
      COALESCE(MAX(created_date), '1900-01-01')
    FROM
      {{ this }}
  ) {% endif %}