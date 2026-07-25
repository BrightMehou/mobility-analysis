{{ config(unique_key = ['id'],) }}
SELECT
  id,
  NAME,
  id_region,
  created_date :: DATE AS created_date
FROM
  {{ ref('stg_department') }} {% if is_incremental() %}
WHERE
  created_date >= (
    SELECT
      COALESCE(MAX(created_date), '1900-01-01')
    FROM
      {{ this }}
  ) {% endif %}