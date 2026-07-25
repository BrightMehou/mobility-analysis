SELECT
    json ->> 'code' AS ID,
    json ->> 'nom' AS NAME,
    json ->>'codeRegion' AS ID_REGION,
    current_date AS CREATED_DATE
FROM
    {{ source('postgres', 'staging_raw') }},
    jsonb_array_elements(data) AS json
WHERE
    nom = 'departements.json'