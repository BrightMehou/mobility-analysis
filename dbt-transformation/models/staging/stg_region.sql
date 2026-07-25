SELECT
    json ->> 'code' AS id,
    json ->> 'nom' AS name,
    current_date AS created_date
FROM
    {{ source('postgres', 'staging_raw') }},
    jsonb_array_elements(data) AS json
WHERE
    nom = 'regions.json'