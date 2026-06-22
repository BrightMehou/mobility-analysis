SELECT
    (ROW ->> 'population') :: integer AS NB_INHABITANTS,
    ROW ->> 'code' AS ID,
    ROW ->> 'nom' AS NAME,
    ROW ->> 'codeDepartement' AS CODE_DEPARTEMENT,
    current_date AS CREATED_DATE
FROM
    {{ source('postgres', 'staging_raw') }},
    jsonb_array_elements(DATA) AS ROW
WHERE
    NOM = 'communes.json'