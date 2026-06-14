-- Paris
SELECT
    '{{ var("PARIS_CITY_CODE", "1") }}' || '-' || (json ->> 'stationcode') AS id,
    json ->> 'stationcode' AS code,
    json ->> 'name' AS name,
    json ->> 'nom_arrondissement_communes' AS city_name,
    json ->> 'code_insee_commune' AS city_code,
    NULL AS address,
    (json -> 'coordonnees_geo' ->> 'lon') :: DOUBLE PRECISION AS longitude,
    (json -> 'coordonnees_geo' ->> 'lat') :: DOUBLE PRECISION AS latitude,
    CASE 
    	 WHEN json ->> 'is_installed' = 'OUI' THEN 'open'
    	 WHEN json ->> 'is_installed' = 'NON' THEN 'closed'
    	 ELSE 'unknown'
    END AS STATUS,
    (json ->> 'capacity') :: INTEGER AS capacity,
    (json ->> 'numdocksavailable') :: INTEGER AS bicycle_docks_available,
    (json ->> 'numbikesavailable') :: INTEGER AS bicycle_available,
    (json ->> 'duedate') :: TIMESTAMP AS last_statement_date,
    current_date AS created_date
FROM
    {{ source('postgres', 'staging_raw') }},
    jsonb_array_elements(data) AS json
WHERE
    nom = 'paris.json'
UNION
ALL -- Nantes and Toulouse
SELECT
    CASE
        WHEN nom = 'nantes.json' THEN '{{ var("NANTES_CITY_CODE", "2") }}'
        WHEN nom = 'toulouse.json' THEN '{{ var("TOULOUSE_CITY_CODE", "3") }}'
    END || '-' || (json ->> 'number') AS id,
    json ->> 'number' AS code,
    json ->> 'name' AS name,
    CASE
        WHEN nom = 'nantes.json' THEN 'Nantes'
        WHEN nom = 'toulouse.json' THEN 'Toulouse'
    END AS city_name,
    CASE
        WHEN nom = 'nantes.json' THEN '44109'
        WHEN nom = 'toulouse.json' THEN '31555'
    END AS city_code,
    json ->> 'address' AS address,
    (json -> 'position' ->> 'lon') :: DOUBLE PRECISION AS longitude,
    (json -> 'position' ->> 'lat') :: DOUBLE PRECISION AS latitude,
    CASE 
    	 WHEN json ->> 'status' = 'OPEN' THEN 'open'
    	 WHEN json ->> 'status' = 'CLOSED' THEN 'closed'
    	 ELSE 'unknown'
    END AS STATUS,
    (json ->> 'bike_stands') :: INTEGER AS capacity,
    (json ->> 'available_bike_stands') :: INTEGER AS bicycle_docks_available,
    (json ->> 'available_bikes') :: INTEGER AS bicycle_available,
    (json ->> 'last_update') :: TIMESTAMP AS last_statement_date,
    current_date AS created_date
FROM
    {{ source('postgres', 'staging_raw') }},
    jsonb_array_elements(data) AS json
WHERE
    nom IN ('nantes.json', 'toulouse.json')
UNION
ALL -- Strasbourg
SELECT
    '{{ var("STRASBOURG_CITY_CODE", "4") }}' || '-' || (json ->> 'id') AS id,
    json ->> 'id' AS code,
    json ->> 'na' AS name,
    'Strasbourg' AS city_name,
    '67482' AS city_code,
    NULL AS address,
    (json ->> 'lon') :: DOUBLE PRECISION AS longitude,
    (json ->> 'lat') :: DOUBLE PRECISION AS latitude,
    CASE 
    	 WHEN json ->> 'is_installed' = '1' THEN 'open'
    	 WHEN json ->> 'is_installed' = '0' THEN 'closed'
    	 ELSE 'unknown'
    END AS STATUS,
    (json ->> 'to') :: INTEGER AS capacity,
    (json ->> 'num_docks_available') :: INTEGER AS bicycle_docks_available,
    (json ->> 'av') :: INTEGER AS bicycle_available,
    to_timestamp((json ->> 'last_reported') :: INT) AS last_statement_date,
    current_date AS created_date
FROM
    {{ source('postgres', 'staging_raw') }},
    jsonb_array_elements(data) AS json
WHERE
    nom = 'strasbourg.json'