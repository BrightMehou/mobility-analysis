SELECT
    id,
    code,
    name,
    address,
    latitude,
    longitude,
    status,
    capacity,
    bicycle_docks_available,
    bicycle_available,
    last_statement_date
FROM
    {{ ref('station') }}
WHERE
    latitude IS NOT NULL
    AND longitude IS NOT NULL