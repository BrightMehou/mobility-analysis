WITH region_aggregation AS (
    SELECT
        d.id_region AS id_region,
        SUM(s.bicycle_docks_available) AS bicycle_docks_available,
        SUM(s.bicycle_available) AS bicycle_available
    FROM {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
    JOIN {{ ref('department') }} AS d ON c.id_departement = d.id
    GROUP BY d.id_region
)

SELECT
    ra.id_region AS id_region,
    r.name AS region_name,
    ra.bicycle_docks_available,
    ra.bicycle_available
FROM region_aggregation AS ra
LEFT JOIN {{ ref('region') }} AS r ON ra.id_region = r.id
