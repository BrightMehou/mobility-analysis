WITH department_aggregation AS (
    SELECT
        c.id_departement AS id_departement,
        SUM(s.bicycle_docks_available) AS bicycle_docks_available,
        SUM(s.bicycle_available) AS bicycle_available
    FROM {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
    GROUP BY c.id_departement
)

SELECT
    id_departement,
    bicycle_docks_available,
    bicycle_available
FROM department_aggregation
