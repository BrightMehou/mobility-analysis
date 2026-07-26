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
    da.id_departement AS id_departement,
    d.name AS department_name,
    da.bicycle_docks_available,
    da.bicycle_available
FROM department_aggregation AS da
LEFT JOIN {{ ref('department') }} AS d ON da.id_departement = d.id
