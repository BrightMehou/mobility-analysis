WITH city_aggregation AS (
    SELECT
        city_code,
        SUM(bicycle_docks_available) AS bicycle_docks_available,
        SUM(bicycle_available) AS bicycle_available
    FROM
        {{ ref('station') }}
    GROUP BY
        city_code
)
SELECT
    c.name,
    ca.bicycle_docks_available,
    ca.bicycle_available
FROM
    {{ ref('city') }} AS c
    INNER JOIN city_aggregation AS ca ON c.id = ca.city_code