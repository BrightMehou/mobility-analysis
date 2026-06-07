WITH latest_data AS (
    SELECT MAX(created_date) AS max_date
    FROM {{ ref('fact_station_statement') }}
),
city_aggregation AS (
    SELECT
        city_id,
        SUM(bicycle_docks_available) AS bicycle_docks_available,
        SUM(bicycle_available) AS bicycle_available
    FROM
        {{ ref('fact_station_statement') }}
    WHERE
        created_date = (SELECT max_date FROM latest_data)
    GROUP BY
        city_id
)
SELECT
    dm.name,
    ca.bicycle_docks_available,
    ca.bicycle_available
FROM
    {{ ref('dim_city') }} AS dm
    INNER JOIN city_aggregation AS ca ON dm.id = ca.city_id