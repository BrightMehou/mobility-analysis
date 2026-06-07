station_aggregation AS (
    SELECT
        station_id,
        AVG(bicycle_available) AS avg_dock_available
    FROM
        {{ ref('fact_station_statement') }}
    GROUP BY
        station_id
)
SELECT
    ds.name,
    ds.code,
    ds.address,
    sa.avg_dock_available
FROM
    {{ ref('dim_station') }} AS ds
    INNER JOIN station_aggregation AS sa ON ds.id = sa.station_id