WITH latest_data AS (
    SELECT MAX(created_date) AS max_date
    FROM {{ ref('fact_station_statement') }}
)
SELECT
    dc.name AS city_name,
    sum(ds.capacity) AS total_capacity
FROM
    {{ ref('dim_station') }} AS ds
    JOIN {{ ref('fact_station_statement') }} AS fss ON ds.id = fss.station_id
    JOIN {{ ref('dim_city') }} AS dc ON fss.city_id = dc.id
WHERE
    fss.created_date = (SELECT max_date FROM latest_data)
GROUP BY
    dc.name