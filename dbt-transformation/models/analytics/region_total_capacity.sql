WITH region_capacity AS (
    SELECT
        d.id_region AS id_region,
        SUM(s.capacity) AS total_capacity
    FROM {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
    JOIN {{ ref('department') }} AS d ON c.id_departement = d.id
    GROUP BY d.id_region
)

SELECT
    rc.id_region AS id_region,
    r.name AS region_name,
    rc.total_capacity
FROM region_capacity AS rc
LEFT JOIN {{ ref('region') }} AS r ON rc.id_region = r.id
