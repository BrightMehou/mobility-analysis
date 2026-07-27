WITH region_stats AS (
    SELECT
        d.id_region AS id_region,
        s.status,
        COUNT(*) AS nb
    FROM {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
    JOIN {{ ref('department') }} AS d ON c.id_departement = d.id
    GROUP BY d.id_region, s.status
)

SELECT
    rs.id_region AS id_region,
    r.name AS region_name,
    rs.status,
    rs.nb
FROM region_stats AS rs
LEFT JOIN {{ ref('region') }} AS r ON rs.id_region = r.id
