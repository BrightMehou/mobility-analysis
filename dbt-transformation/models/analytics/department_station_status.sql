WITH department_stats AS (
    SELECT
        c.id_departement AS id_departement,
        s.status,
        COUNT(*) AS nb
    FROM {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
    GROUP BY c.id_departement, s.status
)

SELECT
    ds.id_departement AS id_departement,
    d.name AS department_name,
    ds.status,
    ds.nb
FROM department_stats AS ds
LEFT JOIN {{ ref('department') }} AS d ON ds.id_departement = d.id
