WITH department_capacity AS (
    SELECT
        c.id_departement AS id_departement,
        SUM(s.capacity) AS total_capacity
    FROM {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
    GROUP BY c.id_departement
)

SELECT
    dc.id_departement AS id_departement,
    d.name AS department_name,
    dc.total_capacity
FROM department_capacity AS dc
LEFT JOIN {{ ref('department') }} AS d ON dc.id_departement = d.id
