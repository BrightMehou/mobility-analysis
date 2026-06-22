SELECT
    c.code_departement AS code_departement,
    SUM(s.capacity) AS total_capacity
FROM {{ ref('station') }} AS s
JOIN {{ ref('city') }} AS c ON s.city_code = c.id
GROUP BY c.code_departement
