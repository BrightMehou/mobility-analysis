SELECT
    c.id_departement AS id_departement,
    SUM(s.capacity) AS total_capacity
FROM {{ ref('station') }} AS s
JOIN {{ ref('city') }} AS c ON s.city_code = c.id
GROUP BY c.id_departement
