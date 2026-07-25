SELECT
    c.id_departement AS id_departement,
    s.status,
    COUNT(*) AS nb
FROM {{ ref('station') }} AS s
JOIN {{ ref('city') }} AS c ON s.city_code = c.id
GROUP BY c.id_departement, s.status
