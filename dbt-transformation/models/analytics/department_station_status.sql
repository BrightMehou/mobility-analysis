SELECT
    c.code_departement AS code_departement,
    s.status,
    COUNT(*) AS nb
FROM {{ ref('station') }} AS s
JOIN {{ ref('city') }} AS c ON s.city_code = c.id
GROUP BY c.code_departement, s.status
