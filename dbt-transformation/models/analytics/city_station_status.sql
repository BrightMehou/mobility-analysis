SELECT
    c.name AS city_name,
    s.status,
    count(*) AS nb
FROM
    {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
GROUP BY
    c.name,
    s.status
