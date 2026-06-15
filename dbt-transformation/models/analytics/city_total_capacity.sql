SELECT
    c.name AS city_name,
    sum(s.capacity) AS total_capacity
FROM
    {{ ref('station') }} AS s
    JOIN {{ ref('city') }} AS c ON s.city_code = c.id
GROUP BY
    c.name