SELECT COUNT(n.NPI) AS provider_count, n.state_abbv
FROM {{ ref('nppes_clean') }} AS n
INNER JOIN {{ ref('states_clean') }} AS s
ON n.state_abbv = s.state
GROUP BY n.state_abbv
ORDER BY provider_count DESC