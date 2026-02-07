SELECT ROUND((COUNT(pr.Taxonomy_Code) / pop.POP)*10000) AS provider_per_capita, county_name, state_abbv
FROM {{ ref('provider_directory') }} AS pr
INNER JOIN {{ ref('population_clean') }} AS pop
USING (county_name, state_abbv)
GROUP BY pop.POP, county_name, state_abbv
ORDER BY county_name