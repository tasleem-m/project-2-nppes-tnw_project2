{% test distinct_providers_by_geography(model) %}

SELECT provider_name, county_name, city_name, state_abbv, COUNT(Taxonomy_Code) AS occurrences
FROM {{ model }}
GROUP BY provider_name, county_name, city_name, state_abbv
HAVING COUNT(Taxonomy_Code) > 1

{% endtest %}