SELECT DISTINCT Classification, county_name, city_name, state_abbv
FROM {{ ref('provider_directory') }}