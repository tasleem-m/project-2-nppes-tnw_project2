SELECT n.provider_name, p.county_name, n.Provider_Business_Practice_Location_Address_City_Name AS city_name, n.state_abbv, n.ZIP, s.Classification, n.Taxonomy_Code, z.county_code
FROM {{ ref('nppes_clean') }} AS n
INNER JOIN {{ ref('zip_county_clean') }} AS z
USING (ZIP)
INNER JOIN {{ ref('population_clean') }} AS p
USING (county_code)
INNER JOIN {{ ref('specialties_clean') }} AS s
USING (Taxonomy_Code)