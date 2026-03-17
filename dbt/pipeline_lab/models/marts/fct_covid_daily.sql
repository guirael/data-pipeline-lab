WITH metrics AS (
    SELECT * FROM {{ ref('int_covid_metrics') }}
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', reference_date) AS INT64) AS date_key,
    FARM_FINGERPRINT(country_code) AS location_key,
    
    cases,
    deaths,
    population,
    cases_per_100k,
    deaths_per_100k
FROM metrics
WHERE country_code IS NOT NULL
