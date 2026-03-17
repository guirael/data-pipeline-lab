WITH staging AS (
    SELECT * FROM {{ ref('stg_covid_data') }}
)

SELECT
    reference_date,
    country,
    country_code,
    continent,
    cases,
    deaths,
    population,
    
    SAFE_DIVIDE(cases, population) * 100000 AS cases_per_100k,
    SAFE_DIVIDE(deaths, population) * 100000 AS deaths_per_100k
FROM staging
WHERE population > 0
