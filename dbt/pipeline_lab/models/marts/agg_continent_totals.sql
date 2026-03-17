WITH daily_fact AS (
    SELECT * FROM {{ ref('fct_covid_daily') }}
),
locations AS (
    SELECT * FROM {{ ref('dim_locations') }}
)

SELECT
    l.continent,
    SUM(f.cases) AS total_cases,
    SUM(f.deaths) AS total_deaths,
    SUM(f.population) AS total_population_sum,
    SAFE_DIVIDE(SUM(f.cases), SUM(f.population)) * 100000 AS continent_cases_per_100k,
    SAFE_DIVIDE(SUM(f.deaths), SUM(f.population)) * 100000 AS continent_deaths_per_100k
FROM daily_fact f
JOIN locations l ON f.location_key = l.location_key
GROUP BY l.continent
ORDER BY total_cases DESC
