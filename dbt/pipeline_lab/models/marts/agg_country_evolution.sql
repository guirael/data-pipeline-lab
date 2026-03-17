WITH daily_fact AS (
    SELECT * FROM {{ ref('fct_covid_daily') }}
),
locations AS (
    SELECT * FROM {{ ref('dim_locations') }}
),
dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

SELECT
    d.full_date AS reference_date,
    l.country,
    l.country_code,
    l.continent,
    f.cases,
    f.deaths,
    f.population,
    f.cases_per_100k,
    f.deaths_per_100k
FROM daily_fact f
JOIN locations l ON f.location_key = l.location_key
JOIN dates d ON f.date_key = d.date_key
ORDER BY l.country, d.full_date
