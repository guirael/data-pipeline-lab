WITH deduplicated_locations AS (
    SELECT
        country,
        country_code,
        continent,
        ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY reference_date DESC) as rn
    FROM {{ ref('stg_covid_data') }}
    WHERE country_code IS NOT NULL
)

SELECT
    FARM_FINGERPRINT(country_code) AS location_key,
    country,
    country_code,
    continent
FROM deduplicated_locations
WHERE rn = 1