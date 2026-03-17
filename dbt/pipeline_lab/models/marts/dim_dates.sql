WITH staging AS (
    SELECT DISTINCT
        reference_date
    FROM {{ ref('stg_covid_data') }}
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', reference_date) AS INT64) AS date_key,
    reference_date AS full_date,
    EXTRACT(DAY FROM reference_date) AS day_of_month,
    EXTRACT(MONTH FROM reference_date) AS month,
    EXTRACT(YEAR FROM reference_date) AS year,
    FORMAT_DATE('%B', reference_date) AS month_name,
    FORMAT_DATE('%A', reference_date) AS day_name
FROM staging
ORDER BY reference_date
