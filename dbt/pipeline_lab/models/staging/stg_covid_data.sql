WITH source AS (
    SELECT * FROM {{ source('ds_ex_lab', 'raw_covid') }}
)

SELECT
    DATE(Date_Year, Date_Month, Date_Day) AS reference_date,
    CAST(Data_Cases AS INT64) AS cases,
    CAST(Data_Deaths AS INT64) AS deaths,
    CAST(Data_Population AS INT64) AS population,
    Location_Country AS country,
    Location_Code AS country_code,
    Location_Continent AS continent,
    Data_Rate AS original_rate
FROM source
WHERE Date_Year IS NOT NULL 
  AND Date_Month IS NOT NULL 
  AND Date_Day IS NOT NULL
