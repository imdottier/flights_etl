INSERT INTO gold.dim_dates (
    date_key, full_date, day_of_month, day_name, day_of_week_iso, day_of_year,
    week_of_year, month_of_year, month_name, quarter_of_year, year, is_weekend
)
SELECT 
    TO_CHAR(d.full_date, 'YYYYMMDD')::INT AS date_key,
    d.full_date,
    EXTRACT(DAY FROM d.full_date) AS day_of_month,
    TRIM(TO_CHAR(d.full_date, 'Day')) AS day_name,
    EXTRACT(ISODOW FROM d.full_date) AS day_of_week_iso,
    EXTRACT(DOY FROM d.full_date) AS day_of_year,
    EXTRACT(WEEK FROM d.full_date) AS week_of_year,
    EXTRACT(MONTH FROM d.full_date) AS month_of_year,
    TRIM(TO_CHAR(d.full_date, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM d.full_date) AS quarter_of_year,
    EXTRACT(YEAR FROM d.full_date) AS year,
    CASE WHEN EXTRACT(ISODOW FROM d.full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM
    SELECT generate_series('2000-01-01'::DATE, '2040-12-31'::DATE, '1 day'::INTERVAL) AS full_date
AS d;

INSERT INTO gold.dim_date (date_key, full_date, day_name) 
VALUES (-1, '1900-01-01', 'Unknown') 
ON CONFLICT (date_key) DO NOTHING;