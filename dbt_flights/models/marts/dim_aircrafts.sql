{{ config(
    materialized='incremental',
    unique_key='aircraft_bk'    
) }}

WITH aircrafts AS (
    SELECT * FROM {{ source('staging', 'aircrafts') }}
)

SELECT * FROM aircrafts