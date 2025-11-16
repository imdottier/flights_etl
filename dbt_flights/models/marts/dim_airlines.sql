{{ config(
    materialized='incremental',
    unique_key='airline_bk'    
) }}


WITH airlines AS (
    SELECT * FROM {{ source('staging', 'airlines') }}
)

SELECT * FROM airlines