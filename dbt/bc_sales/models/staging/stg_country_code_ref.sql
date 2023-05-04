{{ config(materialized='view') }}

select * from {{ source('staging', 'country_code_ref') }}
