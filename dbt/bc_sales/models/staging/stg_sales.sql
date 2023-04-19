{{ config(materialized='view') }}

select * from {{ source('staging', 'bc_sales') }}
limit 100
