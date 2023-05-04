{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('stg_higher_activity_countries') }}
)

select
    cc_ref as cc_ref,
    artist_name as artist,
    sum(amount_paid_usd) as total_sales_usd,
    avg(amount_paid_usd) as avg_sales_usd,
from sales
group by 1,2
order by 1, 3 desc
