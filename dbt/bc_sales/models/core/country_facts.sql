{{ config(materialized='table') }}


with countries as (
    select * from {{ ref('stg_sales_by_country') }}
)

select
    countries.cc_ref as ref,
    count(countries.cc_ref) as total_transactions,
    avg(countries.amount_paid_usd) as average_sale_usd,
    sum(countries.amount_paid_usd) as total_sales_usd,
from countries
group by countries.cc_ref
order by average_sale_usd desc
