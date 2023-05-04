{{ config(materialized='table') }}

with facts as (
    select * from {{ ref('country_facts') }}
),

refs as (
    select * from {{ ref('stg_country_code_ref') }}
)

select
    facts.ref as ref,
    facts.total_transactions as total_transactions,
    facts.average_sale_usd as avg_sale_usd,
    facts.total_sales_usd as total_sales_usd,
    refs.country_code as cc
from facts
left join refs
on facts.ref = refs.cc_ref
where total_transactions > 100
order by 3 desc
