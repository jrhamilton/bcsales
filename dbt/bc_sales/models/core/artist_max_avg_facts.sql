{{ config(materialized='table') }}

with facts as (
    select * from {{ ref('artist_facts') }}
),

refs as (
    select * from {{ ref('stg_country_code_ref') }}
),

max_avg_sales as (
    select
        facts.cc_ref as ref,
        --facts.artist,
        max(facts.avg_sales_usd) as max_avg_usd
    from facts
    group by 1
)

select
    max_avg_sales.ref as ref,
    max_avg_sales.max_avg_usd as max_avg_usd,
    facts.artist as artist
from max_avg_sales
left join facts
on max_avg_sales.ref = facts.cc_ref
and max_avg_sales.max_avg_usd = facts.avg_sales_usd
order by 1
