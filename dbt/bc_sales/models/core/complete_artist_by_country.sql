{{ config(materialized='table') }}

with refs as (
    select * from {{ ref('stg_country_code_ref') }}
),

max_totals as (
    select * from {{ ref('artist_max_sales_facts') }}
),

max_avg as (
    select * from {{ ref('artist_max_avg_facts') }}
)

select
    refs.cc_ref as ref,
    refs.country_code,
    max_totals.artist as highest_selling,
    max_avg.artist as highest_average,
from refs
right join max_totals
on refs.cc_ref = max_totals.ref
right join max_avg
on refs.cc_ref = max_avg.ref
order by 1
