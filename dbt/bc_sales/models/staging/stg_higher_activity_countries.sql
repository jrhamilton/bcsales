{{ config(materialized='view') }}

-- Q2, Q3

{% set sql_statement %}
    select cc_ref
    from {{ source('staging', 'country_code_ref') }}
    where count > 20
    order by
	cc_ref desc
    limit 1
{% endset %}

{%- set lowest_count_country_ref_id = dbt_utils.get_single_value(sql_statement, 0) -%}

select
    cast(cc_ref as integer) as cc_ref,
    cast(artist_name as string) as artist_name,
    cast(amount_paid_usd as numeric) as amount_paid_usd,
    cast(country_code as string) as country_code, --REMOVE THIS TOO??
    cast(country as string) as country -- TODO: REMOVE THIS LINE
from {{ source('staging','sales_clustered') }}
where cc_ref <= {{ lowest_count_country_ref_id }}
order by
    cc_ref asc,
    artist_name asc
