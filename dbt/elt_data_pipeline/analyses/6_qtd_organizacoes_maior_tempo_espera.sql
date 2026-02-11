{{
  config(
    materialized = "view",
    tags = ["analyses"]
  )
}}


-- fonte da verdade para analises dos tickets, pois é a camada mais proxima do negocio 
with tickets_gold as (

    select * from {{ ref('tickets_gold') }}

)

, final as (

    select 
        ds_organization_name
      , sum(nr_on_hold_time_in_minutes) as nr_sum_on_hold_time_in_minutes
    from tickets_gold
    where 1=1
    group by 1
    order by 2 desc
    limit 10

)

select * 
from final