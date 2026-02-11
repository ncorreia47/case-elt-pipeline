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
        ds_user_name
      , avg(nr_reply_time_in_minutes) as nr_avg_reply_time_in_minutes
    from tickets_gold
    where 1=1
    group by 1
    order by 2 desc

)

select * 
from final