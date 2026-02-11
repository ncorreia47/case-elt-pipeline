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
        ds_service_type
      , avg(nr_ticket_duration_in_minutes) as nr_avg_ticket_duration_in_minutes
    from tickets_gold
    where 1=1 
      and ds_status in ('CLOSED', 'SOLVED') --resolvido
    group by 1
    order by 2 desc

)

select * 
from final