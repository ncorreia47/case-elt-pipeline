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
      , count(1) as qtd_tickets
    from tickets_gold ts 
    where 1=1 
      and ds_priority  = 'URGENT'
      group by 1 
      order by 2 desc

)

select * 
from final