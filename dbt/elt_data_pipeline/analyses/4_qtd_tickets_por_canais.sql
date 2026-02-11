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
        ds_channel
      , count(1) as qtd_tickets
    from tickets_gold
    group by 1 

)

select * 
from final