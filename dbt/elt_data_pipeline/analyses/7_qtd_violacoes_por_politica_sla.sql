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
        ds_sla_policy_title
      , count(1)            as nr_qtd_violacoes
    from tickets_gold
    where 1=1 
      and is_breached --violado 
    group by 1
    order by 2 desc

)

select *
from final