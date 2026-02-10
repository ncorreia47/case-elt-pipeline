{{
  config(
    materialized = "view",
    tags = ["analyses", "violacoes_por_politica_sla"]
  )
}}

with ticket_sla_events_silver as (

    select * from {{ ref('ticket_sla_events_silver') }}

)

, final as (
    select 
        cd_sla_policy_id
      , ds_sla_policy_title
      , count(1)            as nr_qtd_violacoes
    from ticket_sla_events_silver
    where 1=1 
      and ds_type = 'BREACH' --violado 
    group by 1, 2
    order by 3 desc
)

select *
from final