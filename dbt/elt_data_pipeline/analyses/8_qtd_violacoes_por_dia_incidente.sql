{{
  config(
    materialized = "view",
    tags = ["analyses"]
  )
}}


-- fonte da verdade para ticket_sla_events
with ticket_sla_events_silver as (

    select * from {{ ref('ticket_sla_events_silver') }}

)

-- fonte da verdade para tickets
, tickets_gold as (

    select * from {{ ref('tickets_gold') }}

)

, qtd_violacoes as (

    select 
        cd_ticket_id
      , to_char(dt_time, 'yyyy-mm-dd') as dt_violacao
      , count(1)                       as nr_qtd_violacoes
    from ticket_sla_events_silver 
    where 1=1 
    and ds_type = 'BREACH' --violado 
    group by 1, 2

)

, final as (

    select
        ts.ds_type 
      , qv.dt_violacao
      , sum(qv.nr_qtd_violacoes) as nr_qtd_violacoes
    from tickets_gold ts 
    inner join qtd_violacoes qv 
      on ts.cd_ticket_id = qv.cd_ticket_id
    where 1=1 
      and ds_type = 'INCIDENT'
      group by 1, 2

)