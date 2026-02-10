with ticket_sla_events_silver as (

   select * from elt_data_pipeline_silver.ticket_sla_events_silver 
 
)

, qtd_violacoes as (

   select 
       cd_ticket_id
     , to_char(dt_time, 'yyyy-mm-dd') as dt_violacao
     , count(1)                       as nr_qtd_violacoes
   from elt_data_pipeline_silver.ticket_sla_events_silver 
   where 1=1 
   and ds_type = 'BREACH' --violado 
   group by 1, 2

)

select
    ts.ds_type 
  , qv.dt_violacao
  , sum(qv.nr_qtd_violacoes) as nr_qtd_violacoes
from elt_data_pipeline_silver.tickets_silver ts 
inner join qtd_violacoes qv 
on ts.cd_ticket_id = qv.cd_ticket_id
where 1=1 
and ds_type = 'INCIDENT'
group by 1, 2