{{
  config(
    materialized = "table",
    tags = ["gold", "tickets_gold"]
  )
}}

with tickets_silver as (

    select * from {{ ref('tickets_silver') }}

)

, ticket_custom_fields_silver as (

    select * from {{ ref('ticket_custom_fields_silver') }}
    where 1=1 
      and cd_ticket_custom_field_id = 1 -- servico

)

, ticket_metrics_silver as (

    select * from {{ ref('ticket_metrics_silver') }}

)

, ticket_sla_events_silver_breached as (

    select
        distinct
        cd_ticket_id 
      , ds_type 
      , ds_sla_policy_title
    from {{ ref('ticket_sla_events_silver') }}
    where 1=1
      and ds_type = 'BREACH' --violado

)

, organizations_silver as ( 

    select * from {{ ref('organizations_silver') }}
    
)

, users_silver as ( 

    select * from {{ ref('users_silver') }}
    
)

, groups_silver as ( 

    select * from {{ ref('groups_silver') }}
    
)

, tickets_gold as (

    select
        t.cd_ticket_id
      , t.ds_url
      , t.ds_type
      , t.is_public
      , t.ds_status
      , t.ds_channel
      , t.cd_group_id
      , g.ds_group_name
      , t.ds_priority
      , tcf.ds_ticket_custom_field as ds_service_type
      , tm.nr_reopens 
      , tm.nr_replies 
      , tm.dt_solved_at 
      , tm.nr_satisfaction_score 
      , tm.nr_reply_time_in_minutes 
      , tm.nr_on_hold_time_in_minutes 
      , tm.nr_requester_wait_time_in_minutes
      , extract(epoch from (tm.dt_solved_at - t.dt_created_at)) / 60 as nr_ticket_duration_in_minutes  
      , tse.ds_sla_policy_title
      , case 
      	    when coalesce(tse.cd_ticket_id, 0) > 0 then true
      	    else false
      	end as is_breached
      , t.cd_assignee_id
      , u.ds_user_name
      , t.ds_description
      , t.cd_organization_id
      , o.ds_organization_name
      , o.cd_external_id
      , t.dt_created_at
      , t.dt_updated_at
    from tickets_silver t
    left join users_silver u 
      on t.cd_assignee_id = u.cd_user_id
    left join groups_silver g 
      on t.cd_group_id = g.cd_group_id
    left join organizations_silver o 
      on t.cd_organization_id = o.cd_organization_id
    left join ticket_custom_fields_silver tcf 
      on t.cd_ticket_id = tcf.cd_ticket_id
    left join ticket_metrics_silver tm 
      on t.cd_ticket_id = tm.cd_ticket_id
    left join ticket_sla_events_silver_breached tse 
      on t.cd_ticket_id = tse.cd_ticket_id
	
)

, final as (
    
    select
        * 
      , current_timestamp    as ingested_at
      , {{ snapshot_ts() }}  as snapshot_ts
    from tickets_gold
    
)

select *
from final