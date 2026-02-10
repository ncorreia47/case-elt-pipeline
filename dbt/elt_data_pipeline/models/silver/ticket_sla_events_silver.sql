{{
  config(
    materialized = "table",
    tags = ["silver", "ticket_sla_events_silver"]
  )
}}

with ticket_sla_events_bronze as (

    select * from {{ ref('ticket_sla_events_bronze') }}

)

, final as (

    select
        id                                              as cd_ticket_sla_event_id
      , sla_policy_id                                   as cd_sla_policy_id
      , upper(sla_policy_title)                         as ds_sla_policy_title
      , regexp_replace(sla_policy_title, '\D', '', 'g') as nr_sla_response_limit_in_hour
      , time                                            as dt_time
      , upper(type)                                     as ds_type
      , upper(metric)                                   as ds_metric
      , ticket_id                                       as cd_ticket_id
      , current_timestamp                               as ingested_at
      , {{ snapshot_ts() }}                             as snapshot_ts
    from ticket_sla_events_bronze
	
)

select *
from final