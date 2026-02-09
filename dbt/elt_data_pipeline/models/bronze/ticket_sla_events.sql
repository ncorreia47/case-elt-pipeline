{{
  config(
    materialized = "table",
    tags = ["bronze", "ticket_sla_events"]
  )
}}

with raw_ticket_metrics as (

    select * from {{ source('landing', 'api_files_landing') }} afl where afl.endpoint = 'ticket_sla_events'

)

, exploded_json as (

    select
        jsonb_array_elements(payload) as obj
    from raw_ticket_sla_events

)

, final as (

    select
        (obj ->> 'id')::int                 as id
      , (obj -> 'sla' ->> 'policy_id')::int as sla_policy_id
      , obj -> 'sla' ->> 'policy_title'     as sla_policy_title
      , (obj ->> 'time')::timestamp         as time
      , obj ->> 'type'                      as type
      , obj ->> 'metric'                    as metric
      , (obj ->> 'ticket_id')::int          as ticket_id
      , current_timestamp                   as ingested_at
      , {{ snapshot_ts() }}                 as snapshot_ts
    from exploded_json
	
)

select *
from final