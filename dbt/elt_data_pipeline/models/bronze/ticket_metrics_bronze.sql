{{
  config(
    materialized = "table",
    tags = ["bronze", "ticket_metrics_bronze"]
  )
}}

-- deduplica arquivos json duplicados, por ocasioes de reprocessamentos. Ainda nao aplicamos nenhum tipo de merge/update nos dados
with raw_ticket_metrics as (

    select distinct * from {{ source('landing', 'api_files_landing') }} afl where afl.endpoint = 'ticket_metrics'

)

, exploded_json as (

    select
        jsonb_array_elements(payload) as obj
    from raw_ticket_metrics

)

, final as (

    select
        (obj ->> 'id')::int                             as id
      , (obj ->> 'reopens')::int                        as reopens
      , (obj ->> 'replies')::int                        as replies
      , (obj ->> 'solved_at')::timestamp                as solved_at
      , (obj ->> 'ticket_id')::int                      as ticket_id
      , (obj ->> 'created_at')::timestamp               as created_at
      , (obj ->> 'assigned_at')::timestamp              as assigned_at
      , obj ->> 'satisfaction_score'                    as satisfaction_score
      , (obj ->> 'assignee_updated_at')::timestamp      as assignee_updated_at
      , (obj ->> 'reply_time_in_minutes')::int          as reply_time_in_minutes
      , (obj ->> 'on_hold_time_in_minutes')::int        as on_hold_time_in_minutes
      , (obj ->> 'requester_wait_time_in_minutes')::int as requester_wait_time_in_minutes
      , current_timestamp                               as ingested_at
      , {{ snapshot_ts() }}                             as snapshot_ts
    from exploded_json
	
)

select *
from final