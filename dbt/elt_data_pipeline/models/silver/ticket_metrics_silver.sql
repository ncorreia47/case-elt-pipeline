{{
  config(
    materialized = "incremental",
    unique_key= ["cd_ticket_metrics_id", "cd_ticket_id"],
    tags = ["silver", "ticket_metrics_silver"]
  )
}}

with ticket_metrics_bronze as (

    select * from {{ ref('ticket_metrics_bronze') }}

    {% if is_incremental() %}
        where updated_at >= coalesce(
            '{{ var("manual_start_time", none) }}'::timestamp
          , (select max(dt_updated_at) from {{ this }})
        )
    {% endif %}

)

, final as (

    select
        id                             as cd_ticket_metrics_id
      , reopens                        as nr_reopens
      , replies                        as nr_replies
      , solved_at                      as dt_solved_at
      , ticket_id                      as cd_ticket_id
      , created_at                     as dt_created_at
      , assigned_at                    as dt_assigned_at
      , satisfaction_score             as nr_satisfaction_score
      , assignee_updated_at            as nr_assignee_updated_at
      , reply_time_in_minutes          as nr_reply_time_in_minutes
      , on_hold_time_in_minutes        as nr_on_hold_time_in_minutes
      , requester_wait_time_in_minutes as nr_requester_wait_time_in_minutes
      , current_timestamp              as ingested_at
      , {{ snapshot_ts() }}            as snapshot_ts
    from ticket_metrics_bronze
	
)

select *
from final