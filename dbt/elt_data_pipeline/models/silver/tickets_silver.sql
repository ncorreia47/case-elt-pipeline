{{
  config(
    materialized = "table",
    tags = ["silver", "tickets_silver]
  )
}}

with tickets_bronze as (

    select * from {{ ref('tickets_bronze') }}

)

, final as (

    select
        id                   as cd_ticket_id
      , url                  as ds_url
      , upper(type)          as ds_type
      , public               as is_public
      , upper(status)        as ds_status
      , upper(channel)       as ds_channel
      , group_id             as cd_group_id
      , upper(priority)      as ds_priority
      , assignee_id          as cd_assignee_id
      , description          as ds_description
      , organization_id      as cd_organization_id
      , created_at           as dt_created_at
      , updated_at           as dt_updated_at
      , current_timestamp    as ingested_at
      , {{ snapshot_ts() }}  as snapshot_ts
    from tickets_bronze
	
)

select *
from final