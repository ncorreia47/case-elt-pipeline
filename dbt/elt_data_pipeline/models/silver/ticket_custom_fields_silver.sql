{{
  config(
    materialized = "table",
    tags = ["silver", "ticket_custom_fields_silver"]
  )
}}

with ticket_custom_fields_bronze as (

    select * from {{ ref('ticket_custom_fields_bronze') }}

)

, final as (

    select
        ticket_id           as cd_ticket_id
      , id                  as cd_ticket_custom_field_id
      , value               as ds_ticket_custom_field
      , current_timestamp   as ingested_at
      , {{ snapshot_ts() }} as snapshot_ts
    from ticket_custom_fields_bronze
	
)

select *
from final