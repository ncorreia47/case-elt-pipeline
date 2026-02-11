{{
  config(
    materialized = "incremental",
    unique_key= ["cd_ticket_custom_field_id", "cd_ticket_id"],
    tags = ["silver", "ticket_custom_fields_silver"]
  )
}}

with ticket_custom_fields_bronze as (

    select * from {{ ref('ticket_custom_fields_bronze') }}

    {% if is_incremental() %}
        where updated_at >= coalesce(
            '{{ var("manual_start_time", none) }}'::timestamp
          , (select max(dt_updated_at) from {{ this }})
        )
    {% endif %}

)

, final as (

    select
        ticket_id           as cd_ticket_id
      , id                  as cd_ticket_custom_field_id
      , upper(value)        as ds_ticket_custom_field
      , current_timestamp   as ingested_at
      , {{ snapshot_ts() }} as snapshot_ts
    from ticket_custom_fields_bronze
	
)

select *
from final