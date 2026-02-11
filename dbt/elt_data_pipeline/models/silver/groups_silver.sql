{{
  config(
    materialized = "incremental",
    unique_key = "cd_group_id",
    tags = ["silver", "groups_silver"]
  )
}}

with groups_bronze as (

    select * from {{ ref('groups_bronze') }}

    {% if is_incremental() %}
        where updated_at >= coalesce(
            '{{ var("manual_start_time", none) }}'::timestamp
          , (select max(dt_updated_at) from {{ this }})
        )
    {% endif %}

)

, final as (
    select
        id                  as cd_group_id
      , upper(name)         as ds_group_name
      , created_at          as dt_created_at
      , updated_at          as dt_updated_at
      , current_timestamp   as ingested_at
    from groups_bronze
)

select * 
from final