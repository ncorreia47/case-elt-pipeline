{{
  config(
    materialized = "table",
    tags = ["silver", "groups_silver"]
  )
}}

with groups_bronze as (

    select * from {{ ref('groups_bronze') }}

)

, final as (

    select
        id                  as cd_group_id
      , upper(name)         as ds_group_name
      , created_at          as dt_created_at
      , updated_at          as dt_updated_at
      , upper(description)  as ds_description
      , current_timestamp   as ingested_at
      , {{ snapshot_ts() }} as snapshot_ts
    from groups_bronze
	
)

select *
from final