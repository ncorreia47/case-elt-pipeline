{{
  config(
    materialized = "table",
    tags = ["silver", "users_silver"]
  )
}}

with users_bronze as (

    select * from {{ ref('users_bronze') }}

)

, final as (

    select
        id                   as cd_user_id
      , upper(name)          as ds_user_name
      , email                as ds_email
      , active               as is_active
      , created_at           as dt_created_at
      , updated_at           as dt_updated_at
      , current_timestamp    as ingested_at
      , {{ snapshot_ts() }}  as snapshot_ts
    from users_bronze
	
)

select *
from final