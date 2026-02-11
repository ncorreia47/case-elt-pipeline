{{
  config(
    materialized = "incremental",
    unique_key = "cd_organization_id",
    tags = ["silver", "organizations_silver"]
  )
}}

with organizations_bronze as (

    select * from {{ ref('organizations_bronze') }}

    {% if is_incremental() %}
        where updated_at >= coalesce(
            '{{ var("manual_start_time", none) }}'::timestamp
          , (select max(dt_updated_at) from {{ this }})
        )
    {% endif %}

)

, final as (

    select
        id                  as cd_organization_id
      , upper(name)         as ds_organization_name
      , external_id         as cd_external_id
      , created_at          as dt_created_at
      , updated_at          as dt_updated_at
      , current_timestamp   as ingested_at
      , {{ snapshot_ts() }} as snapshot_ts
    from organizations_bronze
	
)

select *
from final