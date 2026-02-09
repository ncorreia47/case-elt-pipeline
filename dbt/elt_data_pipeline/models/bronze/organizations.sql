{{
  config(
    materialized = "table",
    tags = ["bronze", "organizations"]
  )
}}

with raw_organizations as (

    select * from {{ source('landing', 'api_files_landing') }} afl where afl.endpoint = 'organizations'

)

, exploded_json as (

    select
        jsonb_array_elements(payload) as obj
    from raw_organizations

)

, final as (

    select
        (obj ->> 'id')::int                            as user_id
      , obj ->> 'name'                                 as name
      , (obj ->> 'created_at')::timestamp              as created_at
      , (obj ->> 'updated_at')::timestamp              as updated_at
      , obj ->> 'external_id'                          as external_id
      , current_timestamp                              as ingested_at
      , {{ snapshot_ts() }}                            as snapshot_ts
    from exploded_json
	
)

select *
from final