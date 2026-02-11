{{
  config(
    materialized = "table",
    tags = ["bronze", "users_bronze"]
  )
}}

-- deduplica arquivos json duplicados, por ocasioes de reprocessamentos. Ainda nao aplicamos nenhum tipo de merge/update nos dados
with raw_users as (

    select distinct * from {{ source('landing', 'api_files_landing') }} afl where afl.endpoint = 'users'

)

, exploded_json as (

    select
        jsonb_array_elements(payload) as obj
    from raw_users

)

, final as (

    select
        (obj ->> 'id')::int                            as id
      , obj ->> 'name'                                 as name
      , obj ->> 'email'                                as email
      , (obj ->> 'active')::boolean                    as active
      , (obj ->> 'created_at')::timestamp              as created_at
      , (obj ->> 'updated_at')::timestamp              as updated_at
      , current_timestamp                              as ingested_at
      , {{ snapshot_ts() }}                            as snapshot_ts
    from exploded_json
	
)

select *
from final