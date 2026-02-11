{{
  config(
    materialized = "table",
    tags = ["bronze", "groups_bronze"]
  )
}}

-- deduplica arquivos json duplicados, por ocasioes de reprocessamentos. Ainda nao aplicamos nenhum tipo de merge/update nos dados
with raw_groups as (

    select distinct * from {{ source('landing', 'api_files_landing') }} afl where afl.endpoint = 'groups'

)

, exploded_json as (

    select
        jsonb_array_elements(payload) as obj
    from raw_groups

)

, final as (

    select
        (obj ->> 'id')::int                            as id
      , obj ->> 'name'                                 as name
      , (obj ->> 'created_at')::timestamp              as created_at
      , (obj ->> 'updated_at')::timestamp              as updated_at
      , obj ->> 'description'                          as description
      , current_timestamp                              as ingested_at
      , {{ snapshot_ts() }}                            as snapshot_ts
    from exploded_json
	
)

select *
from final