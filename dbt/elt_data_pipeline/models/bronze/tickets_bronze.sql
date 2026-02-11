{{
  config(
    materialized = "table",
    tags = ["bronze", "tickets_bronze"]
  )
}}

-- deduplica arquivos json duplicados, por ocasioes de reprocessamentos. Ainda nao aplicamos nenhum tipo de merge/update nos dados
with raw_tickets as (

    select distinct * from {{ source('landing', 'api_files_landing') }} afl where afl.endpoint = 'tickets'

)

, exploded_json as (

    select
        jsonb_array_elements(payload) as obj
    from raw_tickets

)

, final as (

    select
        (obj ->> 'id')::int                  as id
      , obj ->> 'url'                        as url
      , obj ->> 'type'                       as type
      , (obj ->> 'public')::boolean          as public
      , obj ->> 'status'                     as status
      , obj ->> 'channel'                    as channel
      , (obj ->> 'group_id')::int            as group_id
      , obj ->> 'priority'                   as priority
      , (obj ->> 'assignee_id')::int         as assignee_id
      , obj ->> 'description'                as description
      , (obj ->> 'organization_id')::int     as organization_id
      , (obj ->> 'created_at')::timestamp    as created_at
      , (obj ->> 'updated_at')::timestamp    as updated_at
      , current_timestamp                    as ingested_at
      , {{ snapshot_ts() }}                  as snapshot_ts
    from exploded_json
	
)

select *
from final