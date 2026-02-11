{{
  config(
    materialized = "table",
    tags = ["bronze", "ticket_custom_fields_bronze"]
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
        (obj ->> 'id')::int                  as ticket_id
      , (field ->> 'id')::int                as id
      , field ->> 'value'                    as value
      , current_timestamp                    as ingested_at
      , {{ snapshot_ts() }}                  as snapshot_ts
    from exploded_json,
    lateral jsonb_array_elements(obj -> 'custom_fields') as field
	
)

select *
from final