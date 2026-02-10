{{
  config(
    materialized = "table",
    tags = ["gold", "groups_gold"]
  )
}}

with groups_silver as (

    select * from {{ ref('groups_silver') }}

)

, final as (

    select *
    from groups_silver
	
)

select *
from final