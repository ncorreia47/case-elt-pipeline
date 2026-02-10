{{
  config(
    materialized = "table",
    tags = ["gold", "organizations_gold"]
  )
}}

with organizations_silver as (

    select * from {{ ref('organizations_silver') }}

)

, final as (

    select *
    from organizations_silver
	
)

select *
from final