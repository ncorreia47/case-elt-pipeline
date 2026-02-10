{{
  config(
    materialized = "table",
    tags = ["gold", "users_gold"]
  )
}}

with users_silver as (

    select * from {{ ref('users_silver') }}

)

, final as (

    select *
    from users_silver
	
)

select *
from final