select 
    ds_organization_name
  , count(1)
from elt_data_pipeline_silver.tickets_silver ts 
left join elt_data_pipeline_silver.organizations_silver os 
on ts.cd_organization_id = os.cd_organization_id
where 1=1 
and ds_priority  = 'URGENT'
group by 1 
order by 2 desc