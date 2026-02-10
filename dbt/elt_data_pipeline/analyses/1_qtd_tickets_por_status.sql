select 
    ds_status
  , count(1)
from elt_data_pipeline_silver.tickets_silver
group by 1 
order by 2 desc