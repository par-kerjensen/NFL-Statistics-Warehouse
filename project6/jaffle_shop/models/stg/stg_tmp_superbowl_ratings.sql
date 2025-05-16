with stg_superbowl_ratings as(
    select distinct * except(_load_time)
    from {{ source ('football_dataset_raw', 'superbowl_ratings')}}
)

select *
from stg_superbowl_ratings