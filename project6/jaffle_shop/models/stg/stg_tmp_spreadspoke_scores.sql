with stg_spreadspoke_scores as(
    select distinct * except (_load_time)
    from {{ source ('football_dataset_raw', 'spreadspoke_scores')}}
)

select *
from stg_spreadspoke_scores