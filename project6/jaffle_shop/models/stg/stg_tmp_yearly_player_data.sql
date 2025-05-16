with stg_yearly_player_data as(
    select distinct *
    from {{ source ('football_dataset_raw', 'yearly_player_data')}}
)

select *
from stg_yearly_player_data