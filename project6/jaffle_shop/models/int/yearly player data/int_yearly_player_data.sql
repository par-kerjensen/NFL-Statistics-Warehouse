with int_yearly_player_data as(
    select * from {{ref('yearly_player_data')}}
)

select * 
from int_yearly_player_data