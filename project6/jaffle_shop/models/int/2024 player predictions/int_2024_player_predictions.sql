{{ config(
    post_hook = "delete from {{ this }} where team not in (select name from {{ref('team_conference')}})"
)}}

with int_2024_player_predictions as(
    select * from {{ref('2024_player_predictions')}}
)

select * 
from int_2024_player_predictions