{{ config(
    post_hook = "delete from {{ this }} where team not in (select name from {{ref('team_conference')}})"
)}}


with int_yearly_team_data as(
    select * from {{ref('yearly_team_data')}}
)

select * 
from int_yearly_team_data