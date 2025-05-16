{{ config(
    post_hook = "delete from {{ this }} where team not in (select name from {{ref('team_conference')}})"
)}}

with int_2024_weekly_stats as(
    select * from {{ref('2024_weekly_stats')}}
)

select *
from int_2024_weekly_stats