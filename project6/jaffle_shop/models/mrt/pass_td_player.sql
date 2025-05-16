-- models/mrt/player_pass_touchdowns.sql

with base as (
    select
        -- split into [team_id, player_name]
        split(team_players, ', ')[1] as player_name,
        pass_td
    from {{ ref('yearly_player_data') }}
    where pass_td is not null
)

select
    player_name,
    sum(pass_td) as total_pass_touchdowns
from base
group by player_name
order by total_pass_touchdowns desc
