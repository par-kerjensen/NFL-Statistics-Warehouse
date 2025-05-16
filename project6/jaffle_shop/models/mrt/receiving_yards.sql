-- models/mrt/player_pass_touchdowns.sql

with base as (
    select
        -- split into [team_id, player_name]
        split(team_players, ', ')[1] as player_name,
        receiving_yards
    from {{ ref('yearly_player_data') }}
    where receiving_yards is not null
)

select
    player_name,
    sum(receiving_yards) as total_receiving_yards
from base
group by player_name
order by total_receiving_yards desc
