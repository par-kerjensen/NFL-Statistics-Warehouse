-- models/mrt/player_conference_rating.sql

select
    tc.conference,
    split(ypd.team_players, ', ')[1] as player_name,
    split(ypd.team_players, ', ')[0] as team,
    avg(ypd.pass_ypg) as avg_pass_ypg,
    avg(ypd.rush_ypg) as avg_rush_ypg
from {{ ref('yearly_player_data') }} as ypd
join {{ ref('team_identification') }} as ti
    on split(ypd.team_players, ', ')[0] = ti.id 
join {{ ref('team_conference') }} as tc
    on ti.name = tc.name
group by
    tc.conference,
    player_name,
    team
order by
    avg_pass_ypg desc,
    avg_rush_ypg desc
