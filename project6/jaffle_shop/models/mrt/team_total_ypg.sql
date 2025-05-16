-- models/mrt/team_total_ypg.sql

select
    split(ypd.team_players, ', ')[0] as team,
    avg(ypd.ypg) as avg_total_ypg
from {{ ref('yearly_player_data') }} as ypd
where ypd.ypg is not null
group by team
order by avg_total_ypg desc
