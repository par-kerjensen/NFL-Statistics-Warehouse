-- models/mrt/home_field_advantage_by_team.sql

select
    s.stadium_name,
    ss.team_home as team,
    count(case when ss.score_home > ss.score_away then 1 end) as home_wins,
    count(case when ss.score_home < ss.score_away then 1 end) as home_losses
from {{ ref('int_spreadspoke_scores') }} as ss
join {{ ref('tmp_stad_updated') }} as s
  on ss.stadium_name = s.stadium_name
group by
    s.stadium_name,
    ss.team_home
