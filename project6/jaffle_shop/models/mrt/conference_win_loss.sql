-- models/mrt/conference_win_loss.sql

select
    tc.conference,
    tc.name,
    count(case when ss.score_home > ss.score_away then 1 end) as home_wins,
    count(case when ss.score_home < ss.score_away then 1 end) as home_losses
from {{ ref('int_spreadspoke_scores') }} as ss
join {{ ref('team_conference') }} as tc
  on ss.team_home = tc.name
group by
    tc.conference,
    tc.name
