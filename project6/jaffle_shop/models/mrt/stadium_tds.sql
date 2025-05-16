-- models/mrt/stadium_touchdowns.sql

select
    s.name,
    sum(ss.score_home + ss.score_away) as total_score
from {{ ref('spreadspoke_scores') }} as ss
join {{ ref('stadiums') }} as s
  on ss.stadium = s.name
group by
    s.name
order by
    total_score desc
