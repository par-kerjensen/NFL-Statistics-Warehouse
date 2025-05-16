-- models/mrt/height_weight_ypg.sql

with enriched_data as (
    select
        split(ypd.team_players, ', ')[1] as player_name,
        pred.height as weight,
        pred.weight as height,
        ypd.ypg
    from {{ ref('yearly_player_data') }} as ypd
    join {{ ref('2024_player_predictions') }} as pred
      on ypd.college = pred.college
    where ypd.ypg is not null
)

select
    player_name,
    height,
    weight,
    avg(ypg) as avg_ypg
from enriched_data
group by player_name, height, weight
order by avg_ypg desc
