with stg_yearly_team_data as(
    select team,
    season,
    total_snaps,
    yards_gained,
    touchdown as touchdowns,
    extra_point_attempt as extra_point_attempts,
    field_goal_attempt as field_goal_attempts,
    total_points,
    td_points,
    xp_points,
    fg_points,
    fumble,
    fumble_lost,
    shotgun,
    no_huddle,
    qb_dropback,
    pass_snaps_count,
    pass_snaps_pct,
    pass_attempts,
    complete_pass as complete_passes,
    incomplete_pass as incomplete_passes,
    air_yards,
    passing_yards,
    pass_td,
    interception as interceptions,
    targets,
    receptions,
    receiving_yards,
    yards_after_catch,
    receiving_td,
    pass_fumble as pass_fumbles,
    pass_fumble_lost as pass_fumbles_lost,
    rush_snaps_count,
    rush_snaps_pct,
    qb_scramble,
    rushing_yards,
    run_td,
    run_fumble,
    run_fumble_lost,
    home_wins,
    home_losses,
    home_ties,
    away_wins,
    away_losses,
    away_ties,
    wins,
    losses,
    ties,
    win_pct,
    record,
    yps,
    _data_source,
    _load_time
    from {{ source ('football_dataset_raw', 'yearly_team_data')}}
)

select *
from stg_yearly_team_data