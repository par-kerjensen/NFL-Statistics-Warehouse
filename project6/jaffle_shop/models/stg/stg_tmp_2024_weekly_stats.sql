with stg_2024_weekly_stats as(
    select distinct team,
    SAFE_CAST(SPLIT(games_w_l_t, '-')[ORDINAL(1)] AS INT64) AS total_win,
    SAFE_CAST(SPLIT(games_w_l_t, '-')[ORDINAL(2)] AS INT64) AS total_lost,
    SAFE_CAST(SPLIT(games_w_l_t, '-')[ORDINAL(3)] AS INT64) AS total_tie,
    first_downs,
    rushing_yds,
    passing_yds,
    penalties,
    yds_gained,
    avg_yds_game,
    times_sacked,
    _data_source,
    from {{ source ('football_dataset_raw', '2024_weekly_stats')}}
)

select *
from stg_2024_weekly_stats