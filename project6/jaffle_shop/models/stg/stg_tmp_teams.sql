with stg_teams as(
    select distinct team_name as name,
    team_name_short as name_short,
    team_id as id,
    team_id_pfr as id_pfr,
    team_conference as conference,
    team_division as division,
    team_conference_pre_2002 as conference_pre_2002,
    team_division_pre2002 as division_pre_2002,
    _data_source
    from {{ source ('football_dataset_raw', 'teams')}}
)

select *
from stg_teams