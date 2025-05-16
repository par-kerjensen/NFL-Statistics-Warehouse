-- models/int/teams/team_conference.sql

select distinct
    name,
    conference,
    conference_pre_2002,
    division_pre_2002,
    _data_source,
    _load_time
from {{ ref('teams') }}
