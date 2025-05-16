-- models/int/teams/team_identification.sql

select distinct
    name,
    name_short,
    id,
    id_pfr,
    _data_source,
    _load_time
from {{ ref('teams') }}
