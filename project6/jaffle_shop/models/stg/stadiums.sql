with stg_stadiums as(
    select stadium_name as name,
    stadium_location as location,
    stadium_open as open,
    stadium_close as close,
    stadium_type as type,
    stadium_address as address,
    stadium_weather_type,
    stadium_capacity,
    stadium_surface,
    stadium_latitude,
    stadium_longitude,
    _data_source,
    _load_time
    from {{ source ('football_dataset_raw', 'stadiums')}}
)

select *
from stg_stadiums