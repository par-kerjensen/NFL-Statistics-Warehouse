with int_tmp_stadiums as (
    select distinct name,
    location
    from {{ ref('stadiums') }}
)

select * 
from int_tmp_stadiums