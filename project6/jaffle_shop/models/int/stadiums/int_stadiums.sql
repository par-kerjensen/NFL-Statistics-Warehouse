with int_stadiums as (
    select distinct updated.*,
    new_fields.* except(name)
    from {{ref('stadium_new_fields')}} new_fields 
    join {{ref('tmp_stad_updated')}} updated
    on new_fields.name = updated.stadium_name
)

select *
from int_stadiums