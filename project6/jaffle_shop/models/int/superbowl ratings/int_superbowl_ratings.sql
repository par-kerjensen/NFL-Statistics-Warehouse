with int_superbowl_ratings as (
    select * from {{ref('superbowl_ratings')}}
)

select * 
from int_superbowl_ratings