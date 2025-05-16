with stadiums as (
    select
        *,
        stadium as old_stadium_name  -- preserve for join
    from {{ ref('spreadspoke_scores') }}
),
mapping as (
    select * from {{ ref('tmp_stad_mapping') }}
),
joined as (
    select 
        s.* except (stadium),
        m.universal_stadium_id,
        m.original_name as stadium_name
    from stadiums s
    left join mapping m 
    on s.old_stadium_name = m.original_name
)

select * from joined
