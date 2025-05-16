with stadiums as (
    select * from {{ ref('tmp_stadiums_enrichment') }}
),
mapping as (
    select * from {{ ref('tmp_stad_mapping') }}
),
joined as (
    select 
        s.* except (name),
        m.universal_stadium_id,
        m.original_name as stadium_name
    from stadiums s
    left join mapping m 
    on s.name = m.original_name
)

select * from joined

