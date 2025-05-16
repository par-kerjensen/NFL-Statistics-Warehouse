with stadiums as (
    select 'name' from {{ ref('tmp_stadiums_enrichment') }}
),
spreadspoke as (
    select stadium from {{ ref('spreadspoke_scores') }}
),
all_names as (
    select 'name' as original_name from {{ ref('stadiums') }}
    union all
    select stadium as original_name from {{ ref('spreadspoke_scores') }}
),
deduped_names as (
    select distinct original_name from all_names
),
numbered_names as (
    select 
        original_name,
        row_number() over (order by original_name) as universal_stadium_id
    from deduped_names
)

select * from numbered_names
