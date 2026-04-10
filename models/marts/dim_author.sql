with stg_interaction as (
    select * from {{ ref('stg_interaction') }}
),

-- one row per author using the most recent interaction to get latest fans count
deduped as (
    select
        author_id,
        author_fans_count,
        exposed_at,
        row_number() over (
            partition by author_id
            order by exposed_at desc
        ) as rn
    from stg_interaction
)

select
    author_id,
    author_fans_count,
    exposed_at as updated_at
from deduped
where rn = 1
