with stg_interaction as (
    select * from {{ ref('stg_interaction') }}
),

-- one row per video using the most recent exposure record
deduped as (
    select
        video_id,
        author_id,
        category_id,
        category_level,
        parent_id,
        root_id,
        duration_seconds,
        title,
        row_number() over (
            partition by video_id
            order by exposed_at desc
        ) as rn
    from stg_interaction
)

select
    video_id,
    author_id,
    category_id,
    category_level,
    parent_id,
    root_id,
    duration_seconds,
    title
from deduped
where rn = 1
