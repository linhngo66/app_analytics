with stg_interaction as (
    select * from {{ ref('stg_interaction') }}
)

select
    -- keys
    user_id,
    video_id,
    author_id,
    category_sk,

    -- timestamps
    exposed_at,
    p_hour,
    p_date,

    -- watch metrics
    watch_time_seconds,
    is_effective_view,

    -- interaction flags
    is_liked,
    is_clicked,
    is_commented,
    is_followed,
    is_collected,
    is_forwarded,
    is_hated
from stg_interaction
