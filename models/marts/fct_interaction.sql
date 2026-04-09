with stg_interaction as (
    select * from {{ ref('stg_interaction') }}
)

select
    user_id,
    video_id,
    author_id,
    category_id,
    category_level,
    parent_id,
    root_id,
    exposed_at,
    watch_time_seconds,
    is_effective_view,
    is_liked,
    is_clicked,
    is_commented,
    is_followed,
    is_collected,
    is_forwarded,
    is_hated,
    p_hour,
    p_date
from stg_interaction
