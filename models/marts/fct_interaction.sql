with stg_interaction as (
    select * from {{ ref('stg_interaction') }}
)

select
    user_id,
    video_id,
    author_id,
    category_level,
    category1_id,
    category2_id,
    category3_id,
    exposed_at,
    p_hour,
    p_date,
    watch_time_seconds,
    is_effective_view,
    is_liked,
    is_clicked,
    is_commented,
    is_followed,
    is_collected,
    is_forwarded,
    is_hated
from stg_interaction
