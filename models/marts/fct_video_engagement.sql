with fct_interaction as (
    select * from {{ ref('fct_interaction') }}
),

aggregated as (
    select
        video_id,

        -- carry through video-level attributes (constant per video)
        any_value(author_id)        as author_id,
        any_value(category_id)      as category_id,
        any_value(category_level)   as category_level,
        any_value(parent_id)        as parent_id,
        any_value(root_id)          as root_id,

        -- volume metrics
        count(*)                    as views,
        count_if(is_effective_view) as effective_views,
        sum(watch_time_seconds)     as total_watch_time,

        -- interaction counts
        count_if(is_liked)          as likes,
        count_if(is_commented)      as comments,
        count_if(is_forwarded)      as shares,
        count_if(is_followed)       as follows,
        count_if(is_collected)      as collects,
        count_if(is_hated)          as hates

    from fct_interaction
    group by video_id
),

with_engagements as (
    select
        *,
        likes + comments + shares + follows + collects + hates as total_engagements
    from aggregated
),

with_rates as (
    select
        *,
        div0(total_engagements, views) as engagement_rate,
        div0(effective_views, views)   as effective_view_rate,
        div0(total_watch_time, views)  as avg_watch_time_per_view,
        div0(likes, views)             as like_rate,
        div0(comments, views)          as comment_rate,
        div0(shares, views)            as share_rate
    from with_engagements
)

select * from with_rates
