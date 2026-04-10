with fct_interaction as (
    select * from {{ ref('fct_interaction') }}
),

dim_category as (
    select * from {{ ref('dim_category') }}
),

aggregated as (
    select
        video_id,
        any_value(author_id) as author_id,
        any_value(category_sk) as category_sk,

        -- volume metrics
        count(*) as views,
        sum(effective_view_count) as effective_views,
        sum(watch_time_seconds) as total_watch_time,

        -- interaction counts
        count_if(is_liked) as likes,
        count_if(is_commented) as comments,
        count_if(is_forwarded) as shares,
        count_if(is_followed) as follows,
        count_if(is_collected) as collects,
        count_if(is_hated) as hates

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
        div0(effective_views, views) as effective_view_rate,
        div0(total_watch_time, views) as avg_watch_time_per_view,
        div0(likes, views) as like_rate,
        div0(comments, views) as comment_rate,
        div0(shares, views) as share_rate
    from with_engagements
),

final as (
    select
        r.video_id,
        r.author_id,
        r.category_sk,
        dc.category1_name_en as category1_name,
        dc.category2_name_en as category2_name,
        dc.category3_name_en as category3_name,
        r.views,
        r.effective_views,
        r.total_watch_time,
        r.likes,
        r.comments,
        r.shares,
        r.follows,
        r.collects,
        r.hates,
        r.total_engagements,
        r.engagement_rate,
        r.effective_view_rate,
        r.avg_watch_time_per_view,
        r.like_rate,
        r.comment_rate,
        r.share_rate
    from with_rates r
    left join dim_category dc on r.category_sk = dc.category_sk
)

select * from final
