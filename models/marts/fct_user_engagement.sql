with fct_interaction as (
    select * from {{ ref('fct_interaction') }}
),

dim_user as (
    select * from {{ ref('dim_user') }}
),

aggregated as (
    select
        user_id,

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
    group by user_id
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
        div0(total_watch_time, views)  as avg_watch_time_per_view
    from with_engagements
),

with_user as (
    select
        r.*,
        u.gender,
        u.age,
        u.mod_price,
        u.fre_city,
        u.fre_community_type,
        u.fre_city_level,
        u.last_active_at
    from with_rates r
    left join dim_user u on r.user_id = u.user_id
)

select * from with_user
