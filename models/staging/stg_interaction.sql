with source as (
    select * from {{ source('source', 'interaction') }}
),

renamed as (
    select
        -- keys
        cast("user_id" as bigint) as user_id,
        cast("pid" as varchar) as video_id,
        cast("author_id" as bigint) as author_id,
        cast("category_level" as int) as category_level,
        cast("root_id" as int) as category1_id,
        cast("parent_id" as int) as category2_id,
        cast("category_id" as int) as category3_id,

        -- timestamps
        to_timestamp(cast("exposed_time" as bigint)) as exposed_at,
        cast("p_hour" as int) as p_hour,
        to_date(cast("p_date" as varchar), 'YYYYMMDD') as p_date,

        -- video attributes
        cast("watch_time" as float) as watch_time_seconds,
        cast("duration" as float) as duration_seconds,
        cast("title" as varchar) as title,
        cast("tag_name" as varchar) as tag_name,

        -- author attributes
        cast("author_fans_count" as int) as author_fans_count,

        -- interaction flags
        cast("cvm_like" as boolean) as is_liked,
        cast("click" as boolean) as is_clicked,
        cast("comment" as boolean) as is_commented,
        cast("follow" as boolean) as is_followed,
        cast("collect" as boolean) as is_collected,
        cast("forward" as boolean) as is_forwarded,
        cast("hate" as boolean) as is_hated,

        -- user attributes
        cast("gender" as varchar) as gender,
        cast("age" as int) as age,
        cast("mod_price" as float) as mod_price,
        cast("fre_city" as varchar) as fre_city,
        cast("fre_community_type" as varchar) as fre_community_type,
        cast("fre_city_level" as varchar) as fre_city_level,

        -- derived
        iff(cast("watch_time" as float) > 3, true, false) as is_effective_view

    from source
)

select * from renamed
