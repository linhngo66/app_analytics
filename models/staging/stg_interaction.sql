with source as (
    select * from {{ source('source', 'interaction') }}
),

renamed as (
    select
        -- keys
        user_id::bigint                              as user_id,
        pid::varchar                                 as video_id,
        author_id::bigint                            as author_id,
        category_id::int                             as category_id,
        category_level::int                          as category_level,
        parent_id::int                               as parent_id,
        root_id::int                                 as root_id,

        -- timestamps
        to_timestamp(exposed_time::bigint)           as exposed_at,
        p_hour::int                                  as p_hour,
        to_date(p_date::varchar, 'YYYYMMDD')         as p_date,

        -- video attributes
        watch_time::float                            as watch_time_seconds,
        duration::float                              as duration_seconds,
        title::varchar                               as title,
        tag_name::varchar                            as tag_name,

        -- author attributes
        author_fans_count::int                       as author_fans_count,

        -- interaction flags
        cvm_like::boolean                            as is_liked,
        click::boolean                               as is_clicked,
        comment::boolean                             as is_commented,
        follow::boolean                              as is_followed,
        collect::boolean                             as is_collected,
        forward::boolean                             as is_forwarded,
        hate::boolean                                as is_hated,

        -- user attributes
        gender::varchar                              as gender,
        age::int                                     as age,
        mod_price::float                             as mod_price,
        fre_city::varchar                            as fre_city,
        fre_community_type::varchar                  as fre_community_type,
        fre_city_level::varchar                      as fre_city_level,

        -- derived
        (watch_time::float > 3)                      as is_effective_view

    from source
)

select * from renamed
