with source as (
    select * from {{ source('source', 'categories_cn_en') }}
),

renamed as (
    select
        category_id::int          as category3_id,
        category_name_cn::varchar as category_name_cn,
        category_name_en::varchar as category_name_en,
        category_level::int       as category_level,
        parent_id::int            as category2_id,
        root_id::int              as category1_id
    from source
)

select * from renamed
