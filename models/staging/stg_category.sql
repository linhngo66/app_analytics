with source as (
    select * from {{ source('source', 'categories_cn_en') }}
),

renamed as (
    select
        cast("category_level" as int) as category_level,
        cast("root_id" as int) as category1_id,
        cast("parent_id" as int) as category2_id,
        cast("category_id" as int) as category3_id,
        cast("category_name_cn" as varchar) as category_name_cn,
        cast("category_name_en" as varchar) as category_name_en
    from source
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['category3_id']) }} as category_sk,
        category_level,
        category1_id,
        category2_id,
        category3_id,
        category_name_cn,
        category_name_en
    from renamed
)

select * from final
