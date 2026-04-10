with src as (
    select * from {{ source('source', 'categories_cn_en') }}
),

renamed as (
    select
        cast("_line" as int)                as _line,
        cast("category_level" as int)       as category_level,
        cast("root_id" as int)              as category1_id,
        cast("parent_id" as int)            as category2_id,
        cast("category_id" as int)          as category3_id,
        cast("category_name_cn" as varchar) as category_name_cn,
        cast("category_name_en" as varchar) as category_name_en
    from src
),

-- Workaround: 6 level-2 category_ids have duplicate rows with conflicting
-- category_name_en (source data quality issue — see __sources.yml for details).
-- For most duplicates the lower _line has the correct translation, except
-- category3_id 239 (舞蹈教学) and 354 (仿妆) where the higher _line is correct.
-- TODO: remove once corrected at source.
ranked as (
    select *,
        row_number() over (
            partition by category3_id
            order by
                case when category3_id in (239, 354) then _line * -1 else _line end
        ) as rn
    from renamed
),

deduped as (
    select * exclude (rn) from ranked where rn = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['category3_id', 'category2_id', 'category1_id']) }} as category_sk,
        category_level,
        category1_id,
        category2_id,
        category3_id,
        category_name_cn,
        category_name_en
    from deduped
)

select * from final
