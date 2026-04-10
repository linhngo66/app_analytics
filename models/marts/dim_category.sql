with stg_category as (
    select * from {{ ref('stg_category') }}
),

level1_names as (
    select category1_id, category_name_en as category1_name_en
    from stg_category where category_level = 1
),

level2_names as (
    select category3_id as category2_id, category_name_en as category2_name_en
    from stg_category where category_level = 2
),

unioned as (
    -- Level 1: only root name
    select
        category_level,
        category1_id,
        category_name_en  as category1_name_en,
        null::int         as category2_id,
        null::varchar     as category2_name_en,
        null::int         as category3_id,
        null::varchar     as category3_name_en
    from stg_category where category_level = 1

    union all

    -- Level 2: root + self names
    select
        sc.category_level,
        sc.category1_id,
        l1.category1_name_en,
        sc.category3_id   as category2_id,
        sc.category_name_en as category2_name_en,
        null::int         as category3_id,
        null::varchar     as category3_name_en
    from stg_category sc
    left join level1_names l1 on sc.category1_id = l1.category1_id
    where sc.category_level = 2

    union all

    -- Level 3: all three levels resolved
    select
        sc.category_level,
        sc.category1_id,
        l1.category1_name_en,
        sc.category2_id,
        l2.category2_name_en,
        sc.category3_id,
        sc.category_name_en as category3_name_en
    from stg_category sc
    left join level1_names l1 on sc.category1_id = l1.category1_id
    left join level2_names l2 on sc.category2_id = l2.category2_id
    where sc.category_level = 3
),

final as (
    select
        concat(
            coalesce(category1_id, 0),
            coalesce(category2_id, 0),
            coalesce(category3_id, 0)) as category_sk,
        category_level,
        category1_id,
        category1_name_en,
        category2_id,
        category2_name_en,
        category3_id,
        category3_name_en
    from unioned
)

select * from final
