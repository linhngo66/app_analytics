with stg_category as (
    select * from {{ ref('stg_category') }}
)

select
    category3_id as category_id,
    category_name_en,
    category_name_cn,
    category_level,
    category2_id as parent_id,
    category1_id as root_id
from stg_category
