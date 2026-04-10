with stg_category as (
    select * from {{ ref('stg_category') }}
)

select
    category_sk,
    category_level,
    category1_id,
    category2_id,
    category3_id,
    category_name_en
from stg_category
