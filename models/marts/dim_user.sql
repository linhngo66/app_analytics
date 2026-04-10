with stg_interaction as (
    select * from {{ ref('stg_interaction') }}
),

-- one row per user using the most recent interaction to get latest demographic snapshot
deduped as (
    select
        user_id,
        gender,
        age,
        mod_price,
        fre_city,
        fre_community_type,
        fre_city_level,
        exposed_at,
        row_number() over (
            partition by user_id
            order by exposed_at desc
        ) as rn
    from stg_interaction
)

select
    user_id,
    gender,
    age,
    mod_price,
    fre_city,
    fre_community_type,
    fre_city_level,
    exposed_at as last_active_at
from deduped
where rn = 1
