with source as (
    select * from {{ source('bronze', 'users') }}
), typed as (
    select cast(uuid as uuid) as user_id,
           trim(username) as username, trim(name) as name,
           upper(trim(sex)) as sex, lower(trim(mail)) as email,
           cast(birthdate as date) as birthdate_original,
           split_part(address, chr(10), 1) as street,
           split_part(address, chr(10), 2) as address_line_2
    from source
)
select t.user_id, username, lower(trim(username)) as username_normalized,
       name, sex, email, birthdate_original,
       case when first_order_at is not null and birthdate_original > cast(first_order_at as date)
            then null else birthdate_original end as birthdate,
       birthdate_original is not null and
           (first_order_at is null or birthdate_original <= cast(first_order_at as date)) as birthdate_is_valid,
       street,
       nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
       regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
       regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed t
left join (
    select user_id, min(ordered_at) as first_order_at
    from {{ ref('stg_orders') }}
    group by user_id
) first_orders on t.user_id = first_orders.user_id
