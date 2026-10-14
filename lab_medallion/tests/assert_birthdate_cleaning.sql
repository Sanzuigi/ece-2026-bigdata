with first_orders as (
    select user_id, min(cast(ordered_at as date)) as first_order_date
    from {{ ref('stg_orders') }} group by user_id
)
select u.user_id, u.birthdate_original, u.birthdate, u.birthdate_is_valid, o.first_order_date
from {{ ref('stg_users') }} u
left join first_orders o on u.user_id = o.user_id
where u.birthdate_is_valid is distinct from
      (u.birthdate_original is not null and (o.first_order_date is null or u.birthdate_original <= o.first_order_date))
   or u.birthdate is distinct from
      (case when o.first_order_date is not null and u.birthdate_original > o.first_order_date
            then null else u.birthdate_original end)
