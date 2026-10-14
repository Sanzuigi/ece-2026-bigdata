select o.order_id, o.ordered_at, u.user_id, u.birthdate
from "lab_medallion"."main_silver"."stg_orders" o
join "lab_medallion"."main_silver"."stg_users" u on o.user_id = u.user_id
where o.ordered_at < u.birthdate
