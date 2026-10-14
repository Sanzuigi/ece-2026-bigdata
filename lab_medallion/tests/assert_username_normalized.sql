select user_id, username, username_normalized
from {{ ref('stg_users') }}
where username_normalized is null or username_normalized != lower(trim(username))
