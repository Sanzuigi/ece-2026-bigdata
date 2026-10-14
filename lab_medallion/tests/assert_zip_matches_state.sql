{{ config(severity='warn', store_failures=true) }}
-- Faker assigns ZIP codes independently of states. Keep these failures visible.
-- A missing reference is distinguished from a known ZIP assigned to another state.
select u.user_id, u.state, u.zip_code,
       case when not exists (select 1 from {{ ref('zip_states') }} z where z.zip_code=u.zip_code)
            then 'unknown_zip_in_reference' else 'state_zip_mismatch' end as issue
from {{ ref('stg_users') }} u
where u.state not in ('AA','AE','AP')
  and not exists (
      select 1 from {{ ref('zip_states') }} z
      where z.zip_code = u.zip_code and z.state = u.state
  )
