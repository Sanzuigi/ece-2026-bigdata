

  create view "lab_medallion"."main_silver"."stg_users__dbt_tmp" as (
    with source as (
    select * from read_csv('s3://user-r-homsi-ece/bronze/users.csv', strict_mode=false)
), typed as (
    select cast(uuid as uuid) as user_id,
           trim(username) as username, trim(name) as name,
           upper(trim(sex)) as sex, lower(trim(mail)) as email,
           cast(birthdate as date) as birthdate,
           split_part(address, chr(10), 1) as street,
           split_part(address, chr(10), 2) as address_line_2
    from source
)
select user_id, username, name, sex, email, birthdate, street,
       nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
       regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
       regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed
  );
