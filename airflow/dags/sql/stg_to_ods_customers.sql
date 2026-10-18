-- stg -> ods (customers)
-- isim ve email temizlenip ods'e yaziliyor. kayit zaten varsa guncelleniyor (upsert).
-- yazilan her kaydin etl_date'i bugun oluyor, dwh bu kayitlari aliyor.

insert into ods.customers (id, name, email, email_domain, is_valid_email, idate, udate, etl_date)
select distinct on (id)                                   -- ayni id iki kez geldiyse en gunceli
       id,
       regexp_replace(trim(name), '\s+', ' ', 'g'),        -- fazla bosluklari sil
       lower(trim(email)),
       split_part(lower(trim(email)), '@', 2),
       lower(trim(email)) ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$',  -- email formati dogru mu
       idate,
       udate,
       %(etl_date)s
from stg.customers
order by id, udate desc
on conflict (id) do update set
    name           = excluded.name,
    email          = excluded.email,
    email_domain   = excluded.email_domain,
    is_valid_email = excluded.is_valid_email,
    idate          = excluded.idate,
    udate          = excluded.udate,
    etl_date       = excluded.etl_date,
    etl_update_ts  = now();
