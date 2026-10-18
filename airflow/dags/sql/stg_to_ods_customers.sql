-- stg -> ods (customers)
-- isim ve email temizlenip ods'e yaziliyor. kayit zaten varsa guncelleniyor (upsert).

insert into ods.customers (id, name, email, email_domain, is_valid_email, idate, udate, is_deleted, etl_date)
select distinct on (id)                                   -- ayni id iki kez geldiyse en gunceli
       id,
       regexp_replace(trim(name), '\s+', ' ', 'g'),        -- fazla bosluklari sil
       lower(trim(email)),
       split_part(lower(trim(email)), '@', 2),
       lower(trim(email)) ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$',  -- email formati dogru mu
       idate,
       udate,
       false,
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
    is_deleted     = false,
    etl_date       = excluded.etl_date,
    etl_update_ts  = now();

-- kaynakta silinmis kayitlar: ods'te silmiyoruz, is_deleted = true yapiyoruz
update ods.customers
set is_deleted = true, etl_date = %(etl_date)s, etl_update_ts = now()
where is_deleted = false
  and id not in (select id from stg.customers_keys)
  and exists (select 1 from stg.customers_keys);   -- id listesi bos geldiyse (hata) hicbir seyi silme
