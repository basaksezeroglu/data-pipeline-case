-- ods -> dwh (dim_customer)  -- delete + insert
-- load_from = son basarili run'in tarihi. ods'te bu tarihten beri degisen musteriler
-- dwh'tan silinip tekrar ekleniyor. silinmis (is_deleted) olanlar geri eklenmiyor.

delete from dwh.dim_customer d
using ods.customers o
where d.customer_id = o.id
  and o.etl_date >= %(load_from)s;

insert into dwh.dim_customer (customer_id, customer_name, email, email_domain, created_at, updated_at, etl_date)
select id, name, email, email_domain, idate, udate, %(etl_date)s
from ods.customers
where etl_date >= %(load_from)s
  and is_deleted = false;
