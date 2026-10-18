-- ods -> dwh (dim_customer)  -- id'ye gore sil + yukle
-- bugun ods'e yazilan musterilerin id'leri dwh'tan silinip ods'teki guncel halleriyle tekrar ekleniyor.

delete from dwh.dim_customer
where customer_id in (select id from ods.customers where etl_date = %(etl_date)s);

insert into dwh.dim_customer (customer_id, customer_name, email, email_domain, created_at, updated_at, etl_date)
select id, name, email, email_domain, idate, udate, %(etl_date)s
from ods.customers
where etl_date = %(etl_date)s;
