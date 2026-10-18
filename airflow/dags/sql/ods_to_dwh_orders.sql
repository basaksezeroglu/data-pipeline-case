-- ods -> dwh (fact_order)  -- delete + insert
-- musteri dim'de yoksa -1 (UNKNOWN) yaziliyor.

delete from dwh.fact_order f
using ods.orders o
where f.order_id = o.id
  and o.etl_date >= %(load_from)s;

insert into dwh.fact_order (order_id, customer_id, order_date_key, amount, status, is_completed,
                            order_created_at, order_updated_at, etl_date)
select o.id,
       coalesce(c.customer_id, -1),
       to_char(o.order_date, 'YYYYMMDD')::int,
       o.amount,
       o.status,
       o.status = 'completed',
       o.idate,
       o.udate,
       %(etl_date)s
from ods.orders o
left join dwh.dim_customer c on c.customer_id = o.customer_id
where o.etl_date >= %(load_from)s
  and o.is_deleted = false;
