-- stg -> ods (orders)
-- status kucuk harfe cevriliyor, bilinmeyen status 'unknown' oluyor.

insert into ods.orders (id, customer_id, amount, status, order_date, idate, udate, etl_date)
select distinct on (id)
       id,
       customer_id,
       round(amount, 2),
       case when lower(trim(status)) in ('pending', 'completed', 'cancelled')
            then lower(trim(status))
            else 'unknown' end,
       idate::date,
       idate,
       udate,
       %(etl_date)s
from stg.orders
order by id, udate desc
on conflict (id) do update set
    customer_id   = excluded.customer_id,
    amount        = excluded.amount,
    status        = excluded.status,
    order_date    = excluded.order_date,
    idate         = excluded.idate,
    udate         = excluded.udate,
    etl_date      = excluded.etl_date,
    etl_update_ts = now();
