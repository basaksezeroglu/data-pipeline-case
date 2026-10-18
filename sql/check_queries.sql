-- Hedef DB (iyzicoTarget) kontrol sorgulari

-- streaming: kafka'dan gelen son degisiklikler
select * from stream.orders order by cdc_consumed_ts desc;
select * from stream.customers order by cdc_consumed_ts desc;

-- batch: run'lar ve her run'da kac kayit cekildi
select * from etl.run_log order by started_at desc;
select * from etl.watermark;

-- stg: son run'da cekilen kayitlar
select * from stg.customers;
select * from stg.orders;

-- ods: temizlenmis veri
select * from ods.customers order by id;
select * from ods.orders order by id;

-- dwh
select * from dwh.dim_customer order by customer_id;
select * from dwh.fact_order order by order_id;
select * from dwh.v_customer_sales;

-- healthcheck son durum
select * from hc.v_latest order by status, pipeline;

-- update dwh.fact_order set amount = amount + 1000 where order_id = (select max(order_id) from dwh.fact_order);
-- update dwh.fact_order set amount = amount - 1000 where order_id = (select max(order_id) from dwh.fact_order);
