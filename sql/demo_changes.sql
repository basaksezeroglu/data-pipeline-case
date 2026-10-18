-- Kaynakta test icin degisiklik yapar. Her calistirildiginda yeni kayit olusturur.

-- 1) new customer (isim ve email bilerek duzensiz)
insert into public.customers (name, email)
values ('  Demo   Musteri ', 'DEMO_' || to_char(now(), 'HH24MISS') || '@Example.com')
returning *;

-- 2) bu musteriye order tanimi
insert into public.orders (customer_id, amount, status)
select max(id), 199.99, 'PENDING' from public.customers
returning *;

-- 3) son siparis status change
update public.orders set status = 'completed'
where id = (select max(id) from public.orders)
returning *;

-- 4) delete min id
delete from public.orders
where id = (select min(id) from public.orders)
returning *;
