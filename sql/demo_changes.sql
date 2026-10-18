-- Kaynakta test icin degisiklik yapar. Her calistirildiginda yeni kayit olusturur.
-- DBeaver'da postgreSource baglantisinda sorgulari tek tek calistir (Ctrl+Enter).

-- 1) yeni musteri (isim ve email bilerek duzensiz, ods'te temizleniyor mu bakalim)
insert into public.customers (name, email)
values ('  Demo   Musteri ', 'DEMO_' || to_char(now(), 'HH24MISS') || '@Example.com')
returning *;

-- 2) bu musteriye siparis
insert into public.orders (customer_id, amount, status)
select max(id), 199.99, 'PENDING' from public.customers
returning *;

-- 3) son siparisi tamamla
update public.orders set status = 'completed'
where id = (select max(id) from public.orders)
returning *;

-- 4) en eski siparisi sil
delete from public.orders
where id = (select min(id) from public.orders)
returning *;
