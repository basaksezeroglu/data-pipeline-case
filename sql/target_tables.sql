-- HEDEF DB (iyzicoTarget) semalari ve tablolari
--   stg    : kaynaktan cekilen degisen kayitlar (her run'da bosaltilir)
--   ods    : temizlenmis guncel veri
--   dwh    : raporlama tablolari (dim / fact)
--   etl    : run log ve watermark
--   stream : kafka'dan gelen gercek zamanli veri
--   hc     : healthcheck sonuclari

create schema if not exists stg;
create schema if not exists ods;
create schema if not exists dwh;
create schema if not exists etl;
create schema if not exists stream;
create schema if not exists hc;

-- ===================== ETL =====================

-- her dag run'i icin bir satir
create table if not exists etl.run_log (
    run_id       text primary key,
    etl_date     date      not null,
    status       text      not null default 'running',   -- running / success / failed
    started_at   timestamp not null default now(),
    finished_at  timestamp,
    stats        jsonb
);

-- her tablo icin en son hangi udate'e kadar veri cektigimiz
create table if not exists etl.watermark (
    table_name     text primary key,
    last_udate     timestamp not null default '1900-01-01',
    last_etl_date  date,
    updated_at     timestamp not null default now()
);
insert into etl.watermark (table_name) values ('customers'), ('orders')
on conflict (table_name) do nothing;

-- ===================== STG =====================

create table if not exists stg.customers (
    id              integer,
    name            varchar(100),
    email           varchar(255),
    idate           timestamp,
    udate           timestamp,
    etl_date        date,
    etl_extract_ts  timestamp default now()
);

create table if not exists stg.orders (
    id              integer,
    customer_id     integer,
    amount          numeric(12,2),
    status          varchar(20),
    idate           timestamp,
    udate           timestamp,
    etl_date        date,
    etl_extract_ts  timestamp default now()
);

-- ===================== ODS =====================

create table if not exists ods.customers (
    id              integer primary key,
    name            varchar(100) not null,
    email           varchar(255) not null,
    email_domain    varchar(255),
    is_valid_email  boolean      not null,
    idate           timestamp    not null,
    udate           timestamp    not null,
    etl_date        date         not null,      -- kaydin ods'e en son yazildigi gun
    etl_insert_ts   timestamp    not null default now(),
    etl_update_ts   timestamp    not null default now()
);
create index if not exists ix_ods_customers_etl_date on ods.customers(etl_date);

create table if not exists ods.orders (
    id              integer primary key,
    customer_id     integer       not null,
    amount          numeric(12,2) not null,
    status          varchar(20)   not null,
    order_date      date          not null,
    idate           timestamp     not null,
    udate           timestamp     not null,
    etl_date        date          not null,
    etl_insert_ts   timestamp     not null default now(),
    etl_update_ts   timestamp     not null default now()
);
create index if not exists ix_ods_orders_etl_date on ods.orders(etl_date);

-- ===================== DWH =====================

create table if not exists dwh.dim_date (
    date_key     integer primary key,   -- 20260115 gibi
    full_date    date     not null,
    year         smallint not null,
    quarter      smallint not null,
    month        smallint not null,
    day          smallint not null,
    day_of_week  smallint not null,     -- 1 = pazartesi
    is_weekend   boolean  not null
);
insert into dwh.dim_date
select to_char(d, 'YYYYMMDD')::int, d::date,
       extract(year from d), extract(quarter from d), extract(month from d), extract(day from d),
       extract(isodow from d), extract(isodow from d) in (6, 7)
from generate_series('2020-01-01'::date, '2035-12-31'::date, interval '1 day') d
on conflict (date_key) do nothing;

create table if not exists dwh.dim_customer (
    customer_id     integer primary key,   -- -1 = bilinmeyen musteri
    customer_name   varchar(100) not null,
    email           varchar(255),
    email_domain    varchar(255),
    created_at      timestamp,
    updated_at      timestamp,
    etl_date        date,                  -- dwh'a yuklendigi gun
    dwh_load_ts     timestamp not null default now()
);
insert into dwh.dim_customer (customer_id, customer_name) values (-1, 'UNKNOWN')
on conflict (customer_id) do nothing;

create table if not exists dwh.fact_order (
    order_id          integer primary key,
    customer_id       integer       not null,   -- dim delete-insert ile yuklendigi icin FK koymadim
    order_date_key    integer       not null references dwh.dim_date(date_key),
    amount            numeric(12,2) not null,
    status            varchar(20)   not null,
    is_completed      boolean       not null,
    order_created_at  timestamp     not null,
    order_updated_at  timestamp     not null,
    etl_date          date,
    dwh_load_ts       timestamp     not null default now()
);
create index if not exists ix_fact_order_customer on dwh.fact_order(customer_id);
create index if not exists ix_fact_order_date     on dwh.fact_order(order_date_key);

-- musteri bazinda satis ozeti
create or replace view dwh.v_customer_sales as
select c.customer_id, c.customer_name, c.email,
       count(f.order_id)                                        as order_count,
       coalesce(sum(f.amount) filter (where f.is_completed), 0) as completed_amount,
       coalesce(sum(f.amount), 0)                               as total_amount,
       max(f.order_created_at)                                  as last_order_at
from dwh.dim_customer c
left join dwh.fact_order f on f.customer_id = c.customer_id
where c.customer_id <> -1
group by c.customer_id, c.customer_name, c.email;

-- ===================== STREAM =====================
-- kaynakla ayni kolonlar + kafka'dan okundugu zaman

create table if not exists stream.customers (
    id           integer primary key,
    name         varchar(100),
    email        varchar(255),
    idate        timestamp,
    udate        timestamp,
    consumed_ts  timestamp not null default now()
);

create table if not exists stream.orders (
    id           integer primary key,
    customer_id  integer,
    amount       numeric(12,2),
    status       varchar(20),
    idate        timestamp,
    udate        timestamp,
    consumed_ts  timestamp not null default now()
);

-- ===================== HEALTHCHECK =====================

create table if not exists hc.check_result (
    id           bigserial primary key,
    run_id       text      not null,
    run_ts       timestamp not null default now(),
    pipeline     text      not null,   -- batch / streaming
    table_name   text,
    check_name   text      not null,
    status       text      not null,   -- PASS / WARN / FAIL
    expected     text,
    actual       text,
    details      text
);

-- her kontrolun en son sonucu
create or replace view hc.v_latest as
select distinct on (pipeline, table_name, check_name) *
from hc.check_result
order by pipeline, table_name, check_name, run_ts desc, id desc;
