# iyzico Data Engineer Case

Kaynak PostgreSQL'deki `customers` ve `orders` tablolarını hedef PostgreSQL'e iki şekilde aktardım:

- **Batch:** Airflow ile günlük ETL. Katmanlar: `stg` → `ods` → `dwh`
- **Streaming:** Debezium + Kafka ile gerçek zamanlı aktarım → `stream` şeması

Ayrıca iki tarafın da kaynakla tutarlı olup olmadığını kontrol eden bir healthcheck yazdım.

```
                 ┌── Airflow (günlük) ──► stg ──► ods ──► dwh
 kaynak DB ──────┤
 (postgreSource) └── Debezium ──► Kafka ──► kafka_consumer.py ──► stream
                                                                 (iyzicoTarget)
```

## Dosyalar

```
docker-compose.yml          Kafka, Debezium, consumer ve Airflow container'ları
sql/
  source_tables.sql         kaynak tablolar + örnek veri
  target_tables.sql         hedefteki şemalar ve tablolar
  demo_changes.sql          kaynakta test için insert/update/delete
  check_queries.sql         hedefi kontrol etmek için sorgular
airflow/dags/
  batch_etl_dag.py          batch ETL DAG'ı
  healthcheck_dag.py        15 dakikada bir çalışan healthcheck DAG'ı
  healthcheck_functions.py  kontrollerin kendisi
  sql/                      ETL adımlarının SQL'leri (stg→ods, ods→dwh)
streaming/
  debezium_connector.json   Debezium ayarları
  kafka_consumer.py         Kafka'dan okuyup stream tablolarına yazan kod
  Dockerfile
```

## Nasıl çalıştırılır

Kaynak (`localhost:5433`, db: `postgreSource`) ve hedef (`localhost:5434`, db: `iyzicoTarget`) PostgreSQL'ler
ayrı container olarak çalışıyor (kullanıcı/şifre: postgres/postgres). Debezium için kaynakta `wal_level=logical` olmalı.

```bash
docker compose up -d
```

Bu komut tabloları oluşturuyor, Kafka/Debezium/Airflow'u başlatıyor ve Debezium connector'ünü kaydediyor.

- Airflow: http://localhost:8080 (admin / admin)
- Kaynakta değişiklik yapmak için `sql/demo_changes.sql`'i DBeaver'da çalıştır.
- Healthcheck'i elle çalıştırmak için:
  ```bash
  docker exec airflow python /opt/airflow/dags/healthcheck_functions.py
  ```

## Batch ETL

DAG adımları:

1. **start_run** – `etl.run_log`'a bir satır ekler, bugünün tarihini (`etl_date`) belirler.
2. **extract_customers / extract_orders** – Kaynakta `udate > watermark` olan kayıtları `stg`'ye yazar.
   Silinen kayıtları bulabilmek için kaynaktaki tüm id'leri de `stg.*_keys` tablolarına yazar.
3. **load_ods_customers / load_ods_orders** – Veriyi temizleyip `ods`'ye upsert eder:
   - isimdeki fazla boşluklar silinir, email küçük harfe çevrilir
   - status küçük harf olur, bilinmeyen status `unknown` olur
   - kaynakta silinmiş kayıtlar silinmez, `is_deleted = true` yapılır
4. **load_dwh_customers / load_dwh_orders** – `dwh.dim_customer` ve `dwh.fact_order` tablolarına **delete + insert**.
   ODS'te son başarılı run'dan bu yana değişen kayıtlar DWH'tan silinip yeniden eklenir.
5. **end_run** – Watermark'ı günceller, run'ı `success` yapar.
6. **healthcheck** – Kaynak, ODS ve DWH'ı karşılaştırır.

Bazı kararlar:

- **etl_date:** Her tabloda `etl_date` (date) var. ODS'te kaydın en son değiştiği günü, DWH'ta yüklendiği günü gösteriyor.
- **Watermark en sonda güncelleniyor:** Arada bir adım hata alırsa watermark ilerlemiyor, sonraki run aynı kayıtları tekrar çekiyor.
- **DWH son başarılı run'dan itibaren yükleniyor:** Bir run ODS'i yükleyip DWH'ta hata alırsa, sonraki run bu kayıtları da DWH'a taşıyor.
- **udate için trigger:** Kaynağa update'te `udate`'i otomatik güncelleyen bir trigger ekledim. Uygulama udate'i güncellemeyi unutursa batch o değişikliği göremezdi.
- **SCD Type 1:** Müşteri bilgisi değişince DWH'ta güncel hali tutuluyor, geçmiş tutulmuyor. Case'te geçmiş ihtiyacı yoktu.

## Streaming

- **Debezium**, Kafka Connect içinde çalışan bir connector. Kaynak DB'nin WAL'ını (transaction log) logical replication ile okuyor.
  Insert, update ve delete'leri yakalayıp Kafka'ya yazıyor.
- Topic'ler: `cdc.public.customers`, `cdc.public.orders`
- **kafka_consumer.py** topic'leri okuyup `stream.customers` ve `stream.orders` tablolarına yazıyor:
  - insert/update → upsert
  - delete → `is_deleted = true`
  - Önce DB'ye yazıp sonra Kafka offset'ini commit ediyor; hata olursa mesaj kaybolmuyor.
- Debezium delete mesajında silinen satırın tüm kolonları gelsin diye kaynak tablolarda `REPLICA IDENTITY FULL` açtım.

## Healthcheck

Sonuçlar `hc.check_result` tablosuna yazılıyor (son durum: `hc.v_latest`). FAIL varsa Airflow task'ı hata veriyor.

| Kontrol | Ne yapıyor |
|---|---|
| batch / source_vs_ods | Watermark'a kadar değişen her kaynak kaydı ODS'te aynı `udate` ile var mı |
| batch / waiting_next_run | Watermark'tan sonra değişen kayıtlar (sonraki run'da gelecek, sadece WARN) |
| batch / ods_vs_dwh | ODS ve DWH'taki kayıt sayısı ve toplam tutar aynı mı |
| batch / last_run | Son başarılı run 26 saatten eski mi |
| streaming / source_vs_stream | Kaynak ve stream tabloları satır satır aynı mı (fark varsa 15 sn bekleyip tekrar bakıyor) |
| streaming / connector_status | Debezium connector'ü RUNNING mi |
| streaming / replication_slot | Kaynaktaki replication slot aktif mi |

## Eksikler / geliştirilebilecekler

- Bilgisayarımda Docker'a ~4 GB RAM düşüyordu. Kafka zorlandığı için streaming bazen dakikalarca gecikti.
  Bu yüzden Kafka ve Connect'in RAM'ini düşük tuttum.
- Silinen kayıtları bulmak için her run'da kaynaktaki tüm id'leri çekiyorum. Büyük tablolarda bu yavaş olur;
  o durumda silme bilgisini streaming'den almak daha mantıklı.
- Healthcheck satır satır karşılaştırıyor, büyük tablolarda hash ile karşılaştırma gerekir.
- Şifreler compose dosyasında düz yazıyor, gerçek ortamda secret olarak tutulmalı.
- Tek Kafka broker var, sadece local test için.
