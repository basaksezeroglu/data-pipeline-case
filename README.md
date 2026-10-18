# iyzico Data Engineer Case

Kaynak PostgreSQL'deki `customers` ve `orders` tablolarını hedef PostgreSQL'e iki şekilde aktardım:

- **Batch:** Airflow ile günlük ETL. Katmanlar: `stg` → `ods` → `dwh`
- **Streaming:** Kafka Connect (JDBC Source connector) + Kafka → `stream` şeması

Ayrıca iki tarafın da kaynakla tutarlı olup olmadığını kontrol eden bir healthcheck yazdım.

```
                 ┌── Airflow (günlük) ─────────────────► stg ──► ods ──► dwh
 kaynak DB ──────┤
 (postgreSource) └── Kafka Connect (5 sn) ──► Kafka ──► kafka_consumer.py ──► stream
                                                                     (iyzicoTarget)
```

## Dosyalar

```
docker-compose.yml          Kafka, Kafka Connect, consumer ve Airflow container'ları
sql/
  source_tables.sql         kaynak tablolar + örnek veri
  target_tables.sql         hedefteki şemalar ve tablolar
  demo_changes.sql          kaynakta test için insert/update
  check_queries.sql         hedefi kontrol etmek için sorgular
airflow/dags/
  batch_etl_dag.py          batch ETL DAG'ı
  healthcheck_dag.py        15 dakikada bir çalışan healthcheck DAG'ı
  healthcheck_functions.py  kontrollerin kendisi
  sql/                      ETL adımlarının SQL'leri (stg→ods, ods→dwh)
streaming/
  connect/Dockerfile        Kafka Connect + JDBC connector image'ı
  jdbc_connector.json       JDBC connector ayarları
  consumer/                 Kafka'dan okuyup stream tablolarına yazan Python kodu
```

## Nasıl çalıştırılır

Kaynak (`localhost:5433`, db: `postgreSource`) ve hedef (`localhost:5434`, db: `iyzicoTarget`) PostgreSQL'ler
ayrı container olarak çalışıyor (kullanıcı/şifre: postgres/postgres).

```bash
docker compose up -d --build
```

Bu komut tabloları oluşturuyor, Kafka/Kafka Connect/Airflow'u başlatıyor ve JDBC connector'ünü kaydediyor.

- Airflow: http://localhost:8080 (admin / admin)
- Kaynakta değişiklik yapmak için `sql/demo_changes.sql`'i DBeaver'da çalıştır.
- Healthcheck'i elle çalıştırmak için:
  ```bash
  docker exec airflow python /opt/airflow/dags/healthcheck_functions.py
  ```

## Varsayım

Kaynak tablolarda sadece `idate` (oluşturma) ve `udate` (güncelleme) alanları var. Bu yüzden kaynakta kayıtların
eklendiğini ve güncellendiğini, **silinmediğini (hard delete yapılmadığını)** varsaydım. Silme gerekirse kaynakta
bir `is_deleted`/`status` kolonu ile soft delete yapılması ya da streaming'de Debezium gibi log tabanlı bir
araca geçilmesi gerekir.

## Batch ETL

**Watermark:** Her tablo için "en son hangi tarihe kadar veri çektim" bilgisini `etl.watermark` tablosunda tutuyorum.
Her run'da bu tarihten sonraki kayıtlar çekiliyor, run başarıyla biterse tarih ilerletiliyor.

DAG adımları:

1. **start_run** – `etl.run_log`'a bir satır ekler, bugünün tarihini (`etl_date`) belirler.
2. **extract_customers / extract_orders** – Kaynaktan `idate >= watermark or udate >= watermark` olan kayıtları
   (yeni eklenen + güncellenen) `stg` tablosuna yazar. `stg` her run'da boşaltılır.
3. **load_ods_customers / load_ods_orders** – Veriyi temizleyip `ods`'ye upsert eder:
   - isimdeki fazla boşluklar silinir, email küçük harfe çevrilir, email formatı kontrol edilir
   - status küçük harf olur, bilinmeyen status `unknown` olur, tutar 2 haneye yuvarlanır
   - yazılan her kaydın `etl_date`'i bugün olur
4. **load_dwh_customers / load_dwh_orders** – **ID'ye göre sil + yükle.** Bugün ODS'e yazılan kayıtların id'leri
   `dwh.dim_customer` / `dwh.fact_order`'dan silinir, ODS'teki güncel halleri tekrar eklenir.
5. **end_run** – Watermark'ı çekilen en yeni kaydın `udate`'ine ilerletir, run'ı `success` yapar.
6. **healthcheck** – Kaynak, ODS ve DWH'ı karşılaştırır.

Bazı kararlar:

- **Watermark en sonda güncelleniyor:** Arada bir adım hata alırsa watermark ilerlemiyor, sonraki run aynı kayıtları
  tekrar çekip ODS ve DWH'a yazıyor. Veri kaçmıyor.
- **`>=` kullanımı:** Tam sınırdaki kayıt kaçmasın diye. Aynı kayıt tekrar gelse de DWH'a id'ye göre sil-yükle
  yapıldığı için sonuç değişmiyor.
- **udate için trigger:** Kaynağa update'te `udate`'i otomatik güncelleyen bir trigger ekledim. Uygulama `udate`'i
  güncellemeyi unutursa ETL o değişikliği göremezdi.
- **SCD Type 1:** Müşteri bilgisi değişince DWH'ta güncel hali tutuluyor, geçmiş tutulmuyor. Case'te geçmiş ihtiyacı yoktu.

## Streaming

Case'te Kafka'ya veri aktarmak için Debezium veya Kafka Connect önerilmişti. **Kafka Connect'in JDBC Source
connector'ünü** seçtim:

- Connector her 5 saniyede bir kaynağa batch'tekiyle aynı mantıkta sorgu atıyor: "en son baktığım `udate`'ten sonra
  değişen kayıtlar hangileri?" (`mode: timestamp+incrementing`, `udate` + `id`).
- Bulduğu her kaydı JSON mesaj olarak Kafka'ya yazıyor. Topic'ler: `src_customers`, `src_orders`.
- Hangi `udate`'e kadar okuduğunu Kafka Connect kendisi saklıyor; connector yeniden başlasa kaldığı yerden devam ediyor.
- **kafka_consumer.py** topic'leri okuyup `stream.customers` ve `stream.orders` tablolarına id'ye göre upsert yapıyor.
  Önce DB'ye yazıp sonra Kafka offset'ini commit ediyor; hata olursa mesaj kaybolmuyor.

Neden Debezium değil: Debezium kaynak DB'nin transaction log'unu (WAL) okuyarak silmeleri de anlık yakalayabiliyor ama
kaynakta `wal_level=logical`, replication slot ve ek yetki gerektiriyor. Kaynakta silme olmadığını varsaydığım için
batch ile aynı mantıkta çalışan, kurulumu ve takibi daha basit JDBC connector'ü yeterli buldum.

## Healthcheck

Sonuçlar `hc.check_result` tablosuna yazılıyor (son durum: `hc.v_latest`). FAIL varsa Airflow task'ı hata veriyor.

| Kontrol | Ne yapıyor |
|---|---|
| batch / source_vs_ods | Watermark'a kadar değişen her kaynak kaydı ODS'te aynı `udate` ile var mı |
| batch / waiting_next_run | Watermark'tan sonra değişen kayıtlar (sonraki run'da gelecek, sadece WARN) |
| batch / ods_vs_dwh | ODS ve DWH'taki kayıt sayısı ve toplam tutar aynı mı |
| batch / last_run | Son başarılı run 26 saatten eski mi |
| streaming / source_vs_stream | Kaynak ve stream tabloları satır satır aynı mı (fark varsa 15 sn bekleyip tekrar bakıyor) |
| streaming / connector_status | Kafka Connect connector'ü RUNNING mi |

## Eksikler / geliştirilebilecekler

- Kaynakta silme olursa ne batch ne streaming bunu görür (yukarıdaki varsayım). Gerekirse soft delete kolonu ya da Debezium.
- Bilgisayarımda Docker'a ~4 GB RAM düşüyordu, bu yüzden Kafka ve Kafka Connect'in RAM'ini düşük tuttum.
- Kafka Connect tarihleri milisaniye hassasiyetinde taşıyor; healthcheck'te karşılaştırma ms'e yuvarlanarak yapılıyor.
- Healthcheck satır satır karşılaştırıyor, büyük tablolarda hash ile karşılaştırma gerekir.
- Şifreler compose dosyasında düz yazıyor, gerçek ortamda secret olarak tutulmalı.
- Tek Kafka broker var, sadece local test için.
