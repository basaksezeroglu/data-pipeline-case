# Kafka'daki mesajlari okuyup hedef DB'deki stream tablolarina yazar.
#
# Kafka Connect JDBC connector her 5 saniyede bir kaynakta yeni eklenen/guncellenen
# kayitlari buluyor ve her kaydi bir mesaj olarak Kafka'ya yaziyor:
#   topic: src_orders   mesaj: {"id": 101, "customer_id": 1, "amount": 250.00, "status": "completed", ...}

import json
import os
import time
from datetime import datetime, timedelta

import psycopg2
from confluent_kafka import Consumer

TARGET_DSN = os.environ["TARGET_DSN"]
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = ["src_customers", "src_orders"]

COLUMNS = {
    "customers": ["id", "name", "email", "idate", "udate"],
    "orders": ["id", "customer_id", "amount", "status", "idate", "udate"],
}


def to_timestamp(millis):
    # kafka connect tarih kolonlarini 1970'ten beri gecen milisaniye olarak gonderiyor
    if millis is None:
        return None
    return datetime(1970, 1, 1) + timedelta(milliseconds=millis)


def save_row(cur, table, row):
    columns = COLUMNS[table]
    values = [to_timestamp(row[c]) if c in ("idate", "udate") else row[c] for c in columns]

    # kayit varsa guncelle yoksa ekle
    update_part = ", ".join(f"{c} = excluded.{c}" for c in columns if c != "id")
    sql = f"""
        insert into stream.{table} ({", ".join(columns)})
        values ({", ".join(["%s"] * len(columns))})
        on conflict (id) do update set {update_part}, consumed_ts = now()
    """
    cur.execute(sql, values)
    print(f"{table} id={row['id']} yazildi", flush=True)


def main():
    # db hazir olana kadar bekle
    while True:
        try:
            conn = psycopg2.connect(TARGET_DSN)
            break
        except psycopg2.OperationalError:
            print("db bekleniyor...", flush=True)
            time.sleep(3)

    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": "stream-consumer",
        "auto.offset.reset": "earliest",   # ilk calistiginda topic'in basindan oku
        "enable.auto.commit": False,       # offset'i db'ye yazdiktan sonra kendimiz commit ediyoruz
    })
    consumer.subscribe(TOPICS)
    print(f"dinleniyor: {TOPICS}", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("kafka hatasi:", msg.error(), flush=True)
            continue

        table = msg.topic().replace("src_", "")   # src_orders -> orders
        row = json.loads(msg.value())

        save_row(conn.cursor(), table, row)
        conn.commit()

        # once db'ye yaziyoruz, sonra offset commit ediyoruz.
        # arada hata olursa program kapanir, docker yeniden baslatir ve ayni mesaj tekrar islenir (kayip olmaz).
        consumer.commit(message=msg, asynchronous=False)


if __name__ == "__main__":
    main()
