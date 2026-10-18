# Kafka'daki Debezium mesajlarini okuyup hedef DB'deki stream tablolarina yazar.
#
# Debezium her degisiklik icin su formatta mesaj gonderiyor:
#   {"before": {...}, "after": {...}, "op": "c", "source": {"lsn": ..., "ts_ms": ...}}
#   op: r = ilk yukleme (snapshot), c = insert, u = update, d = delete

import json
import os
import time
from datetime import datetime, timedelta

import psycopg2
from confluent_kafka import Consumer

TARGET_DSN = os.environ["TARGET_DSN"]
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = ["cdc.public.customers", "cdc.public.orders"]

COLUMNS = {
    "customers": ["id", "name", "email", "idate", "udate"],
    "orders": ["id", "customer_id", "amount", "status", "idate", "udate"],
}


def to_timestamp(micros):
    # debezium timestamp kolonlarini 1970'ten beri gecen mikro saniye olarak gonderiyor
    if micros is None:
        return None
    return datetime(1970, 1, 1) + timedelta(microseconds=micros)


def save_event(cur, table, event):
    op = event["op"]
    row = event["before"] if op == "d" else event["after"]   # delete'te after bos geliyor

    columns = COLUMNS[table] + ["is_deleted", "cdc_op", "cdc_lsn", "cdc_source_ts"]
    values = [to_timestamp(row[c]) if c in ("idate", "udate") else row[c] for c in COLUMNS[table]]
    values += [op == "d", op, event["source"]["lsn"], datetime.utcfromtimestamp(event["source"]["ts_ms"] / 1000)]

    # kayit varsa guncelle yoksa ekle. delete'te satiri silmiyoruz, is_deleted = true oluyor.
    update_part = ", ".join(f"{c} = excluded.{c}" for c in columns if c != "id")
    sql = f"""
        insert into stream.{table} ({", ".join(columns)})
        values ({", ".join(["%s"] * len(columns))})
        on conflict (id) do update set {update_part}, cdc_consumed_ts = now()
    """
    cur.execute(sql, values)
    print(f"{table} op={op} id={row['id']}", flush=True)


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
        if msg.value() is None:
            continue

        table = msg.topic().split(".")[-1]   # cdc.public.orders -> orders
        event = json.loads(msg.value())
        if event["op"] not in ("r", "c", "u", "d"):
            continue

        save_event(conn.cursor(), table, event)
        conn.commit()

        # once db'ye yaziyoruz, sonra offset commit ediyoruz.
        # arada hata olursa program kapanir, docker yeniden baslatir ve ayni mesaj tekrar islenir (kayip olmaz).
        consumer.commit(message=msg, asynchronous=False)


if __name__ == "__main__":
    main()
