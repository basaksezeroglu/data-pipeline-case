# Kaynak ile hedef arasindaki tutarliligi kontrol eden fonksiyonlar.
# Sonuclar hc.check_result tablosuna yaziliyor.
#
# Terminalden calistirmak icin:
#   docker exec airflow python /opt/airflow/dags/healthcheck_functions.py

import json
import os
import sys
import time
import urllib.request

import psycopg2

TABLES = {
    "customers": ["id", "name", "email", "idate", "udate"],
    "orders": ["id", "customer_id", "amount", "status", "idate", "udate"],
}


def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.rollback()
    return rows


def add_result(results, pipeline, table, check, ok, expected, actual, details="", only_warning=False):
    if ok:
        status = "PASS"
    elif only_warning:
        status = "WARN"
    else:
        status = "FAIL"
    results.append({"pipeline": pipeline, "table": table, "check": check, "status": status,
                    "expected": str(expected), "actual": str(actual), "details": details})


# ---------------- BATCH ----------------

def batch_checks(source, target, results):
    for table in TABLES:
        watermark = query(target, f"select last_udate from etl.watermark where table_name = '{table}'")[0][0]

        source_rows = dict(query(source, f"select id, udate from public.{table}"))
        ods_rows = dict(query(target, f"select id, udate from ods.{table} where is_deleted = false"))

        # watermark'a kadar degisen kayitlar ods'te ayni udate ile olmali
        wrong = [i for i, udate in source_rows.items()
                 if udate <= watermark and ods_rows.get(i) != udate]
        add_result(results, "batch", table, "source_vs_ods", len(wrong) == 0, 0, len(wrong),
                   f"eksik veya eski kayitlar: {sorted(wrong)[:10]}")

        # watermark'tan sonra degisen / silinen kayitlar bir sonraki run'da gelecek, sadece uyari
        waiting = [i for i, udate in source_rows.items() if udate > watermark]
        waiting += [i for i in ods_rows if i not in source_rows]
        add_result(results, "batch", table, "waiting_next_run", len(waiting) == 0, 0, len(waiting),
                   f"sonraki run'i bekleyen kayitlar: {sorted(waiting)[:10]}", only_warning=True)

    # ods ve dwh ayni olmali
    ods_count = query(target, "select count(*) from ods.customers where is_deleted = false")[0][0]
    dwh_count = query(target, "select count(*) from dwh.dim_customer where customer_id <> -1")[0][0]
    add_result(results, "batch", "customers", "ods_vs_dwh", ods_count == dwh_count, ods_count, dwh_count)

    ods_orders = query(target, "select count(*), coalesce(sum(amount), 0) from ods.orders where is_deleted = false")[0]
    dwh_orders = query(target, "select count(*), coalesce(sum(amount), 0) from dwh.fact_order")[0]
    add_result(results, "batch", "orders", "ods_vs_dwh", ods_orders == dwh_orders,
               f"{ods_orders[0]} kayit / {ods_orders[1]} tutar", f"{dwh_orders[0]} kayit / {dwh_orders[1]} tutar")

    # son basarili run 26 saatten eski olmamali (gunluk calisiyor)
    hours = query(target, "select extract(epoch from now() - max(finished_at)) / 3600 "
                          "from etl.run_log where status = 'success'")[0][0]
    add_result(results, "batch", None, "last_run", hours is not None and hours < 26, "< 26 saat",
               "hic basarili run yok" if hours is None else f"{hours:.1f} saat once")


# ---------------- STREAMING ----------------

def compare_stream(source, target, table):
    columns = ", ".join(TABLES[table])
    source_rows = set(query(source, f"select {columns} from public.{table}"))
    stream_rows = set(query(target, f"select {columns} from stream.{table} where is_deleted = false"))
    return source_rows ^ stream_rows   # sadece bir tarafta olan satirlar


def streaming_checks(source, target, results):
    for table in TABLES:
        diff = compare_stream(source, target, table)
        if diff:
            # streaming birkac saniye gecikebilir, biraz bekleyip tekrar bak
            time.sleep(15)
            diff = compare_stream(source, target, table)
        diff_ids = sorted({row[0] for row in diff})
        add_result(results, "streaming", table, "source_vs_stream", len(diff) == 0, 0, len(diff_ids),
                   f"farkli id'ler: {diff_ids[:10]}")

    # debezium connector calisiyor mu
    try:
        url = os.getenv("CONNECT_URL", "http://connect:8083") + "/connectors/source-pg-connector/status"
        status = json.load(urllib.request.urlopen(url, timeout=20))
        states = [status["connector"]["state"]] + [t["state"] for t in status["tasks"]]
    except Exception as e:
        states = [f"ulasilamadi: {e}"]
    add_result(results, "streaming", None, "connector_status", all(s == "RUNNING" for s in states),
               "RUNNING", ", ".join(states))

    # replication slot aktif mi (debezium kaynagi okuyor mu)
    slot = query(source, "select active from pg_replication_slots where slot_name = 'debezium_slot'")
    add_result(results, "streaming", None, "replication_slot", bool(slot) and slot[0][0],
               "active", slot[0][0] if slot else "slot yok")


# ---------------- ANA FONKSIYON ----------------

def run_healthcheck(source, target, pipelines=("batch", "streaming"), run_name=None):
    run_name = run_name or "manual_" + time.strftime("%Y%m%d_%H%M%S")
    results = []

    if "batch" in pipelines:
        batch_checks(source, target, results)
    if "streaming" in pipelines:
        streaming_checks(source, target, results)

    # sonuclari tabloya yaz
    cur = target.cursor()
    for r in results:
        cur.execute("""
            insert into hc.check_result (run_id, pipeline, table_name, check_name, status, expected, actual, details)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (run_name, r["pipeline"], r["table"], r["check"], r["status"], r["expected"], r["actual"], r["details"]))
    target.commit()

    # ekrana yazdir
    print(f"\nHEALTHCHECK {run_name}")
    for r in results:
        print(f"{r['status']:5} {r['pipeline']:10} {str(r['table'] or '-'):10} {r['check']:18} "
              f"beklenen={r['expected']}  gelen={r['actual']}")

    failed = [f"{r['table']}.{r['check']}" for r in results if r["status"] == "FAIL"]
    print(f"FAIL sayisi: {len(failed)}\n")
    return failed


if __name__ == "__main__":
    source = psycopg2.connect(os.environ["SOURCE_DSN"])
    target = psycopg2.connect(os.environ["TARGET_DSN"])
    failed = run_healthcheck(source, target)
    sys.exit(1 if failed else 0)
