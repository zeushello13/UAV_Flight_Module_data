import csv
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# --- НАСТРОЙКА ПОДКЛЮЧЕНИЯ К БД ---
DB_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="uav_monitoring",      # <-- имя базы
    user="postgres",              # <-- пользователь
    password="postgres"           # <-- пароль
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CATALOG_DIR = PROJECT_ROOT / "out" / "alfa_catalog"


def load_flights(cur):
    path = CATALOG_DIR / "flights_catalog.csv"
    rows = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                r["FlightID"],
                r["StartUTC"] or None,
                r["EndUTC"] or None,
                float(r["DurationSec"]) if r["DurationSec"] else None,
                r["HasFailureGT"] in ("True", "true", "1"),
                int(r["FilesCSV"]) if r["FilesCSV"] else 0,
            ))

    sql = """
        INSERT INTO flight (flight_id, start_utc, end_utc,
                            duration_sec, has_failure_gt, files_csv)
        VALUES %s
        ON CONFLICT (flight_id) DO UPDATE
        SET start_utc     = EXCLUDED.start_utc,
            end_utc       = EXCLUDED.end_utc,
            duration_sec  = EXCLUDED.duration_sec,
            has_failure_gt= EXCLUDED.has_failure_gt,
            files_csv     = EXCLUDED.files_csv;
    """
    execute_values(cur, sql, rows)
    print(f"Загружено полётов: {len(rows)}")


def load_file_assets(cur):
    path = CATALOG_DIR / "files_catalog.csv"
    rows = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                r["FlightID"],
                r["FilePath"],
                r["TopicName"],
                int(r["Rows"]) if r["Rows"] else None,
                int(r["Cols"]) if r["Cols"] else None,
                r["HasTimeCol"] in ("True", "true", "1"),
                r["TimeMinUTC"] or None,
                r["TimeMaxUTC"] or None,
                r["MD5"] or None,
            ))

    sql = """
        INSERT INTO file_asset (
            flight_id, path, topic_name,
            rows, cols, has_timecol,
            time_min_utc, time_max_utc, md5
        )
        VALUES %s
        ON CONFLICT (md5) DO NOTHING;
    """
    execute_values(cur, sql, rows)
    print(f"Загружено файлов-логов: {len(rows)}")


def load_failure_events(cur):
    path = CATALOG_DIR / "failure_events.csv"
    if not path.exists():
        print("failure_events.csv не найден, пропускаю")
        return

    rows = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                r["FlightID"],
                r["FaultType"],
                r["EventTimeUTC"] or None,
                r.get("FailFile") or None,
            ))

    sql = """
        INSERT INTO failure_event (
            flight_id, fault_type, event_time_utc, source
        )
        VALUES %s;
    """
    execute_values(cur, sql, rows)
    print(f"Загружено событий отказов: {len(rows)}")


def load_topic_schema(cur):
    path = CATALOG_DIR / "topic_schema.csv"
    if not path.exists():
        print("topic_schema.csv не найден, пропускаю")
        return

    rows = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                r["TopicName"],
                r["Column"],
                r["ObservedDTypes"],
            ))

    sql = """
        INSERT INTO topic_schema (topic_name, column_name, observed_dtypes)
        VALUES %s
        ON CONFLICT (topic_name, column_name) DO UPDATE
        SET observed_dtypes = EXCLUDED.observed_dtypes;
    """
    execute_values(cur, sql, rows)
    print(f"Загружено описаний полей: {len(rows)}")


def load_signals(cur):
    """
    Загрузка телеметрии из signal_data.csv в таблицу signal.

    Ожидаемые колонки в CSV:
    flight_id, ts, channel, value
    """
    candidate_paths = [
        PROJECT_ROOT / "signal_data.csv",
        CATALOG_DIR / "signal_data.csv",
    ]
    path = None
    for p in candidate_paths:
        if p.exists():
            path = p
            break

    if path is None:
        print("signal_data.csv не найден, телеметрия не загружена")
        return

    print("Загрузка телеметрии из:", path)

    rows_batch = []
    total_inserted = 0
    total_skipped = 0

    batch_size = 10_000
    log_every = 100_000
    MAX_ROWS = None

    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, r in enumerate(reader, start=1):

            if MAX_ROWS is not None and i > MAX_ROWS:
                print(f"Достигнут лимит {MAX_ROWS} строк, останавливаемся.")
                break
            raw_val = (r.get("value") or "").strip()
            if not raw_val or raw_val.lower() in ("nan", "infinity", "-infinity"):
                total_skipped += 1
                continue

            try:
                val = float(raw_val)
            except ValueError:
                total_skipped += 1
                continue

            rows_batch.append((
                r["flight_id"],
                r["ts"],
                r["channel"],
                val,
            ))

            if len(rows_batch) >= batch_size:
                execute_values(
                    cur,
                    """
                    INSERT INTO signal (flight_id, ts, channel, value)
                    VALUES %s
                    ON CONFLICT (flight_id, ts, channel) DO NOTHING;
                    """,
                    rows_batch
                )
                total_inserted += len(rows_batch)
                rows_batch.clear()

                if total_inserted % log_every < batch_size:
                    print(f"Вставлено {total_inserted} строк телеметрии...")

        # добиваем остаток
        if rows_batch:
            execute_values(
                cur,
                """
                INSERT INTO signal (flight_id, ts, channel, value)
                VALUES %s
                ON CONFLICT (flight_id, ts, channel) DO NOTHING;
                """,
                rows_batch
            )
            total_inserted += len(rows_batch)

    print(f"Итого вставлено измерений телеметрии: {total_inserted}")
    print(f"Пропущено строк с некорректным value: {total_skipped}")




def main():
    print("Каталог с CSV:", CATALOG_DIR)
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                load_flights(cur)
                load_file_assets(cur)
                load_failure_events(cur)
                load_topic_schema(cur)
                load_signals(cur)
        print("Импорт успешно завершён.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
