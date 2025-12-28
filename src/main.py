from pathlib import Path
import pandas as pd
import numpy as np
import hashlib
import re
import sys

# ---------- НАСТРОЙКИ ПУТЕЙ ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# python -m src.main --base "C:/.../data/processed_extracted"
cli_base = None
if '--base' in sys.argv:
    i = sys.argv.index('--base')
    if i + 1 < len(sys.argv):
        cli_base = Path(sys.argv[i + 1])

BASE = cli_base if cli_base else (PROJECT_ROOT / 'data' / 'processed_extracted')
OUT = PROJECT_ROOT / 'out' / 'alfa_catalog'
OUT.mkdir(parents=True, exist_ok=True)


# ---------- КЛАСС ОБРАБОТКИ ДАННЫХ ----------
class FlightDataProcessor:
    def __init__(self, base_path: Path, output_path: Path):
        self.base_path = base_path
        self.output_path = output_path
        self.flight_rows = []
        self.file_rows = []
        self.failure_rows = []
        self.topic_schemas = {}
        self.flight_bounds = {}

    def _to_datetime_guess(self, series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors='coerce')
        mx = s.max(skipna=True)
        if pd.isna(mx):  # если всё NaN
            return pd.to_datetime([pd.NaT] * len(s))
        if mx > 1e14:
            u = 'ns'
        elif mx > 1e12:
            u = 'us'
        elif mx > 1e10:
            u = 'ms'
        elif mx > 1e5:
            u = 's'
        else:
            return pd.to_datetime([pd.NaT] * len(s))
        return pd.to_datetime(s, unit=u, errors='coerce')

    def _count_rows_quick(self, path: Path):
        try:
            with open(path, 'rb') as f:
                return max(0, sum(1 for _ in f) - 1)  # минус заголовок
        except Exception:
            return None

    def _file_md5(self, path: Path, block=1 << 20) -> str:
        h = hashlib.md5()
        with open(path, 'rb') as f:
            while True:
                b = f.read(block)
                if not b: break
                h.update(b)
        return h.hexdigest()

    def _read_csv_head(self, path: Path, nrows=25) -> pd.DataFrame | None:
        for sep in (',', ';', '\t'):
            try:
                return pd.read_csv(path, nrows=nrows, sep=sep, engine='python', encoding_errors='ignore')
            except Exception:
                pass
        try:
            return pd.read_csv(path, nrows=nrows, engine='python', encoding_errors='ignore')
        except Exception:
            return None

    def _flight_id_for(self, csv_path: Path) -> str:
        """FlightID = первая подпапка под ROOT (имя каталога полёта)."""
        parts = csv_path.relative_to(self.base_path).parts
        return parts[0] if len(parts) > 1 else csv_path.stem

    def _process_csv_files(self):
        # Обрабатываем все CSV в каталоге
        csv_files = sorted(self.base_path.rglob('*.csv'))
        for csv_path in csv_files:
            fid = self._flight_id_for(csv_path)
            topic = csv_path.stem

            head = self._read_csv_head(csv_path, nrows=25)
            if head is not None:
                cols = list(head.columns)
                dtypes = {c: str(head[c].dtype) for c in head.columns}
            else:
                cols, dtypes = [], {}

            ts_col = next(
                (c for c in ['%time', 'time', 'stamp', 'rosbagTimestamp'] if head is not None and c in head.columns),
                None)
            tmin = tmax = None
            if ts_col:
                try:
                    tser = pd.read_csv(csv_path, usecols=[ts_col], engine='python', encoding_errors='ignore')[ts_col]
                    dt = self._to_datetime_guess(tser)
                    tmin = pd.to_datetime(dt.min());
                    tmax = pd.to_datetime(dt.max())
                except Exception:
                    pass
            if tmin is not None and not pd.isna(tmin):
                lo, hi = self.flight_bounds.get(fid, (None, None))
                lo = tmin if lo is None else min(lo, tmin)
                hi = tmax if hi is None else max(hi, tmax)
                self.flight_bounds[fid] = (lo, hi)

            nrows = self._count_rows_quick(csv_path)
            self.file_rows.append({
                'FlightID': fid,
                'FilePath': str(csv_path.relative_to(self.base_path)).replace('\\', '/'),
                'TopicName': topic,
                'Rows': nrows,
                'Cols': len(cols),
                'HasTimeCol': ts_col is not None,
                'TimeMinUTC': tmin,
                'TimeMaxUTC': tmax,
                'MD5': self._file_md5(csv_path),
            })

            # схема топика
            if cols:
                self.topic_schemas.setdefault(topic, {})
                for c in cols:
                    self.topic_schemas[topic].setdefault(c, set()).add(dtypes.get(c, 'object'))

            # GT отказов
            failure_pat = re.compile(r'failure_status-(?P<kind>[^.]+)\.csv$', re.IGNORECASE)
            m = failure_pat.search(csv_path.name)
            if m:
                kind = m.group('kind');
                evt_time = None;
                val_col = None
                try:
                    df_fail = pd.read_csv(csv_path, engine='python', encoding_errors='ignore')
                    cand = [c for c in df_fail.columns if c not in ['%time', 'time', 'stamp', 'rosbagTimestamp']]
                    val_col = cand[0] if cand else None
                    if val_col:
                        arr = pd.to_numeric(df_fail[val_col], errors='coerce').fillna(0).values
                        nz = np.where(arr != 0)[0]
                        if len(nz):
                            i0 = nz[0]
                            if '%time' in df_fail.columns:
                                evt_time = self._to_datetime_guess(pd.Series([df_fail.loc[i0, '%time']]))[0]
                except Exception:
                    pass
                self.failure_rows.append({
                    'FlightID': fid, 'FaultType': kind, 'EventTimeUTC': evt_time,
                    'FailFile': csv_path.name, 'ValueColumn': val_col
                })

    def create_signal_table_from_telemetry(self, csv_files):
        # Создаём пустую таблицу сигналов
        signal_data = []
        print("[i] Начало сбора сигналов...")

        # Пример: собираем данные из всех файлов телеметрии (например, mAVROS)
        for csv_path in csv_files:
            # Пропустим, если файл не подходит для сбора временных рядов
            if "mavros" not in csv_path.name.lower():
                continue

            try:
                # Прочитаем файл, извлечём временные метки и значения
                df = pd.read_csv(csv_path, engine='python', encoding_errors='ignore')
                print(f"[i] Обрабатываем файл: {csv_path.name} - {len(df)} строк")

                # Извлекаем временные метки (например, %time, time, timestamp)
                tcol = next((c for c in ['%time', 'time', 'stamp'] if c in df.columns), None)
                if tcol is None:
                    print(f"[!] В файле {csv_path.name} нет временной колонки.")
                    continue

                # Конвертируем временные метки в datetime
                timestamps = self._to_datetime_guess(df[tcol])

                # Собираем данные по каналам (например, скорость, высота)
                for channel in df.columns:
                    if channel in ['%time', 'time', 'stamp']:  # Пропускаем временные колонки
                        continue

                    # Создаём строки для каждой метки времени и канала
                    for idx, ts in enumerate(timestamps):
                        signal_data.append({
                            'flight_id': self._flight_id_for(csv_path),  # Идентификатор полёта
                            'ts': ts,  # Временная метка
                            'channel': channel,  # Канал данных
                            'value': df[channel].iloc[idx],  # Значение
                        })
            except Exception as e:
                print(f"[!] Ошибка при обработке файла {csv_path.name}: {e}")

        # Преобразуем в DataFrame и сохраняем в БД или файл
        if signal_data:
            signal_df = pd.DataFrame(signal_data)
            signal_df.to_csv(self.output_path / 'signal_data.csv', index=False)  # Сохраняем как CSV
            print(f"[ok] Витрина сигналов сохранена: {self.output_path / 'signal_data.csv'}")
        else:
            print("[!] Не было данных для создания витрины сигналов.")

    def process_data(self):
        self._process_csv_files()

        # Сводка по полётам
        for fid, (lo, hi) in self.flight_bounds.items():
            dur = (hi - lo).total_seconds() if (lo is not None and hi is not None) else None
            has_fail = any(fr['FlightID'] == fid for fr in self.failure_rows)
            self.flight_rows.append({
                'FlightID': fid,
                'FilesCSV': sum(1 for r in self.file_rows if r['FlightID'] == fid),
                'HasFailureGT': has_fail,
                'StartUTC': lo,
                'EndUTC': hi,
                'DurationSec': dur
            })

        # Преобразуем все данные в DataFrame
        df_flights = pd.DataFrame(self.flight_rows).sort_values('FlightID').reset_index(drop=True)
        df_files = pd.DataFrame(self.file_rows).sort_values(['FlightID', 'TopicName']).reset_index(drop=True)
        df_fail = pd.DataFrame(self.failure_rows).sort_values(['FlightID', 'EventTimeUTC', 'FaultType']).reset_index(
            drop=True)
        df_schema = pd.DataFrame(
            [{'TopicName': t, 'Column': c, 'ObservedDTypes': ', '.join(sorted(dts))}
             for t, cols in self.topic_schemas.items() for c, dts in cols.items()]
        ).sort_values(['TopicName', 'Column']).reset_index(drop=True)

        # Сохраняем файлы в директорию
        df_flights.to_csv(self.output_path / 'flights_catalog.csv', index=False)
        df_files.to_csv(self.output_path / 'files_catalog.csv', index=False)
        df_fail.to_csv(self.output_path / 'failure_events.csv', index=False)
        df_schema.to_csv(self.output_path / 'topic_schema.csv', index=False)

        print("[ok] Файлы успешно сохранены.")
        print(f"  - {self.output_path / 'flights_catalog.csv'}")
        print(f"  - {self.output_path / 'files_catalog.csv'}")
        print(f"  - {self.output_path / 'failure_events.csv'}")
        print(f"  - {self.output_path / 'topic_schema.csv'}")


# Пример использования
processor = FlightDataProcessor(BASE, OUT)
processor.process_data()

# Найдём все CSV для сбора сигналов
csv_files = sorted(BASE.rglob('*.csv'))

# Вызываем функцию для создания таблицы signal
processor.create_signal_table_from_telemetry(csv_files)
