import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import traceback

# matplotlib для графика телеметрии
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

# ==== НАСТРОЙКИ ПОДКЛЮЧЕНИЯ К БД ==== #
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "uav_monitoring",
    "user": "postgres",
    "password": "postgres",
}

LOG_FILE = "gui_errors.log"


def log_error(msg: str, exc: Exception | None = None) -> None:
    """Пишем ошибки в лог-файл, чтобы было 'централизованное логирование'."""
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            now = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
            f.write(f"[{now}] {msg}\n")
            if exc is not None:
                traceback.print_exception(
                    type(exc), exc, exc.__traceback__, file=f
                )
            f.write("\n")
    except Exception:
        pass


def get_connection():
    """Создаёт новое подключение к PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Система мониторинга БПЛА")
        self.geometry("1100x600")
        self.minsize(900, 500)

        # текущий выбранный полёт
        self.current_flight_id: str | None = None
        # последний отчёт (как dict) для экспорта
        self.last_report: dict | None = None

        # переменные фильтров по полётам
        self.filter_text_var = tk.StringVar()
        self.filter_fail_only_var = tk.BooleanVar(value=False)

        # фильтры по телеметрии
        self.sig_t_from_var = tk.StringVar()
        self.sig_t_to_var = tk.StringVar()
        self.selected_channel: str | None = None

        # для автоподбора ширины
        self.flight_columns = ()
        self.signal_columns = ("ts", "value")

        # ссылки на виджеты телеметрии
        self.channel_listbox: tk.Listbox | None = None
        self.signal_tree: ttk.Treeview | None = None
        self.fig = None
        self.ax = None
        self.canvas: FigureCanvasTkAgg | None = None

        # стиль таблиц
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure(
            "Treeview",
            font=("Arial", 10),
            rowheight=22,
        )
        style.configure(
            "Treeview.Heading",
            font=("Arial", 10, "bold"),
        )

        self._build_layout()
        self.load_flights()

    # ---------- GUI-РАЗМЕТКА ---------- #
    def _build_layout(self):
        top_frame = ttk.Frame(self)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        title_lbl = ttk.Label(
            top_frame,
            text="Мониторинг данных полётов БПЛА",
            font=("Arial", 14, "bold"),
        )
        title_lbl.pack(side=tk.LEFT)
        export_flights_btn = ttk.Button(
            top_frame,
            text="Экспорт полётов в CSV",
            command=self.export_flights_csv,
        )
        export_flights_btn.pack(side=tk.RIGHT, padx=5)

        refresh_btn = ttk.Button(
            top_frame,
            text="Обновить список полётов",
            command=self.load_flights,
        )
        refresh_btn.pack(side=tk.RIGHT, padx=5)
        main_frame = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        left_frame = ttk.Frame(main_frame)
        main_frame.add(left_frame, weight=1)
        flights_lbl = ttk.Label(left_frame, text="Список полётов")
        flights_lbl.pack(anchor=tk.W)
        filter_frame = ttk.Frame(left_frame)
        filter_frame.pack(fill=tk.X, pady=(0, 4))

        ttk.Label(filter_frame, text="Фильтр ID:").grid(
            row=0, column=0, padx=2, pady=2, sticky="w"
        )
        filter_entry = ttk.Entry(
            filter_frame, textvariable=self.filter_text_var, width=22
        )
        filter_entry.grid(row=0, column=1, padx=2, pady=2, sticky="we")

        fail_chk = ttk.Checkbutton(
            filter_frame,
            text="Только с отказами",
            variable=self.filter_fail_only_var,
            command=self.load_flights,
        )
        fail_chk.grid(row=0, column=2, padx=4, pady=2, sticky="w")

        apply_filter_btn = ttk.Button(
            filter_frame, text="Применить фильтр", command=self.load_flights
        )
        apply_filter_btn.grid(row=0, column=3, padx=4, pady=2, sticky="e")

        filter_frame.columnconfigure(1, weight=1)

        columns = (
            "flight_id",
            "start_utc",
            "end_utc",
            "duration_sec",
            "has_failure_gt",
            "files_csv",
        )
        self.flight_columns = columns

        tree_frame = ttk.Frame(left_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        x_scroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.flights_tree = ttk.Treeview(
            tree_frame,
            columns=columns,
            show="headings",
            xscrollcommand=x_scroll.set,
            yscrollcommand=y_scroll.set,
        )
        self.flights_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        x_scroll.config(command=self.flights_tree.xview)
        y_scroll.config(command=self.flights_tree.yview)

        self.flights_tree.heading("flight_id", text="ID полёта")
        self.flights_tree.heading("start_utc", text="Начало")
        self.flights_tree.heading("end_utc", text="Окончание")
        self.flights_tree.heading("duration_sec", text="Длительность, с")
        self.flights_tree.heading("has_failure_gt", text="Есть отказы")
        self.flights_tree.heading("files_csv", text="CSV файлов")

        for col in columns:
            self.flights_tree.column(col, width=100, anchor=tk.W)

        for col in columns:
            self.flights_tree.heading(
                col,
                text=self.flights_tree.heading(col, "text"),
                command=lambda c=col: self._sort_column(
                    self.flights_tree, c, False
                ),
            )

        self.flights_tree.bind("<<TreeviewSelect>>", self.on_flight_selected)

        right_frame = ttk.Notebook(main_frame)
        main_frame.add(right_frame, weight=2)
        events_tab = ttk.Frame(right_frame)
        right_frame.add(events_tab, text="События отказов")

        events_top = ttk.Frame(events_tab)
        events_top.pack(fill=tk.X)
        events_lbl = ttk.Label(
            events_top, text="События отказов по выбранному полёту"
        )
        events_lbl.pack(side=tk.LEFT, padx=2, pady=2)

        export_events_btn = ttk.Button(
            events_top,
            text="Экспорт событий в CSV",
            command=self.export_events_csv,
        )
        export_events_btn.pack(side=tk.RIGHT, padx=5, pady=2)

        add_event_btn = ttk.Button(
            events_top,
            text="Добавить отказ",
            command=self.add_failure_event_gui,
        )
        add_event_btn.pack(side=tk.RIGHT, padx=5, pady=2)

        del_event_btn = ttk.Button(
            events_top,
            text="Удалить выбранный",
            command=self.delete_selected_failure_event,
        )
        del_event_btn.pack(side=tk.RIGHT, padx=5, pady=2)

        ev_columns = ("fault_type", "event_time_utc", "source")

        ev_frame = ttk.Frame(events_tab)
        ev_frame.pack(fill=tk.BOTH, expand=True)

        ev_x_scroll = ttk.Scrollbar(ev_frame, orient=tk.HORIZONTAL)
        ev_x_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        ev_y_scroll = ttk.Scrollbar(ev_frame, orient=tk.VERTICAL)
        ev_y_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.events_tree = ttk.Treeview(
            ev_frame,
            columns=ev_columns,
            show="headings",
            xscrollcommand=ev_x_scroll.set,
            yscrollcommand=ev_y_scroll.set,
        )
        self.events_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        ev_x_scroll.config(command=self.events_tree.xview)
        ev_y_scroll.config(command=self.events_tree.yview)

        self.events_tree.heading("fault_type", text="Тип отказа")
        self.events_tree.heading("event_time_utc", text="Время события")
        self.events_tree.heading("source", text="Источник")

        for col in ev_columns:
            self.events_tree.column(col, width=150, anchor=tk.W)

        # === НОВАЯ ВКЛАДКА «ТЕЛЕМЕТРИЯ» ===
        telemetry_tab = ttk.Frame(right_frame)
        right_frame.add(telemetry_tab, text="Телеметрия")

        tele_pane = ttk.Panedwindow(telemetry_tab, orient=tk.HORIZONTAL)
        tele_pane.pack(fill=tk.BOTH, expand=True)

        ch_frame = ttk.Frame(tele_pane, padding=2)
        tele_pane.add(ch_frame, weight=1)

        ttk.Label(ch_frame, text="Каналы телеметрии").pack(anchor="w")

        ch_list_frame = ttk.Frame(ch_frame)
        ch_list_frame.pack(fill=tk.BOTH, expand=True, pady=(2, 0))

        ch_scroll = ttk.Scrollbar(ch_list_frame, orient=tk.VERTICAL)
        ch_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.channel_listbox = tk.Listbox(
            ch_list_frame,
            exportselection=False,
        )
        self.channel_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.channel_listbox.config(yscrollcommand=ch_scroll.set)
        ch_scroll.config(command=self.channel_listbox.yview)

        self.channel_listbox.bind("<<ListboxSelect>>", self.on_channel_selected)

        ch_btn_frame = ttk.Frame(ch_frame)
        ch_btn_frame.pack(fill=tk.X, pady=2)

        ch_reload_btn = ttk.Button(
            ch_btn_frame,
            text="Обновить список каналов",
            command=self.load_channels,
        )
        ch_reload_btn.pack(side=tk.LEFT, padx=2, pady=2)

        right_tele_frame = ttk.Frame(tele_pane)
        tele_pane.add(right_tele_frame, weight=3)

        # верх: фильтр по времени + кнопка загрузки
        filter_sig_frame = ttk.Frame(right_tele_frame)
        filter_sig_frame.pack(fill=tk.X, pady=2)

        ttk.Label(filter_sig_frame, text="Время от (YYYY-MM-DD ...):").grid(
            row=0, column=0, padx=2, pady=2, sticky="w"
        )
        ttk.Entry(
            filter_sig_frame, textvariable=self.sig_t_from_var, width=20
        ).grid(row=0, column=1, padx=2, pady=2, sticky="w")

        ttk.Label(filter_sig_frame, text="до:").grid(
            row=0, column=2, padx=2, pady=2, sticky="w"
        )
        ttk.Entry(
            filter_sig_frame, textvariable=self.sig_t_to_var, width=20
        ).grid(row=0, column=3, padx=2, pady=2, sticky="w")

        load_sig_btn = ttk.Button(
            filter_sig_frame,
            text="Загрузить и построить график",
            command=self.load_and_plot_signal,
        )
        load_sig_btn.grid(row=0, column=4, padx=5, pady=2, sticky="e")

        filter_sig_frame.columnconfigure(1, weight=1)
        filter_sig_frame.columnconfigure(3, weight=1)

        plot_frame = ttk.LabelFrame(right_tele_frame, text="График канала")
        plot_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=(2, 2))

        self.fig = Figure(figsize=(5, 3), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.ax.set_xlabel("Время")
        self.ax.set_ylabel("Значение")
        self.ax.grid(True)

        self.canvas = FigureCanvasTkAgg(self.fig, master=plot_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        raw_frame = ttk.LabelFrame(right_tele_frame, text="Сырые значения")
        raw_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=(0, 2))

        sig_columns = ("ts", "value")
        self.signal_columns = sig_columns

        sig_tbl_frame = ttk.Frame(raw_frame)
        sig_tbl_frame.pack(fill=tk.BOTH, expand=True)

        sig_x_scroll = ttk.Scrollbar(sig_tbl_frame, orient=tk.HORIZONTAL)
        sig_x_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        sig_y_scroll = ttk.Scrollbar(sig_tbl_frame, orient=tk.VERTICAL)
        sig_y_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.signal_tree = ttk.Treeview(
            sig_tbl_frame,
            columns=sig_columns,
            show="headings",
            xscrollcommand=sig_x_scroll.set,
            yscrollcommand=sig_y_scroll.set,
        )
        self.signal_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        sig_x_scroll.config(command=self.signal_tree.xview)
        sig_y_scroll.config(command=self.signal_tree.yview)

        self.signal_tree.heading("ts", text="Время")
        self.signal_tree.heading("value", text="Значение")

        for col in sig_columns:
            self.signal_tree.column(col, width=120, anchor=tk.W)
        report_tab = ttk.Frame(right_frame)
        right_frame.add(report_tab, text="Отчёт по полёту")

        report_top = ttk.Frame(report_tab)
        report_top.pack(fill=tk.X)

        report_btn = ttk.Button(
            report_top,
            text="Сформировать отчёт по полёту",
            command=self.load_report,
        )
        report_btn.pack(side=tk.LEFT, padx=5, pady=5)

        export_report_btn = ttk.Button(
            report_top,
            text="Экспорт отчёта (JSON)",
            command=self.export_report_json,
        )
        export_report_btn.pack(side=tk.LEFT, padx=5, pady=5)
        summary_lbl = ttk.Label(report_tab, text="Краткое описание полёта:")
        summary_lbl.pack(anchor="w", padx=5)
        self.report_summary = tk.Text(report_tab, height=6, wrap="word")
        self.report_summary.pack(fill=tk.X, padx=5, pady=(0, 5))
        self.report_text = tk.Text(report_tab, wrap="word")
        self.report_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    # ---------- СЛУЖЕБНЫЕ МЕТОДЫ ---------- #
    def _autosize_columns(
        self, tree: ttk.Treeview, columns, padding: int = 20
    ) -> None:
        """Подбор ширины колонок под максимальную длину текста."""
        for col in columns:
            max_len = len(tree.heading(col, "text"))
            for item in tree.get_children():
                val = str(tree.set(item, col))
                if len(val) > max_len:
                    max_len = len(val)
            new_width = max(60, min(max_len * 7 + padding, 400))
            tree.column(col, width=new_width)

    def _sort_column(self, tree: ttk.Treeview, col: str, reverse: bool):
        """Сортировка по столбцу по клику по заголовку."""
        data = [(tree.set(k, col), k) for k in tree.get_children("")]

        def try_cast(v):
            try:
                return float(str(v).replace(",", "."))
            except Exception:
                return str(v)

        data.sort(key=lambda t: try_cast(t[0]), reverse=reverse)

        for index, (_, iid) in enumerate(data):
            tree.move(iid, "", index)

        tree.heading(
            col,
            command=lambda: self._sort_column(tree, col, not reverse),
        )

    # ---------- ЗАГРУЗКА ДАННЫХ ИЗ БД ---------- #
    def load_flights(self):
        """Загрузка списка полётов из таблицы flight c учётом фильтров."""
        try:
            conn = get_connection()
            cur = conn.cursor()

            sql = """
                SELECT flight_id, start_utc, end_utc,
                       duration_sec, has_failure_gt, files_csv
                FROM flight
                WHERE 1=1
            """
            params = []

            flt = self.filter_text_var.get().strip()
            if flt:
                sql += " AND flight_id ILIKE %s"
                params.append(f"%{flt}%")

            if self.filter_fail_only_var.get():
                sql += " AND has_failure_gt = TRUE"

            sql += " ORDER BY start_utc;"

            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            log_error("Не удалось загрузить полёты", e)
            messagebox.showerror("Ошибка БД", f"Не удалось загрузить полёты:\n{e}")
            return

        for item in self.flights_tree.get_children():
            self.flights_tree.delete(item)

        for r in rows:
            flight_id, start_utc, end_utc, duration_sec, has_failure_gt, files_csv = r
            self.flights_tree.insert(
                "",
                tk.END,
                values=(
                    flight_id,
                    start_utc,
                    end_utc,
                    round(duration_sec, 1) if duration_sec is not None else "",
                    "Да" if has_failure_gt else "Нет",
                    files_csv or 0,
                ),
            )

        self._autosize_columns(self.flights_tree, self.flight_columns)

    def on_flight_selected(self, event):
        """Обработчик выбора полёта в списке."""
        sel = self.flights_tree.selection()
        if not sel:
            return
        values = self.flights_tree.item(sel[0], "values")
        self.current_flight_id = values[0]
        self.load_events_for_flight(self.current_flight_id)
        self.load_channels()
        self.clear_telemetry_views()

    def load_events_for_flight(self, flight_id: str):
        """Загрузка событий отказов для выбранного полёта."""
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT event_id, fault_type, event_time_utc, source
                FROM failure_event
                WHERE flight_id = %s
                ORDER BY event_time_utc;
                """,
                (flight_id,),
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            log_error("Не удалось загрузить события отказов", e)
            messagebox.showerror("Ошибка БД", f"Не удалось загрузить события:\n{e}")
            return

        for item in self.events_tree.get_children():
            self.events_tree.delete(item)

        for event_id, fault_type, event_time_utc, source in rows:
            self.events_tree.insert(
                "",
                tk.END,
                iid=str(event_id),  # event_id хранится здесь, но не показывается
                values=(fault_type, event_time_utc, source or ""),
            )

        self._autosize_columns(
            self.events_tree, ("fault_type", "event_time_utc", "source")
        )

    # ---------- ТЕЛЕМЕТРИЯ ---------- #
    def clear_telemetry_views(self):
        """Очистка графика и таблицы телеметрии."""
        if self.signal_tree is not None:
            for item in self.signal_tree.get_children():
                self.signal_tree.delete(item)
        if self.ax is not None and self.canvas is not None:
            self.ax.clear()
            self.ax.set_xlabel("Время")
            self.ax.set_ylabel("Значение")
            self.ax.grid(True)
            self.canvas.draw()

    def load_channels(self):
        """Загрузка списка каналов для выбранного полёта."""
        if not self.current_flight_id:
            return

        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT channel
                FROM signal
                WHERE flight_id = %s
                ORDER BY channel;
                """,
                (self.current_flight_id,),
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            log_error("Не удалось загрузить список каналов", e)
            messagebox.showerror(
                "Ошибка БД", f"Не удалось загрузить список каналов:\n{e}"
            )
            return

        # заполняем listbox
        self.selected_channel = None
        self.sig_t_from_var.set("")
        self.sig_t_to_var.set("")
        if self.channel_listbox is not None:
            self.channel_listbox.delete(0, tk.END)
            for (ch,) in rows:
                self.channel_listbox.insert(tk.END, ch)

    def on_channel_selected(self, event):
        """Выбор канала в списке — просто запоминаем, можно сразу строить."""
        if self.channel_listbox is None:
            return
        sel = self.channel_listbox.curselection()
        if not sel:
            self.selected_channel = None
            return
        self.selected_channel = self.channel_listbox.get(sel[0])

    def load_and_plot_signal(self):
        """Загрузка телеметрии для выбранного полёта и канала + построение графика."""
        if not self.current_flight_id:
            messagebox.showwarning(
                "Нет полёта", "Сначала выберите полёт в списке."
            )
            return
        if not self.selected_channel:
            messagebox.showwarning(
                "Нет канала", "Сначала выберите канал в списке слева."
            )
            return

        try:
            conn = get_connection()
            cur = conn.cursor()

            sql = """
                SELECT ts, value
                FROM signal
                WHERE flight_id = %s
                  AND channel = %s
            """
            params = [self.current_flight_id, self.selected_channel]

            t_from = self.sig_t_from_var.get().strip()
            if t_from:
                sql += " AND ts >= %s"
                params.append(t_from)

            t_to = self.sig_t_to_var.get().strip()
            if t_to:
                sql += " AND ts <= %s"
                params.append(t_to)

            sql += " ORDER BY ts LIMIT 10000;"
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            log_error("Не удалось загрузить телеметрию", e)
            messagebox.showerror("Ошибка БД", f"Не удалось загрузить телеметрию:\n{e}")
            return

        # обновляем таблицу сырых значений
        if self.signal_tree is not None:
            for item in self.signal_tree.get_children():
                self.signal_tree.delete(item)
            for ts, value in rows:
                self.signal_tree.insert("", tk.END, values=(ts, value))
            self._autosize_columns(self.signal_tree, self.signal_columns)

        # обновляем график
        if self.ax is not None and self.canvas is not None:
            self.ax.clear()
            self.ax.set_title(self.selected_channel)
            self.ax.set_xlabel("Время")
            self.ax.set_ylabel("Значение")
            self.ax.grid(True)

            if rows:
                ts_list = [r[0] for r in rows]
                val_list = [r[1] for r in rows]
                self.ax.plot(ts_list, val_list)
                # повернём подписи по X, чтобы не слиплось
                for label in self.ax.get_xticklabels():
                    label.set_rotation(30)
                    label.set_ha("right")

            self.fig.tight_layout()
            self.canvas.draw()

    # ---------- ОТЧЁТ И ДИАГНОСТИКА ---------- #
    def simple_diagnostics(self, flight_id: str) -> list[str]:
        """Простейшая 'диагностика' по разбросу значений по каналам."""
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT channel,
                       count(*) AS cnt,
                       min(value) AS v_min,
                       max(value) AS v_max
                FROM signal
                WHERE flight_id = %s
                GROUP BY channel;
                """,
                (flight_id,),
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            log_error("Диагностика не выполнена", e)
            return ["Не удалось выполнить диагностику (ошибка БД)."]

        alerts: list[str] = []
        for channel, cnt, v_min, v_max in rows:
            if v_min is None or v_max is None:
                continue
            span = v_max - v_min
            if span > 1000:
                alerts.append(
                    f"Канал {channel}: большой разброс значений (Δ={span:.1f})."
                )
            if v_max > 1000:
                alerts.append(
                    f"Канал {channel}: значение превышает 1000 (max={v_max:.1f})."
                )
            if v_min < -1000:
                alerts.append(
                    f"Канал {channel}: значение ниже -1000 (min={v_min:.1f})."
                )

        if not alerts:
            alerts.append(
                "Явных аномалий по простым правилам не обнаружено."
            )
        return alerts

    def load_report(self):
        """Вызов SQL-функции отчёта по выбранному полёту и отображение JSON."""
        if not self.current_flight_id:
            messagebox.showwarning(
                "Нет полёта", "Сначала выберите полёт в списке."
            )
            return

        try:
            conn = get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                "SELECT flight_report_json(%s) AS report",
                (self.current_flight_id,),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
        except Exception as e:
            log_error("Не удалось получить отчёт", e)
            messagebox.showerror("Ошибка БД", f"Не удалось получить отчёт:\n{e}")
            return

        report = row.get("report") if row else None

        self.report_text.delete("1.0", tk.END)
        self.report_summary.delete("1.0", tk.END)
        self.last_report = None

        if report is None:
            self.report_text.insert(
                tk.END, "Отчёт не сформирован или функция вернула NULL."
            )
            return
        try:
            if isinstance(report, (dict, list)):
                obj = report
            else:
                obj = json.loads(str(report))

            self.last_report = obj

            # полный JSON
            formatted = json.dumps(obj, ensure_ascii=False, indent=4)
            self.report_text.insert(tk.END, formatted)

            # краткое резюме
            flight = obj.get("flight", {}) or {}
            failures = obj.get("failures") or []
            channels = obj.get("channels") or []
            files_csv = obj.get("files_csv", 0)

            fid = flight.get("flight_id", self.current_flight_id)
            dur = flight.get("duration_sec")
            has_failure = flight.get("has_failure_gt")

            lines = []
            lines.append(f"Полёт: {fid}")
            lines.append(f"Файлов CSV: {files_csv}")
            if dur is not None:
                try:
                    lines.append(f"Длительность: {float(dur):.1f} с")
                except Exception:
                    lines.append(f"Длительность: {dur}")
            if has_failure:
                if failures:
                    fail_parts = [
                        f"{f.get('type', '?')} — {f.get('count', 0)}"
                        for f in failures
                    ]
                    lines.append("Отказы: " + "; ".join(fail_parts))
                else:
                    lines.append(
                        "Отказы: признак есть, но детализация отсутствует."
                    )
            else:
                lines.append("Отказов не зарегистрировано.")

            lines.append(f"Каналов телеметрии: {len(channels)}")
            lines.append("")
            lines.append("Диагностика по простым правилам:")
            for msg in self.simple_diagnostics(self.current_flight_id):
                lines.append(" • " + msg)

            self.report_summary.insert(tk.END, "\n".join(lines))

        except Exception as e:
            log_error("Ошибка при разборе отчёта", e)
            self.report_text.insert(tk.END, str(report))

    # ---------- ЭКСПОРТ ДАННЫХ ---------- #

    def add_failure_event_gui(self):
        """GUI-демонстрация процедуры add_failure_event(...)."""
        if not self.current_flight_id:
            messagebox.showwarning("Нет полёта", "Сначала выберите полёт в списке.")
            return

        win = tk.Toplevel(self)
        win.title("Добавить отказ")
        win.resizable(False, False)
        win.transient(self)
        win.grab_set()

        fault_var = tk.StringVar(value="engines")
        time_var = tk.StringVar(value=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        source_var = tk.StringVar(value="gui")

        frm = ttk.Frame(win, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frm, text=f"Полёт: {self.current_flight_id}").grid(row=0, column=0, columnspan=2, sticky="w",
                                                                     pady=(0, 8))

        ttk.Label(frm, text="Тип отказа:").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Combobox(
            frm,
            textvariable=fault_var,
            values=["engines", "aileron", "elevator", "rudder"],
            state="readonly",
            width=18,
        ).grid(row=1, column=1, sticky="we", pady=4)

        ttk.Label(frm, text="Время (YYYY-MM-DD HH:MM:SS):").grid(row=2, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=time_var, width=22).grid(row=2, column=1, sticky="we", pady=4)

        ttk.Label(frm, text="Источник:").grid(row=3, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=source_var, width=22).grid(row=3, column=1, sticky="we", pady=4)

        frm.columnconfigure(1, weight=1)

        def on_ok():
            fault = fault_var.get().strip()
            t = time_var.get().strip()
            src = source_var.get().strip() or "gui"

            # простая проверка формата
            try:
                datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
            except Exception:
                messagebox.showerror("Ошибка", "Некорректное время. Пример: 2025-01-15 12:30:00")
                return

            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(
                    "CALL add_failure_event(%s, %s, %s, %s);",
                    (self.current_flight_id, fault, t, src),
                )
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                log_error("Не удалось добавить отказ (CALL add_failure_event)", e)
                messagebox.showerror("Ошибка БД", f"Не удалось добавить отказ:\n{e}")
                return

            win.destroy()
            self.load_events_for_flight(self.current_flight_id)
            self.load_flights()  # чтобы обновился has_failure_gt

        btns = ttk.Frame(frm)
        btns.grid(row=4, column=0, columnspan=2, sticky="e", pady=(10, 0))
        ttk.Button(btns, text="Отмена", command=win.destroy).pack(side=tk.RIGHT, padx=6)
        ttk.Button(btns, text="Добавить", command=on_ok).pack(side=tk.RIGHT)

    def delete_selected_failure_event(self):
        """GUI-демонстрация удаления отказов (с проверкой trg_failure_unflag_on_delete)."""
        if not self.current_flight_id:
            messagebox.showwarning("Нет полёта", "Сначала выберите полёт в списке.")
            return

        sel = self.events_tree.selection()
        if not sel:
            messagebox.showinfo("Нет выбора", "Сначала выберите событие в таблице.")
            return

        event_id = sel[0]  # так как iid = event_id

        ok = messagebox.askyesno("Подтверждение", "Удалить выбранное событие отказа?")
        if not ok:
            return

        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("DELETE FROM failure_event WHERE event_id = %s;", (event_id,))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            log_error("Не удалось удалить событие отказа", e)
            messagebox.showerror("Ошибка БД", f"Не удалось удалить событие:\n{e}")
            return

        # После удаления последнего события триггер trg_failure_unflag_on_delete сам сбросит has_failure_gt
        self.load_events_for_flight(self.current_flight_id)
        self.load_flights()

    def export_report_json(self):
        """Экспорт последнего отчёта в JSON-файл."""
        if not self.last_report:
            messagebox.showinfo(
                "Нет отчёта", "Сначала сформируйте отчёт по полёту."
            )
            return

        filename = filedialog.asksaveasfilename(
            title="Сохранить отчёт",
            defaultextension=".json",
            filetypes=(("JSON файлы", "*.json"), ("Все файлы", "*.*")),
        )
        if not filename:
            return

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.last_report, f, ensure_ascii=False, indent=4)
            messagebox.showinfo("Экспорт", "Отчёт успешно сохранён.")
        except Exception as e:
            log_error("Не удалось сохранить отчёт", e)
            messagebox.showerror("Ошибка", f"Не удалось сохранить отчёт:\n{e}")

    def export_flights_csv(self):
        """Экспорт списка полётов в CSV."""
        filename = filedialog.asksaveasfilename(
            title="Сохранить список полётов",
            defaultextension=".csv",
            filetypes=(("CSV файлы", "*.csv"), ("Все файлы", "*.*")),
        )
        if not filename:
            return

        try:
            with open(filename, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, delimiter=";")
                # заголовки
                headers = [
                    self.flights_tree.heading(col, "text")
                    for col in self.flight_columns
                ]
                writer.writerow(headers)
                # строки
                for iid in self.flights_tree.get_children():
                    row = [
                        self.flights_tree.set(iid, col)
                        for col in self.flight_columns
                    ]
                    writer.writerow(row)
            messagebox.showinfo("Экспорт", "Список полётов успешно сохранён.")
        except Exception as e:
            log_error("Не удалось экспортировать полёты", e)
            messagebox.showerror(
                "Ошибка", f"Не удалось экспортировать полёты:\n{e}"
            )

    def export_events_csv(self):
        """Экспорт событий отказов в CSV."""
        filename = filedialog.asksaveasfilename(
            title="Сохранить события отказов",
            defaultextension=".csv",
            filetypes=(("CSV файлы", "*.csv"), ("Все файлы", "*.*")),
        )
        if not filename:
            return

        cols = ("fault_type", "event_time_utc", "source")
        try:
            with open(filename, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, delimiter=";")
                headers = [
                    self.events_tree.heading(col, "text") for col in cols
                ]
                writer.writerow(headers)
                for iid in self.events_tree.get_children():
                    row = [self.events_tree.set(iid, col) for col in cols]
                    writer.writerow(row)
            messagebox.showinfo("Экспорт", "События успешно сохранены.")
        except Exception as e:
            log_error("Не удалось экспортировать события", e)
            messagebox.showerror(
                "Ошибка", f"Не удалось экспортировать события:\n{e}"
            )



if __name__ == "__main__":
    app = MainApp()
    app.mainloop()


