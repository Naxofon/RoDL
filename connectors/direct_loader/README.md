# loader_direct

`loader_direct` — это асинхронный загрузчик статистики из API Яндекс Директ в PostgreSQL. Он умеет:

- получать список клиентов агентского аккаунта и хранить их токены доступа;
- снимать отчёты (полные и облегчённые профили) с произвольными наборами полей;
- сравнивать свежие данные API с агрегатами в базе и перезагружать только изменённые дни;
- запускаться по расписанию через Prefect или вручную через CLI.

> Токены теперь складываются в единую таблицу `Accesses` (колонки `login`, `token`, `container`, `type`), где `type=direct`, а `container` обозначает источник токена (например, `agency` или `client`).


## Обзор архитектуры

```
┌──────────────────────┐        ┌──────────────────────────┐
│    Prefect / CLI     │        │   Таблицы accesses_*     │
└─────────┬────────────┘        └────────────┬─────────────┘
          │                                  │
          │ запускает                        │ даёт токены
          ▼                                  ▼
┌──────────────────────────────────────────────────────────┐
│                 loader_service.YaStatUploader             │
│  • получает цели кампаний                                │
│  • снимает отчёты v5/reports                              │
│  • приводит DataFrame                                     │
│  • пишет в PostgreSQL через AsyncDirectDatabase                 │
└─────────┬────────────────────────────────────────────────┘
          │ использует
          ▼
┌────────────────────────────┐
│ base_service.AsyncDirectDatabase │
│  • создаёт таблицы         │
│  • добавляет/удаляет колонки│
│  • COPY/INSERT, чистка дат │
└─────────┬──────────────────┘
          │ агрегаты            ▲
          ▼                     │ сравнение
┌───────────────────────────────┴──────────────────────────┐
│        change_tracker.DirectChangeTracker                │
│  • запрашивает отчёты LSC                                 │
│  • сравнивает с daily summary в БД                        │
│  • отдаёт список дат для перезагрузки                     │
└──────────────────────────────────────────────────────────┘
```

## Подключения и окружение

| Что подключаем | Где задаётся | По умолчанию | Назначение |
| --- | --- | --- | --- |
| `DEFAULT_ANALYTICS_DSN` | `.env` → `ANALYTICS_DB_HOST/PORT/USER/PASSWORD/NAME` (или `ANALYTICS_DB_DSN`) | параметры подключения к Postgres | База с полной статистикой (профиль `analytics`). |
| `DEFAULT_LIGHT_DSN` | `.env` → `LIGHT_DB_HOST/PORT/USER/PASSWORD/NAME` (или `LIGHT_DB_DSN`) | параметры подключения к Postgres | Облегчённый профиль (`light`). |
| Токены клиентов/агентств | Таблицы `accesses_make_agency` и `accesses_not_agency` | создаются автоматически | Хранение соответствия *логин → токен* (агентские токены используют subtype `agency_token`). |
| API Яндекс Директ | эндпоинты `json/v5/agencyclients`, `json/v5/campaigns`, `json/v5/reports` | — | Источник статистики. |

> **Важно:** все DSN автоматически переводятся в схему `postgresql+asyncpg://…`, поэтому достаточно указать обычный PostgreSQL URL.

## Основные компоненты

### 1. `base_service.AsyncDirectDatabase`

Асинхронная обёртка над SQLAlchemy + asyncpg. Ключевые свойства:

| Метод | Параметры | Что делает |
| --- | --- | --- |
| `__init__(dsn)` | `dsn` — строка подключения; можно не передавать → возьмётся профиль analytics. | Создаёт движок, `Metadata`, очередь на запись и семафор (`db_semaphore`) для ограничений INSERT. |
| `init_db()` | — | Рефлектит все таблицы, чтобы `metadata.tables` содержал актуальную схему. |
| `write_dataframe_to_table_fast(df, table_name, min_rows_for_copy=500)` | `df: pandas.DataFrame`, `table_name: str`. | Создаёт таблицу, добавляет новые столбцы, затем либо копирует данные через COPY (если строк ≥ 500), либо вызывает `_write_dataframe_to_table_chunked()`. |
| `_write_dataframe_to_table_chunked(df, table_name, table=None)` | `table` можно заранее передать. | Делит DataFrame на куски, вставляет через SQLAlchemy `insert` с повторными попытками (до 3) и прогрессом `tqdm_asyncio`. |
| `add_new_columns(df, table_name)` | — | Анализирует `df.dtypes` и добавляет отсутствующие столбцы через `ALTER TABLE`. |
| `delete_records_between_dates(table, start_date, end_date)` | Даты `datetime/date`. | Удаляет строки по диапазону `Date >= start_date AND Date <= end_date`. |
| `get_daily_summary(table, start_date, end_date)` | Возвращает `pd.DataFrame` с колонками `Date, Impressions, Clicks, Cost, SumConversion`. | Агрегирует данные для детектора изменений. |
| `add_agency_client_list_to_table(client_list, token, table)` | — | Полностью пересобирает таблицу логинов → токен (полезно для `accesses_make_agency`). |

Типы столбцов подбираются автоматически: `int64 → BigInteger`, `datetime64 → Date`, остальное → `String` или `Float`.

### 2. `loader_service.YaStatUploader`

Один экземпляр отвечает за одного клиента и выбранный профиль (`analytics` или `light`).

| Метод | Важные параметры | Описание |
| --- | --- | --- |
| `__init__(login, token, time_to, time_from, profile='analytics')` | `login` в формате с `_`; `token` — OAuth; даты `YYYY-MM-DD`. | Настраивает профиль, инициализирует `AsyncDirectDatabase` с нужным DSN. |
| `api_login` (property) | — | Возвращает логин с `-`, как требует API. |
| `table_name` (property) | — | Имя таблицы = логин с `_`. |
| `get_client_list()` | — | Через `agencyclients` получает список логинов агентского аккаунта (без токенов). |
| `get_goal_ids_by_client(login)` | — | Ходит в `campaigns`, собирает `PriorityGoals` из текстовых, динамических и смарт-кампаний, исключая служебный GoalId 12. |
| `upload_data()` | — | Основной метод выгрузки диапазона: формирует отчёт `CUSTOM_REPORT`, добавляет цели батчами по 10, ждёт готовности (201/202 → пауза), приводит типы колонок и пишет результат в БД. |
| `unload_data_by_days()` | — | Делит исходный диапазон на отрезки по 5 дней, чтобы не перегружать API, и вызывает `upload_data()` для каждого. |

Дополнительно:

- `ANALYTICS_FIELD_NAMES` и `LIGHT_FIELDS` определяют наборы столбцов; типы лежат в словарях `*_COLUMN_TYPES`.
- `REPORT_QUEUE_SEM` (глобальный семафор) ограничивает одновременные запросы к `/reports`; сверху работает `report_rate_limiter` (20 запросов в 10 секунд на рекламодателя) и `offline_report_tracker` (не больше 5 оффлайн-отчётов одновременно на пользователя).
- После каждой выгрузки добавляется поле `SumConversion` (сумма всех колонок, начинающихся на `Conv`).

### 3. Функции верхнего уровня `loader_service.py`

| Функция | Что делает |
| --- | --- |
| `unload_data_by_day_for_all_clients(start_date, end_date, profile)` | 1) загружает список клиентов, 2) объединяет словари логин→токен (`accesses_make_agency` + `accesses_not_agency`), 3) удаляет данные за диапазон, 4) запускает пул процессов (по числу CPU), где для каждого клиента выполняется `unload_data_by_days()`. |
| `unload_data_by_day_for_single_client(login, start_date, end_date, profile)` | То же, но только для одного логина; токен ищется в таблицах доступа. |
| `unload_data_with_changes_tracking_for_all_clients(profile, lookback_days=60)` | Использует `DirectChangeTracker` (окно `lookback_days`) для всех клиентов → вычисляет диапазоны дат, которые «расходятся», очищает их и перезагружает. Перед форком обязательно вызывает `await target_db.engine.dispose()`, чтобы не копировать активные коннекты. |
| `unload_data_with_changes_tracking_for_single_client(login, profile, lookback_days=60)` | Та же логика для одного клиента. |
| `main()` | CLI-роутер: разбирает аргументы и вызывает подходящий сценарий. |

Доступные CLI-аргументы:

```
--update-interval START_DATE END_DATE      # Force reload для всех клиентов
--track-changes                            # Пересчёт по детектору изменений
--track-changes-days N                     # Окно в днях для детектора (по умолчанию 60)
--single-client LOGIN START_DATE END_DATE  # Ручной reload одного клиента
--track-changes-client LOGIN               # Перезагрузить только изменённые даты клиента
--db-profile analytics|light|both          # Профиль базы (можно последовательно оба)
```

### 4. `change_tracker.DirectChangeTracker`

Определяет расхождения между отчётом API и данными в базе.

| Метод | Кратко |
| --- | --- |
| `get_goal_ids_by_client(login)` | Безопасно проходит список кампаний (везде проверка на `dict`), возвращает уникальные GoalId. |
| `_fetch_report_slice(login, start_date, end_date, goals)` | Запрашивает кастомный отчёт; если передан список целей, добавляет `Conversions`, `AttributionModels=['LSC']`. Делит запросы на «срезы», чтобы не упереться в лимиты. |
| `get_report_data_for_period(login, start_date, end_date, goal_ids)` | Делит цели на батчи по 10, собирает все DataFrame в один и считает `SumConversion`. |
| `detect_changes(login)` | 1) задаёт «окно» сравнения: последние `lookback_days` до вчера. 2) Берёт агрегаты БД через `AsyncDirectDatabase.get_daily_summary`. 3) По каждому дню сравнивает показы, клики, стоимость (с допуском `cost_tolerance`) и конверсии (если `compare_conversions=True`). 4) Возвращает `{"changes_detected": bool, "days_to_update": ["YYYY-MM-DD", ...]}`. |

Особенности:

- `skip_cost_check_clients` содержит логины, для которых стоимость лучше не сравнивать (например, «lovol_russia»).
- `api_semaphore` (по умолчанию 5) ограничивает параллельные запросы к API, чтобы не ловить HTTP 429.

### 5. Планирование запусков

Регулярные прогоны выполняет Prefect (см. деплойменты в `orchestration/flows/`). Для ручного запуска используйте CLI `loader_service.py` с нужными аргументами (раздел CLI).

## Полный справочник функций и их связей

### `base_service.py`

| Функция/метод | Кем вызывается | Назначение и связь |
| --- | --- | --- |
| `_ensure_asyncpg_scheme(dsn)` | `AsyncDirectDatabase.__init__` | Приводит DSN к виду `postgresql+asyncpg://`, чтобы SQLAlchemy понимал асинхронный драйвер. |
| `AsyncDirectDatabase.__init__(dsn)` | Все места, где создаётся БД (`loader_service`, `change_tracker`) | Создаёт движок, `sessionmaker`, очередь записи, семафор. |
| `AsyncDirectDatabase.init_db()` | Почти каждый публичный метод БД | Рефлексия схемы. Обязательна перед просмотрами/изменением таблиц. |
| `AsyncDirectDatabase.write_to_db()` | Можно запустить из фоновой задачи (пока не используется) | Читает элементы из `write_queue` и вызывает `write_dataframe_to_table`. |
| `AsyncDirectDatabase._raw_connection()` | `write_dataframe_to_table_fast` (путь COPY) | Получает «сырое» соединение asyncpg для операций `copy_records_to_table`. |
| `create_table_from_dataframe(df, table_name)` | `write_dataframe_to_table_fast` | Создаёт таблицу с колонками из DataFrame; при гонке отражает уже созданную. |
| `get_column_type(dtype)` | `create_table_from_dataframe`, `_inspect_and_add_columns` | Определяет SQL тип по pandas dtype. |
| `add_new_columns(df, table_name)` | `write_dataframe_to_table_fast` (если таблица есть) | Находит отсутствующие в таблице колонки и добавляет их через `ALTER TABLE`. |
| `_inspect_and_add_columns(...)` | Внутренний sync-метод для `add_new_columns` | Работает в синхронном контексте, чтобы использовать инспектор SQLAlchemy. |
| `reflect_table(table_name)` | `write_dataframe_to_table_fast`, `_write_dataframe_to_table_chunked`, delete-методы | Обновляет `metadata.tables[table_name]`. |
| `write_dataframe_to_table_fast(df, table_name, min_rows_for_copy)` | `YaStatUploader.upload_data` | Определяет стратегию загрузки: COPY или chunked INSERT. |
| `_write_dataframe_to_table_chunked(...)` | Непосредственно `write_dataframe_to_table_fast` | Разбивает кадр на куски и вставляет с повторными попытками (3 раза). |
| `write_dataframe_to_table(df, table_name)` | Сохранённый «старый» API, равен fast-версии | Обратная совместимость. |
| `delete_records_by_date(table_name, target_date)` | При необходимости очистить конкретный день | Фильтр по `table.c.Date`. |
| `delete_column(table_name, column_name)` | Ручное обслуживание схемы | `ALTER TABLE DROP COLUMN`. |
| `delete_records_between_dates(table_name, start_date, end_date)` | Все сценарии перезагрузки диапазона | Удаляет данные перед новой выгрузкой. |
| `add_agency_client_list_to_table(client_list, token, table)` | `unload_data_by_day_for_all_clients` | Пересобирает таблицу соответствий логин → общий агентский токен. |
| `add_other_client_list_to_table(...)` | Можно использовать для неагентских клиентов | Просто добавляет пары логин/токен. |
| `create_login_token_table(table)` | Вызывается автоматически из двух методов выше | Создаёт таблицу `login/token`. |
| `delete_records_by_login(table_name, client_login)` | Администрирование списка доступов | Удаление одной записи в таблице логинов. |
| `get_login_key_dictionary(table_name)` | Все функции, где нужен токен | Возвращает словарь `login -> token`. |
| `erase_all_tables()` | Технический метод (использовать осторожно) | Удаляет все таблицы, кроме `accesses_make_agency`. |
| `remove_client_from_accesses_not_agency(login)` | Управление доступами | Безопасно удаляет клиента и логгирует результат. |
| `get_daily_summary(table_name, start_date, end_date)` | `DirectChangeTracker.detect_changes` | Возвращает агрегации для сравнения с API. |

Связи: `loader_service` и `change_tracker` создают собственные экземпляры `AsyncDirectDatabase`, но работают с одной и той же базе (по профилю). Методы очистки вызываются перед перезаписью данных, а `get_daily_summary` — только детектором изменений.

### `loader_service.py`

| Функция/метод | Связи |
| --- | --- |
| `resolve_profiles(option)` | Используется в CLI (`main`) для поддержки `--db-profile both`. |
| `build_forced_refresh_window(days)` | Можно использовать в кастомных скриптах для списка дат (по умолчанию 60); сейчас не вызывается напрямую. |
| `YaStatUploader.__init__` | Создаётся везде, где нужна выгрузка клиента. |
| `YaStatUploader.api_login/table_name` | Связывают имя таблицы с форматом логина. |
| `get_client_list()` | `unload_data_by_day_for_all_clients` собирает список агентских логинов. |
| `get_goal_ids_by_client(login)` | `upload_data` и `DirectChangeTracker` (его версия) используют GoalId для конверсий. |
| `cast_conversion_columns_to_int` + `fill_na_for_conversion_columns` | Помощники `upload_data`, чтобы итоговая таблица имела корректные типы. |
| `upload_data()` | Основной сбор отчётов; вызывается из `unload_data_by_days`. |
| `unload_data_by_days()` | Дробит диапазон на 5‑дневные сегменты и перезапускает `upload_data`. |
| `run_uploader_chunk(chunk)` | Целевой worker для `multiprocessing.Pool`: получает JSON-подобный словарь и вызывает нужную функцию экземпляра. |
| `unload_data_by_day_for_all_clients(start_date, end_date, profile)` | Вызывается CLI `--update-interval`; внутри: получает токены, очищает БД, форкает пул для каждого логина. |
| `unload_data_by_day_for_single_client(login, start_date, end_date, profile)` | CLI `--single-client`; ищет токен в таблицах доступа и прогоняет `YaStatUploader`. |
| `unload_data_with_changes_tracking_for_all_clients(profile)` | CLI `--track-changes`; для каждого клиента вызывает `DirectChangeTracker`, собирает диапазоны дат и запускает удаление+перезагрузку только изменённых интервалов. |
| `unload_data_with_changes_tracking_for_single_client(login, profile)` | CLI `--track-changes-client`; локальная версия функции выше. |
| `main()` | Точка входа CLI, маршрутизирует аргументы к нужным функциям. |

Связи:  
- `unload_data_by_day_for_all_clients` и дружественные функции используют `AsyncDirectDatabase` для двух целей: хранить токены (`access_db`) и записывать статистику (`target_db`).  
- `run_uploader_chunk` общается с `YaStatUploader` в отдельных процессах, чтобы ускорить загрузку.  
- Все сценарии обновлений (включая track-changes) перед записью вызывают `delete_records_between_dates`, чтобы не было дублей.

### `change_tracker.py`

| Метод | Связи |
| --- | --- |
| `DirectChangeTracker.__init__(token, db, …)` | Получает готовый `AsyncDirectDatabase`. Используется в функциях track-changes. |
| `get_goal_ids_by_client` | Ходит в API кампаний, чтобы включить все приоритетные цели клиента. |
| `_fetch_report_slice` | Низкоуровневая функция, которую вызывает `get_report_data_for_period`; отвечает за один HTTP-запрос к `/reports`. |
| `get_report_data_for_period` | Делит список целей на блоки по 10 и объединяет DataFrame; возвращает полную статистику API. |
| `detect_changes(client_login)` | Сравнивает агрегаты API и БД (через `db.get_daily_summary`), возвращает `days_to_update`. |

Связи: `unload_data_with_changes_tracking_*` создают трекер, передают ему общий семафор API и тот же `AsyncDirectDatabase`, что будет очищать/записывать данные. Это обеспечивает единый источник правды.

## Как подготовить окружение

1. **Python и зависимости.** Необходим Python 3.11+ с пакетами `pandas`, `aiohttp`, `asyncpg`, `sqlalchemy>=2`, `tqdm`. Проще всего установить их в virtualenv или conda:
   ```bash
   pip install pandas aiohttp asyncpg sqlalchemy tqdm python-dotenv
   ```
2. **PostgreSQL.** Создайте две базы (или измените DSN в переменных окружения).
3. **.env с секретами.** Скопируйте `.env.example` в `.env` и укажите `*_DB_HOST/PORT/USER/PASSWORD/NAME` (или полный `*_DB_DSN`, если нужен фулл-URL). Эти переменные можно задавать и через `export`.
4. **Обновите таблицы доступа.** Таблицы `accesses_make_agency` и `accesses_not_agency` можно заполнить вручную или доверить это `unload_data_by_day_for_all_clients`; агентские токены теперь берутся только из этих таблиц.
5. **Логи.** Все модули ведут собственные лог-файлы в корне проекта (`loader.log`, `change_tracker.log`, `async_database.log`). Убедитесь, что каталог доступен на запись.

## Типовые сценарии запуска

1. **Полная перезагрузка диапазона по всем клиентам:**
   ```bash
   python loader_service.py --update-interval 2024-05-01 2024-05-31
   ```
   Скрипт обновит таблицы `accesses_*`, удалит старые записи за период и перезагрузит статистику.

2. **Обновление только изменившихся дней (analytics + light последовательно):**
   ```bash
   python loader_service.py --track-changes --db-profile both
   ```

3. **Принудительно обновить клиента за последние 7 дней:**
   ```bash
   python loader_service.py --single-client client_login_with_underscores 2024-04-01 2024-04-07
   ```

## Как работает детектор изменений (подробно)

```
┌──────────────┐      ┌──────────────┐      ┌─────────────────────┐
│  API Direct  │ ---> │ change_tracker│ ---> │  список дат с diff │
└──────────────┘      └──────┬───────┘      └──────┬──────────────┘
                              │                     │
                              ▼                     ▼
                       ┌─────────────┐       ┌─────────────────────────┐
                       │ AsyncDirectDatabase ▶──── │ purge + reload диапазона│
                       └─────────────┘       └─────────────────────────┘
```

1. Для клиента определяется окно дат (≈ последние 60 дней).
2. На каждую дату агрегируются показатели API (Impressions, Clicks, Cost, SumConversion).
3. Из БД берётся аналогичный агрегат.
4. Если хотя бы один показатель отличается (с учётом настроек `cost_tolerance` и `compare_conversions`), дата попадает в `days_to_update`.
5. Даты сгруппированы в непрерывные диапазоны; эти диапазоны очищаются и перезагружаются.

Такой подход минимизирует нагрузку на API и базу.

## Контроль и отладка

- **Логи AsyncDirectDatabase.** ищите ошибки создания таблиц, COPY или ALTER.
- **Логи loader_service.** отражают статус выгрузок и ответы API (включая лимиты очереди 9000).
- **Логи change_tracker.** полезны при понимании, почему день попал в `days_to_update`.
При проблемах с API:

1. Убедитесь, что `Client-Login` указан без подчёркиваний.
2. Если получаете HTTP 201/202 слишком долго — увеличьте окно ожидания или уменьшите конкуренцию (`REPORT_QUEUE_SEM`). Ограничители теперь учитывают лимиты API (20 запросов / 10 секунд, не более 5 оффлайн отчётов).
3. Ошибка 9000 («очередь переполнена») уже обрабатывается: скрипт спит 60 секунд и повторяет попытку, при этом слот в оффлайн-очереди удерживается.

## Часто задаваемые вопросы

- **Почему данные пишутся в таблицы вида `client_login`?**  
  Имя таблицы = логин с `_`. Это упрощает раздачу прав и поиск по клиентам.

- **Что делать, если в DataFrame появляется новый столбец?**  
  `AsyncDirectDatabase.add_new_columns` автоматически добавит колонку, тип выбирается по `dtype`. Перезапуск скрипта не нужен.

- **Как добавить клиента без агентского доступа?**  
  Используйте `AsyncDirectDatabase.add_other_client_list_to_table`, передав список логинов и токен. Метод просто вставит пары `login/token` в `accesses_not_agency`.

- **Можно ли менять окно сравнения у трекера?**  
  Сейчас оно «минус 60 дней» от текущей даты. Для изменения поправьте расчёт `start` в `DirectChangeTracker.detect_changes`.

- **Где хранить дополнительные секреты?**  
  Используйте переменные окружения или секреты Prefect/Docker. При локальных прогонах можно загрузить `.env` через `python-dotenv`.

## Следующие шаги

1. Настройте переменные окружения для подключения к БД и добавьте токены в таблицы `accesses_*` (агентские — с subtype `agency_token`).
2. Прогоните `python loader_service.py --track-changes`.
3. Отслеживайте логи и при необходимости адаптируйте расписание Prefect или набор полей профиля.
