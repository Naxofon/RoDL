# metrika_loader

Коннектор загружает статистику посещений из Яндекс.Метрики в ClickHouse. Поддерживает change tracking и параллельную обработку счётчиков. Запускается через Prefect по расписанию или вручную.

## Стек и зависимости

- Python 3.10+
- Библиотеки: `asyncio`, `aiohttp`, `pandas`, `prefect`
- ClickHouse (через `clickhouse-connect`)

Зависимости описаны в `orchestration/requirements.txt`.

## Структура файлов

```
connectors/metrika_loader/
├── access.py                # Сбор токенов счётчиков из Accesses (явные + агентские)
├── change_tracker.py        # MetrikaChangeTracker — обнаружение изменённых дней
├── change_utils.py          # AsyncRequestLimiter, GoalMetadata, classify_goals и утилиты
├── jobs.py                  # MetrikaReloadJob, plan_metrika_reload_jobs
├── operations.py            # Массовые операции: upload, fetch/write для заданий
├── uploader.py              # YaMetrikaUploader — загрузка из Logs API и Reporting API
├── prefect/
│   ├── flows.py             # Prefect flow и tasks-обёртки
│   ├── clickhouse_utils.py  # AsyncMetrikaDatabase
│   └── prefect.yaml         # Deployment и расписание
└── bot/
    ├── handlers.py          # Пользовательские команды Метрики
    ├── keyboards.py         # Клавиатуры раздела
    ├── plugin.py            # Регистрация router/keyboard/admin-кнопки
    └── admin/handlers.py    # Админ-массовые выгрузки
```

## Поток данных

```
Prefect / Telegram Bot
        │
        ▼
metrika_loader_flow  (connectors/metrika_loader/prefect/flows.py)
        │
        ├── track_changes=True
        │       ├── plan_metrika_jobs_task()         ← change detection для всех счётчиков
        │       └── fetch_metrika_range_task() / write_metrika_range_task()  × N заданий
        │               (до 9 параллельных fetch)
        │
        └── track_changes=False  (явный диапазон дат)
                └── run_metrika_range()
                        └── upload_data_for_all_counters()
                                └── process_chunks_in_parallel()
                                        └── YaMetrikaUploader.upload_data()
```

## Конфигурация клиентов

Клиенты хранятся в `loader.Accesses` (ClickHouse):

| Поле        | Значение                                                           |
|-------------|--------------------------------------------------------------------|
| `login`     | `counter_id` (число) — для явного токена                          |
| `container` | `counter_id` (число) — альтернативное поле для явного токена      |
| `token`     | OAuth-токен Яндекс.Метрики                                        |
| `subtype`   | `agency` / `agency_token` — для агентского токена                 |

Типы токенов:
- **Явный токен** — числовой `login` или `container` = counter_id; используется напрямую для загрузки данных
- **Агентский токен** — `subtype = agency` или `agency_token`; система автоматически запрашивает список избранных счётчиков через Management API, затем сопоставляет их с явными токенами
- Явные токены имеют приоритет над агентскими

Управление токенами — через Telegram-бот (раздел Метрика) или напрямую через `AsyncMetrikaDatabase`.

## Переменные окружения

Все переменные задаются в `.env` корня проекта (см. `.env.example`):

```bash
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default_user
CLICKHOUSE_PASSWORD=strong_password

CLICKHOUSE_ACCESS_USER=access_user
CLICKHOUSE_ACCESS_PASSWORD=access_password
```

Имя базы данных (`loader_metrika`) настраивается в `config/loaders.yaml`.

## Схема таблицы `m_{counter_id}`

Таблица создаётся автоматически на основе данных. Основные поля:

| Колонка           | Тип      | Описание                                 |
|-------------------|----------|------------------------------------------|
| `dateTime`        | DateTime | Дата и время визита                      |
| `visitID`         | Int64    | Уникальный идентификатор визита          |
| `visits`          | Int64    | Количество визитов                       |
| `g_goal_*`        | Int64    | Цели с "madd" в названии                |
| `u_goal_*`        | Bool     | Все цели в бинарном формате (уникальные) |
| `g_sum_goal`      | Int64    | Сумма по g_goal_*                        |
| `u_sum_goal`      | Int64    | Сумма по u_goal_*                        |

## Основные компоненты

### `YaMetrikaUploader` (`uploader.py`)

Один экземпляр = один счётчик + один диапазон дат.

| Метод | Описание |
|-------|----------|
| `load_metrika(counter_id, token, start_date, end_date)` | Параллельно запрашивает Logs API и Reporting API, объединяет данные по `visitID` |
| `upload_data()` | Вызывает `load_metrika`, затем пишет результат в ClickHouse через `AsyncMetrikaDatabase` |
| `split_date_range(start, end, days)` | Дробит диапазон на чанки заданной длины |

Особенности:
- Logs API: создаёт задачу логов, ожидает готовности, скачивает части
- Reporting API: опрашивает с шагом 5 дней, собирает агрегаты по `visitID`
- Классификация целей: `g_goal_*` (name содержит "madd"), `u_goal_*` (все остальные, уникальные)
- Дедупликация по `visitID` после записи

### `MetrikaChangeTracker` (`change_tracker.py`)

Сравнивает агрегаты API с данными в ClickHouse.

| Метод | Описание |
|-------|----------|
| `detect_changes(table_name)` | Возвращает `{changes_detected, days_to_update, api_total_visits, db_total_visits, ...}` |

Алгоритм:
1. Окно сравнения: последние `lookback_days` дней до вчера
2. Запрашивает агрегаты из API (`APIVisits`, `APIConversions`) и ClickHouse (`DBVisits`, `DBConversions`)
3. По каждому дню сравнивает визиты и конверсии
4. Обнаруживает новые колонки целей, помечает дни для перезагрузки
5. Если таблица пуста — форсирует загрузку вчерашнего дня

Параметры `MetrikaChangeTracker`:
- `lookback_days` (по умолчанию 60)
- `counter_id`, `token`, `db`, `login`

### `access.py`

| Функция | Описание |
|---------|----------|
| `collect_metrika_access_data(async_db)` | Собирает mapping counter_id→token из Accesses; поддерживает агентские токены (запрашивает избранные счётчики через Management API) |
| `counters_from_access(df_access)` | Строит DataFrame счётчиков из rows Accesses |

### `jobs.py`

| Элемент | Описание |
|---------|----------|
| `MetrikaReloadJob` | Датакласс: `counter_id`, `domain_name`, `token`, `start_date`, `end_date`, `source`, `fact_login` |
| `plan_metrika_reload_jobs(...)` | Последовательно запускает change detection для всех счётчиков, возвращает `(jobs, counters_without_changes, failed_counters, counter_diagnostics)` |

### `operations.py` (функции верхнего уровня)

| Функция | Описание |
|---------|----------|
| `upload_data_for_all_counters(start, end)` | Принудительная перезагрузка всех счётчиков за диапазон, 5-дневными чанками |
| `upload_data_for_single_counter(counter_id, start, end)` | То же для одного счётчика |
| `continue_upload_data_for_counters(counter_id, start, end)` | Продолжить загрузку начиная с указанного counter_id |
| `refresh_data_with_change_tracker(lookback_days, counter_id)` | Change tracking: обнаружение + перезагрузка изменённых дней |
| `fetch_metrika_job_data(job)` | Загружает данные для одного `MetrikaReloadJob` |
| `write_metrika_job_data(job, df)` | Пишет DataFrame в ClickHouse (очищает диапазон перед записью) |

Параметры параллелизма:
- Глобальный семафор: 15 одновременных загрузок
- На токен: 3 одновременные загрузки
- Параллельных fetch в flow: до 9 (`MAX_PARALLEL_FETCH`)

### `change_utils.py`

| Элемент | Описание |
|---------|----------|
| `AsyncRequestLimiter` | Ограничивает одновременные HTTP-запросы и минимальный интервал между ними |
| `GoalMetadata` | Датакласс: `goal_id`, `identifier` (`g` или `u`) |
| `classify_goals(goals)` | Классифицирует цели: `g` если name содержит "madd", иначе `u` |

## Деплоймент Prefect

Определён в `connectors/metrika_loader/prefect/prefect.yaml`:

```yaml
- name: metrika-loader-clickhouse
  entrypoint: connectors/metrika_loader/prefect/flows.py:metrika_loader_flow
  schedule:
    cron: "0 7 * * *"
    timezone: Asia/Novosibirsk
  parameters:
    track_changes: true
    lookback_days: 60
```

Запускается ежедневно в 07:00 по Новосибирску. По умолчанию выполняет change tracking за последние 60 дней для всех счётчиков.

### Параметры flow

| Параметр       | Тип            | По умолчанию | Описание                                           |
|----------------|----------------|--------------|-----------------------------------------------------|
| `track_changes`| `bool`         | `true`       | Использовать change tracking                        |
| `lookback_days`| `int`          | `60`         | Глубина проверки в днях                             |
| `counter_id`   | `int \| None`  | `None`       | Один счётчик (иначе — все)                          |
| `start_date`   | `str \| None`  | `None`       | Начало диапазона (YYYY-MM-DD), если `track_changes=False` |
| `end_date`     | `str \| None`  | `None`       | Конец диапазона, если `track_changes=False`         |

### Retry-логика задач

| Задача | Повторы | Задержка | Таймаут |
|--------|---------|----------|---------|
| `plan_metrika_jobs_task` | 2 | 60 сек | 10 мин |
| `fetch_metrika_range_task` | 2 | 60 сек | 3 ч |
| `write_metrika_range_task` | 2 | 60 сек | 2 ч |
| `run_metrika_range` | 2 | 25 сек | 6 ч |
| `run_metrika_change_tracker` | 2 | 20 сек | 6 ч |

## Запуск вручную

**Через Prefect UI:**
1. Откройте http://localhost:4200
2. Deployments → `metrika-loader-clickhouse`
3. Run → Quick run (все счётчики) или укажите `counter_id`

**Через Telegram-бот:**
1. Раздел Метрика → "Обновить данные"
2. Выберите "Все счётчики" или введите `counter_id`
