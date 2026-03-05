# direct_loader

Коннектор загружает статистику рекламных кампаний из API Яндекс.Директ в ClickHouse. Поддерживает два профиля данных (`analytics` и `light`), change tracking и параллельную обработку клиентов. Запускается через Prefect 3 по расписанию или вручную.

## Стек и зависимости

- Python 3.10+
- Библиотеки: `asyncio`, `aiohttp`, `pandas`, `prefect`
- ClickHouse (через `clickhouse-connect`)

Зависимости описаны в `orchestration/requirements.txt`.

## Структура файлов

```
connectors/direct_loader/
├── config.py                # Профили данных, списки полей, типы токенов, rate limiters
├── access.py                # Получение и обновление токенов клиентов из Accesses
├── uploader.py              # YaStatUploader — загрузка отчётов из API и запись в ClickHouse
├── change_tracker.py        # DirectChangeTracker — обнаружение изменённых дней
├── tasks.py                 # Async-функции верхнего уровня
├── jobs.py                  # DirectReloadJob, plan_direct_reload_jobs
├── shared_utils.py          # Нормализация логина, даты, emit_prefect_event
├── report_limits.py         # GlobalRateLimiter, PerAdvertiserRateLimiter, OfflineReportTracker
├── logging_utils.py         # get_logger() — Prefect-логгер
├── prefect/
│   ├── flows.py             # Prefect flow и tasks-обёртки
│   ├── clickhouse_utils.py  # AsyncDirectDatabase
│   └── prefect.yaml         # Deployment и расписания
└── bot/
    ├── handlers.py          # Пользовательские команды Директа
    ├── keyboards.py         # Клавиатуры раздела
    ├── plugin.py            # Регистрация router/keyboard/admin-кнопки
    └── admin/handlers.py    # Админ-массовые выгрузки
```

## Поток данных

```
Prefect / Telegram Bot
        │
        ▼
direct_loader_flow  (connectors/direct_loader/prefect/flows.py)
        │
        ├── track_changes=True
        │       ├── get_direct_clients_task()
        │       ├── detect_changes_for_client_task() × N клиентов (параллельно)
        │       └── fetch_direct_range_task() / write_direct_range_task() × N задач
        │
        └── track_changes=False  (явный диапазон дат)
                └── run_direct_range()
                        └── unload_data_by_day_for_all_clients()
                                └── Pool(cpu_count) → YaStatUploader.unload_data_by_days()
```

## Конфигурация клиентов

Клиенты хранятся в `loader.Accesses` (ClickHouse):

| Поле        | Значение для Direct                                                          |
|-------------|------------------------------------------------------------------------------|
| `login`     | Логин клиента (с `_` вместо `-`) или `-` для агентского токена               |
| `token`     | OAuth-токен Яндекс.Директ                                                    |
| `container` | Название агентства / источника                                               |
| `type`      | `agency_token` / `agency_parsed` / `not_agency_token`                        |

Типы токенов:
- `agency_token` — агентский токен; при каждом запуске система автоматически получает список клиентов через `agencyclients` и сохраняет их с типом `agency_parsed`
- `agency_parsed` — клиент, найденный через агентский аккаунт
- `not_agency_token` — токен конкретного клиента без агентского доступа

Управление токенами — через Telegram-бот (раздел Директ) или напрямую через `AsyncDirectDatabase` из `connectors/direct_loader/prefect/clickhouse_utils.py`.

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

Имена баз данных (`loader_direct_analytics`, `loader_direct_light`) настраиваются в `config/loaders.yaml`.

## Профили данных

### analytics

Полный профиль (база `loader_direct_analytics`):

| Поле                      | Тип    |
|---------------------------|--------|
| `Date`                    | date   |
| `Criterion`               | str    |
| `AdFormat`                | str    |
| `AdId`                    | int    |
| `CampaignType`            | str    |
| `CampaignName`            | str    |
| `AdGroupName`             | str    |
| `Clicks`                  | int    |
| `Ctr`                     | float  |
| `Cost`                    | float  |
| `Impressions`             | int    |
| `Conversions`             | int    |
| `AvgImpressionPosition`   | float  |
| `AvgPageviews`            | float  |
| `AvgClickPosition`        | float  |
| `Bounces`                 | int    |
| `Age`                     | str    |
| `Gender`                  | str    |
| `Device`                  | str    |
| `LocationOfPresenceName`  | str    |
| `Sessions`                | int    |
| `Slot`                    | str    |
| `Placement`               | str    |
| `AdNetworkType`           | str    |
| `AvgTrafficVolume`        | float  |
| `Conv_*` (цели)           | int    |
| `SumConversion`           | int    |

- `cost_tolerance`: 500 (допуск при сравнении стоимости в change tracker)

### light

Упрощённый профиль (база `loader_direct_light`):

Поля: `Date`, `CampaignType`, `CampaignName`, `AdGroupName`, `Clicks`, `Conversions`, `Cost`, `Impressions`, `Bounces`, `Sessions`, `Placement`, `AdNetworkType`, `Conv_*`, `SumConversion`

- `cost_tolerance`: 2

## Основные компоненты

### `YaStatUploader` (`uploader.py`)

Один экземпляр = один клиент + один профиль + один диапазон дат.

| Метод | Описание |
|-------|----------|
| `get_client_list()` | Получает список логинов агентских клиентов через `/json/v5/agencyclients` |
| `upload_data()` | Запрашивает отчёт `CUSTOM_REPORT` через `/json/v5/reports`, обрабатывает ответы 200/201/202/400/9000, записывает в ClickHouse |
| `unload_data_by_days()` | Дробит диапазон на 5-дневные отрезки и вызывает `upload_data()` для каждого |

Особенности `upload_data()`:
- Цели (`GoalId`) запрашиваются через `get_goal_ids_by_client`; GoalId 12 исключается
- Цели делятся на батчи по 10; каждый батч — отдельный отчёт
- 6 попыток с экспоненциальным backoff (5–300 сек)
- Если отчёт не готов за 10 минут или обрывается поток — автоматический переход к поdённым срезам (`fetch_daily_slices`)
- Конверсионные колонки (`Conv_*`) суммируются в `SumConversion`

### `DirectChangeTracker` (`change_tracker.py`)

Сравнивает агрегаты API с данными в ClickHouse.

| Метод | Описание |
|-------|----------|
| `detect_changes(client_login)` | Возвращает `{"changes_detected": bool, "days_to_update": ["YYYY-MM-DD", ...]}` |
| `get_report_data_for_period(...)` | Запрашивает агрегаты API за период (Impressions, Clicks, Cost, Conversions) |
| `_fetch_report_slice(...)` | Один HTTP-запрос к `/json/v5/reports`; ждёт готовности до 60 попыток по 10 сек |

Алгоритм `detect_changes`:
1. Окно сравнения: последние `lookback_days` дней до вчера
2. Запрашивает агрегаты из API и ClickHouse
3. По каждому дню сравнивает Impressions, Clicks, Cost (с `cost_tolerance`), Conversions
4. Если обнаружены новые конверсионные колонки — автоматически добавляет пустые столбцы в таблицу или помечает дни для перезагрузки
5. Если таблица не существует — форсирует загрузку вчерашнего дня

Параметры:
- `lookback_days` (по умолчанию 60)
- `compare_conversions` (True — сравнивать конверсии)
- `cost_tolerance` (из профиля: 500 / 2)
- `skip_cost_check_clients`: `{"lovol_russia", "changan_asmoto_ads"}` — клиенты, для которых стоимость не сравнивается

### `access.py`

| Функция | Описание |
|---------|----------|
| `refresh_agency_clients(access_db, profile)` | Для каждого `agency_token` запрашивает список клиентов и обновляет `Accesses` с типом `agency_parsed` |
| `collect_direct_login_tokens(access_db, profile)` | Возвращает `dict[login → token]` с учётом приоритетов типов токенов |

### `jobs.py`

| Элемент | Описание |
|---------|----------|
| `DirectReloadJob` | Датакласс: `login`, `token`, `start_date`, `end_date`, `profile`, `source` |
| `plan_direct_reload_jobs(...)` | Запускает change detection для всех клиентов параллельно, возвращает список `DirectReloadJob` |

### `tasks.py` (функции верхнего уровня)

| Функция | Описание |
|---------|----------|
| `unload_data_by_day_for_all_clients(start_date, end_date, profile)` | Принудительная перезагрузка всех клиентов за диапазон; использует `multiprocessing.Pool` |
| `unload_data_by_day_for_single_client(login, start_date, end_date, profile)` | То же для одного клиента |
| `unload_data_with_changes_tracking_for_all_clients(profile, lookback_days)` | Change tracking для всех; параллельное обнаружение через `asyncio.gather`, загрузка через semaphore |
| `unload_data_with_changes_tracking_for_single_client(login, profile, lookback_days)` | То же для одного клиента |
| `fetch_direct_job_data(job)` | Выполняет загрузку для одного `DirectReloadJob` |

### Rate Limiting (`report_limits.py`)

| Класс | Описание |
|-------|----------|
| `GlobalRateLimiter` | Глобальный лимит: 19 запросов за 10 секунд по всем клиентам |
| `PerAdvertiserRateLimiter` | Лимит на рекламодателя: 20 запросов за 10 секунд |
| `OfflineReportTracker` | Семафор на 5 одновременных оффлайн-отчётов на токен |

Кроме того, `get_report_queue_semaphore()` ограничивает до 5 одновременных HTTP-запросов к `/reports`.

## Деплойменты Prefect

Определены в `connectors/direct_loader/prefect/prefect.yaml`:

| Деплоймент | Расписание (NSK) | Профиль | track_changes |
|------------|-----------------|---------|---------------|
| `direct-analytics-change` | 06:00 ежедневно | analytics | true |
| `direct-light-hourly` | 07:00–13:00 ежечасно | light | true |
| `direct-light-3h` | 14:00, 17:00, 20:00, 23:00 | light | true |

### Параметры flow `direct-loader-clickhouse`

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `track_changes` | bool | false | Использовать change tracking |
| `lookback_days` | int | 60 | Глубина проверки в днях |
| `profile` | str | "analytics" | Профиль данных |
| `login` | str\|None | None | Один клиент (иначе — все) |
| `start_date` | str\|None | None | Начало диапазона (YYYY-MM-DD), если `track_changes=False` |
| `end_date` | str\|None | None | Конец диапазона, если `track_changes=False` |

### Retry-логика задач

- Все задачи: 2 повтора, задержка 20–60 сек
- Таймаут change tracker / date-range: 6 часов
- Таймаут detect-changes на клиента: 10 минут
- Таймаут fetch на одну задачу: 3 часа

## Запуск вручную

**Через Prefect UI:**
1. Откройте http://localhost:4200
2. Deployments → выберите деплоймент
3. Run → Quick run или задайте параметры

**Через Telegram-бот:**
1. Раздел Директ → Обновить данные
2. Выберите профиль (Аналитическая БД / Легкая БД)
3. Все клиенты или введите `login`
