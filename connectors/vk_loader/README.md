# vk_loader

Коннектор загружает статистику рекламных кампаний из VK Ads в ClickHouse. Поддерживает агентские аккаунты с автодискавери активных клиентов. Запускается через Prefect по расписанию или вручную.

## Стек и зависимости

- Python 3.10+
- Библиотеки: `asyncio`, `aiohttp`, `pandas`, `prefect`
- ClickHouse (через `clickhouse-connect`)

Зависимости описаны в `orchestration/requirements.txt`.

## Структура файлов

```
connectors/vk_loader/
├── access.py           # Сбор агентских credentials из Accesses, фильтрация активных клиентов
├── api.py              # VkApiClient — работа с VK Ads API
├── config.py           # Константы, списки колонок, семафор запросов
└── loader_service.py   # Основная логика загрузки данных
```

## Поток данных

```
Prefect / Telegram Bot
        │
        ▼
vk_loader_flow  (orchestration/flows/vk.py)
        │
        └── run_vk_all()
                └── upload_data_for_all_agencies()
                        ├── get_vk_agencies()              ← читает credentials из Accesses
                        └── для каждого агентства:
                                ├── get_agency_clients_with_activity()   ← список активных клиентов
                                └── upload_vk_data_for_agency_client()   × N клиентов
```

## Конфигурация клиентов

Клиенты хранятся в `loader.Accesses` (ClickHouse):

| Поле        | Значение                                  |
|-------------|-------------------------------------------|
| `service`   | `vk`                                      |
| `login`     | `client_id` приложения VK Ads             |
| `token`     | `client_secret` приложения VK Ads         |
| `container` | Метка агентства (опционально)             |

Каждая запись соответствует одному агентству. Клиенты агентства обнаруживаются автоматически через API.

Управление — через Telegram-бот (раздел VK Ads) или напрямую через `AsyncVkDatabase`.

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

Имя базы данных (`loader_vk`) настраивается в `config/loaders.yaml`.

## Схема таблицы `vk_{user_id}`

Таблица создаётся автоматически. Основные поля:

| Колонка          | Тип      | Описание                         |
|------------------|----------|----------------------------------|
| `date`           | DateTime | Дата статистики                  |
| `campaign_name`  | String   | Название кампании                |
| `campaign_id`    | String   | ID кампании                      |
| `group_name`     | String   | Название группы объявлений       |
| `group_id`       | String   | ID группы объявлений             |
| `base_shows`     | Int64    | Показы                           |
| `base_clicks`    | Int64    | Клики                            |
| `base_spent`     | Float64  | Расходы                          |
| `base_vk_goals`  | Int64    | Цели VK                          |
| `events_*`       | Int64    | События (клики, лайки, шеры…)    |
| `social_network_*` | Int64  | Соцсетевые метрики               |
| `uniques_*`      | Int64/Float64 | Уникальные метрики и видео  |
| `video_*`        | Int64/Float64 | Видеометрики                |

Полный список колонок — в `VK_REQUIRED_COLUMNS` (`config.py`).

## Основные компоненты

### `VkApiClient` (`api.py`)

| Метод | Описание |
|-------|----------|
| `get_access_token(agency_client_id?)` | Получает OAuth-токен (агентский или клиентский) |
| `delete_access_token(user_id)` | Удаляет токен после использования |
| `get_agency_clients(token)` | Возвращает список клиентов агентства |
| `get_ad_plans_with_groups(token)` | Возвращает кампании с группами объявлений |
| `get_ad_group_daily_statistics(token, plans, start, end)` | Статистика по группам за диапазон дат |

Особенности:
- Retry с экспоненциальным backoff (до 3 попыток)
- 401/403 не ретраятся (токен невалиден)
- Параллельные запросы ограничены семафором (по умолчанию 5)

### `access.py`

| Функция | Описание |
|---------|----------|
| `get_vk_agencies(db)` | Читает все агентские credentials из Accesses |
| `get_agency_clients_with_activity(client_id, client_secret)` | Возвращает клиентов с ненулевыми показами за последние `lookback_days` дней |

### `loader_service.py`

| Функция | Описание |
|---------|----------|
| `upload_vk_data_for_agency_client(client_id, client_secret, user_id, start, end)` | Загружает данные одного клиента агентства |
| `upload_data_for_all_agencies(start?, end?, lookback_days?)` | Загружает данные всех агентств и их активных клиентов |
| `normalize_vk_dataframe(df)` | Приводит DataFrame к схеме (`VK_REQUIRED_COLUMNS`) |

## Деплоймент Prefect

Определён в `orchestration/prefect.yaml`:

```yaml
- name: vk-loader-daily
  entrypoint: orchestration/flows/vk.py:vk_loader_flow
  schedule:
    cron: "30 8 * * *"
    timezone: Asia/Novosibirsk
  parameters:
    lookback_days: 10
```

Запускается ежедневно в 08:30 по Новосибирску. По умолчанию загружает данные за последние 10 дней для всех агентств.

### Параметры flow

| Параметр       | Тип           | По умолчанию | Описание                                         |
|----------------|---------------|--------------|--------------------------------------------------|
| `start_date`   | `str \| None` | `None`       | Начало диапазона (YYYY-MM-DD)                    |
| `end_date`     | `str \| None` | `None`       | Конец диапазона (YYYY-MM-DD, по умолчанию вчера) |
| `lookback_days`| `int`         | `10`         | Глубина загрузки, если `start_date` не задан     |

### Retry-логика задач

| Задача      | Повторы | Задержка | Таймаут |
|-------------|---------|----------|---------|
| `run_vk_all` | 2      | 25 сек   | 4 ч     |

## Запуск вручную

**Через Prefect UI:**
1. Откройте http://localhost:4200
2. Deployments → `vk-loader-daily`
3. Run → Quick run

**Через Telegram-бот:**
1. Раздел VK Ads → "Выгрузка"
