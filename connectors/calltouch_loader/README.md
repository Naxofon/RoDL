# calltouch_loader

Коннектор загружает звонки и заявки из Calltouch API, нормализует их и записывает в ClickHouse. Запускается через Prefect по расписанию или вручную.

## Стек и зависимости

- Python 3.10+
- Библиотеки: `asyncio`, `aiohttp`, `pandas`, `numpy`, `prefect`
- ClickHouse (через `clickhouse-connect`)

Зависимости описаны в `orchestration/requirements.txt`.

## Структура

```
connectors/calltouch_loader/
├── loader_service.py            # Основной модуль: загрузка, нормализация, запись в ClickHouse
├── prefect/
│   ├── flows.py                 # Prefect flow для запуска коннектора
│   ├── clickhouse_utils.py      # AsyncCalltouchDatabase
│   └── prefect.yaml             # Deployment и расписание
└── bot/
    ├── handlers.py              # Пользовательские команды Calltouch в боте
    ├── keyboards.py             # Клавиатуры раздела Calltouch
    ├── plugin.py                # Регистрация router/keyboard/admin-кнопки
    └── admin/handlers.py        # Админ-массовая выгрузка Calltouch
```

## Как устроен поток данных

```
Prefect Schedule / Bot
        │
        ▼
calltouch_loader_flow  (connectors/calltouch_loader/prefect/flows.py)
        │
        ├── site_id задан → process_single_client(site_id, tdelta)
        └── site_id не задан → process_all_clients(tdelta)
                │  asyncio.Semaphore(5)
                └── process_data(site_id, api_token, tdelta)
                        │
                        ├── download_call_data()  →  /calls-diary/calls
                        ├── download_lead_data()  →  /requests
                        └── ClickhouseDatabase.write_dataframe(ct_{site_id})
```

1. `process_all_clients` читает список клиентов из `loader.Accesses` (тип `calltouch`).
2. Для каждого клиента запускается `process_data` через `asyncio.Semaphore(5)` (не более 5 одновременно).
3. Параллельно запрашиваются данные звонков и заявок из Calltouch API за период `[вчера - tdelta ... вчера]`.
4. Данные нормализуются, объединяются и записываются в ClickHouse в таблицу `ct_{site_id}`.
5. Перед записью диапазон дат очищается (`delete_between_dates`), чтобы не было дублей.

## Конфигурация клиентов

Клиенты хранятся в таблице `loader.Accesses` (ClickHouse):

| Поле        | Значение                                      |
|-------------|-----------------------------------------------|
| `login`     | `site_id` (строка)                            |
| `token`     | API-токен Calltouch                           |
| `container` | Название аккаунта (опционально)               |
| `type`      | `NULL` (не используется для calltouch)        |

Добавление/удаление клиентов — через Telegram-бот (раздел Calltouch) или напрямую через `AsyncCalltouchDatabase` из `connectors/calltouch_loader/prefect/clickhouse_utils.py`.

## Переменные окружения

Имя базы данных (`loader_calltouch`) настраивается в `config/loaders.yaml`.

## Функции модуля

### `process_all_clients(tdelta=10)`

Загружает данные для всех клиентов из таблицы `Accesses`. Запускает параллельную обработку с ограничением 5 одновременных клиентов.

### `process_single_client(site_id, tdelta=10, api_token=None)`

Загружает данные для одного клиента. Если `api_token` не передан, берёт токен из `Accesses`.

### `process_data(site_id, api_token, tdelta=10, db=None)`

Ядро пайплайна:
1. Вычисляет диапазон: `date_from = вчера - tdelta`, `date_to = вчера`.
2. Параллельно запрашивает звонки (`download_call_data`) и заявки (`download_lead_data`).
3. Нормализует и объединяет данные.
4. Очищает и записывает результат в таблицу `ct_{site_id}`.

### `download_call_data(session, api_token, site_id, date_from, date_to, ...)`

Пагинированный запрос к `/calls-diary/calls`. Формат дат: `DD/MM/YYYY`. Поддерживает параметры `with_call_tags`, `with_comments`, `with_map_visits`.

### `download_lead_data(session, api_token, site_id, date_from, date_to)`

Запрос к `/requests`. Формат дат: `MM/DD/YYYY` (особенность Calltouch API).

## Схема итоговой таблицы `ct_{site_id}`

| Колонка            | Тип      | Описание                                     |
|--------------------|----------|----------------------------------------------|
| `Date`             | DateTime | Дата и время звонка/заявки                   |
| `tag_category`     | String   | Категории тегов (через запятую)              |
| `tag_type`         | String   | Типы тегов                                   |
| `tag_names`        | String   | Названия тегов                               |
| `additionalTags`   | String   | Дополнительные теги                          |
| `comment_text`     | String   | Первый комментарий из массива                |
| `manager`          | String   | Менеджер                                     |
| `attribution`      | Int64    | Модель атрибуции                             |
| `uniqTargetRequest`| Bool     | Уникальная целевая заявка                    |
| `uniqueRequest`    | Bool     | Уникальная заявка                            |
| `targetRequest`    | Bool     | Целевая заявка                               |
| `uniqueCall`       | Bool     | Уникальный звонок                            |
| `targetCall`       | Bool     | Целевой звонок                               |
| `uniqTargetCall`   | Bool     | Уникальный целевой звонок                    |
| `callbackCall`     | Bool     | Обратный звонок                              |
| `CallSuccessful`   | Bool     | Звонок состоялся                             |
| `CallDuration`     | Int64    | Длительность звонка (секунды)                |
| `subject`          | String   | Тема обращения                               |
| `requestId`        | Int64    | ID заявки                                    |
| `sessionId`        | Int64    | ID сессии                                    |
| `callId`           | Int64    | ID звонка                                    |
| `yaClientId`       | String   | Яндекс ClientID                              |
| `city`             | String   | Город                                        |
| `browser`          | String   | Браузер                                      |
| `device`           | String   | Устройство                                   |
| `os`               | String   | Операционная система                         |
| `keywords`         | String   | Ключевое слово                               |
| `ref`              | String   | Реферер                                      |
| `url`              | String   | URL страницы                                 |
| `source`           | String   | Источник трафика                             |
| `medium`           | String   | Канал трафика                                |
| `utmTerm`          | String   | UTM Term                                     |
| `utmContent`       | String   | UTM Content                                  |
| `utmCampaign`      | String   | UTM Campaign                                 |
| `domain`           | String   | Домен (извлекается из `url`)                 |

Описание полей API: [Calltouch API Docs](https://www.calltouch.ru/support/api/)

## Деплоймент Prefect

Определён в `connectors/calltouch_loader/prefect/prefect.yaml`:

```yaml
- name: calltouch-loader-clickhouse
  entrypoint: connectors/calltouch_loader/prefect/flows.py:calltouch_loader_flow
  schedule:
    cron: "0 8 * * *"
    timezone: Asia/Novosibirsk
  parameters:
    tdelta: 10
```

Запускается ежедневно в 08:00 по Новосибирску. По умолчанию загружает данные за последние 10 дней для всех клиентов.

### Параметры flow

| Параметр  | Тип            | По умолчанию | Описание                                         |
|-----------|----------------|--------------|--------------------------------------------------|
| `site_id` | `int \| None`  | `None`       | Загрузить только этот клиент (иначе — все)       |
| `tdelta`  | `int`          | `10`         | Глубина перезаливки в днях                       |

### Retry-логика

- Задачи: 2 повтора с задержкой 25 секунд
- Таймаут для всех клиентов: 4 часа
- Таймаут для одного клиента: 2 часа

## Запуск вручную

**Через Prefect UI:**
1. Откройте http://localhost:4200
2. Deployments → `calltouch-loader-clickhouse`
3. Run → Quick run (все клиенты) или укажите `site_id`

**Через Telegram-бот:**
1. Раздел Calltouch → "Обновить данные"
2. Выберите "Все клиенты" или введите `site_id`
