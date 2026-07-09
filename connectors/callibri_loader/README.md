# callibri_loader

Коннектор загружает статистику звонков и лидов из Callibri API, нормализует данные и записывает в ClickHouse. Запускается через Prefect по расписанию или вручную (из Prefect UI/Telegram-бота).

## Стек и зависимости

- Python 3.10+
- Библиотеки: `asyncio`, `aiohttp`, `pandas`, `numpy`, `prefect`
- ClickHouse (через `clickhouse-connect`)

Зависимости описаны в `orchestration/requirements.txt`.

## Структура

```
connectors/callibri_loader/
├── loader_service.py            # Основной модуль: загрузка, нормализация, запись в ClickHouse
├── prefect/
│   ├── flows.py                 # Prefect flow для запуска коннектора
│   ├── clickhouse_utils.py      # AsyncCallibriDatabase (управление Accesses и БД)
│   └── prefect.yaml             # Deployment и расписание
└── bot/
    ├── handlers.py              # Пользовательские команды Callibri в боте
    ├── keyboards.py             # Клавиатуры раздела Callibri
    ├── plugin.py                # Регистрация router/keyboard/admin-кнопки
    └── admin/handlers.py        # Админ-массовая выгрузка Callibri
```

## Как устроен поток данных

```
Prefect Schedule / Bot
        │
        ▼
callibri_loader_flow  (connectors/callibri_loader/prefect/flows.py)
        │
        ├── site_id задан → process_single_client(site_id, tdelta)
        └── site_id не задан → process_all_clients(tdelta)
                │  asyncio.Semaphore(5)
                └── process_data(user_email, api_token, site_id, tdelta)
                        │
                        ├── get_static()  →  https://api.callibri.ru/site_get_statistics
                        ├── prepare_callibri_df()
                        └── ClickhouseDatabase.write_dataframe(callibri_{site_id})
```

1. `process_all_clients` читает список клиентов из `loader.Accesses` (`service='callibri'`).
2. Для каждого клиента запускается `process_data` через `asyncio.Semaphore(5)` (не более 5 одновременно).
3. `get_static` режет диапазон на интервалы по 7 дней и запрашивает API Callibri по каждому интервалу.
4. Из ответа берутся `channels_statistics[*].calls[*]`, затем данные приводятся к единой схеме.
5. Перед записью очищается диапазон дат (`delete_between_dates`) и выполняется перезаливка в `callibri_{site_id}`.

## Конфигурация клиентов

Клиенты хранятся в таблице `loader.Accesses` (ClickHouse):

| Поле        | Значение                                                  |
|-------------|-----------------------------------------------------------|
| `login`     | `site_id` (строка)                                        |
| `token`     | API-токен Callibri                                        |
| `container` | `user_email`/аккаунт (используется как `user_email` в API) |
| `type`      | `NULL` (не используется для callibri)                     |
| `service`   | `callibri`                                                |

Добавление/удаление клиентов — через Telegram-бот (раздел Callibri) или через `AsyncCallibriDatabase` из `connectors/callibri_loader/prefect/clickhouse_utils.py`.

## Переменные окружения

Имя базы данных (`loader_callibri`) настраивается в `config/loaders.yaml`.

ClickHouse-доступы задаются через `.env` (см. `.env.example`).

## Функции модуля

### `process_all_clients(tdelta=10)`

Загружает данные для всех клиентов из `Accesses`. Обрабатывает клиентов параллельно с ограничением 5 задач.

### `process_single_client(site_id, tdelta=10, api_token=None)`

Загружает данные для одного клиента. Если токен не передан, берёт его из `Accesses`.

### `process_data(user_email, api_token, site_id, tdelta=10, db=None)`

Ядро пайплайна:
1. Вычисляет диапазон: `date_from = вчера - tdelta`, `date_to = вчера`.
2. Загружает данные через `get_static`.
3. Нормализует данные через `prepare_callibri_df`.
4. Очищает диапазон и записывает результат в таблицу `callibri_{site_id}`.

### `get_static(session, user_email, api_token, site_id, date_from, date_to)`

Запрашивает `site_get_statistics` пакетами по 7 дней.

Особенности:
- до 5 попыток при `429` (линейный backoff: 5, 10, 15, 20, 25 сек);
- даты в формате `DD.MM.YYYY`;
- из ответа извлекается массив звонков/лидов из `channels_statistics`.

### `prepare_callibri_df(df)`

Приводит DataFrame к фиксированному набору колонок и типам (`Int64`, `string`, `boolean`, `datetime64[ns, UTC]`).

## Схема итоговой таблицы `callibri_{site_id}`

| Колонка               | Тип      | Описание                                      |
|-----------------------|----------|-----------------------------------------------|
| `id`                  | Int64    | Уникальный идентификатор обращения            |
| `date`                | DateTime | Дата обращения (для `calls` — дата звонка)    |
| `source`              | String   | Источник перехода                              |
| `is_lid`              | Bool     | Уникальность обращения (первое или повторное) |
| `region`              | String   | Регион клиента                                 |
| `name_type`           | String   | Название типа обращения                        |
| `traffic_type`        | String   | Тип трафика                                   |
| `landing_page`        | String   | Посадочная страница                            |
| `utm_source`          | String   | UTM Source                                    |
| `utm_medium`          | String   | UTM Medium                                    |
| `utm_campaign`        | String   | UTM Campaign                                  |
| `utm_content`         | String   | UTM Content                                   |
| `utm_term`            | String   | UTM Term                                      |
| `conversations_number`| Int64    | Номер обращения                                |
| `device`              | String   | Устройство клиента                             |
| `status`              | String   | Класс обращения                                |
| `accurately`          | Bool     | Точность трекинга (`нет`, если номер видели несколько пользователей одновременно) |
| `ym_uid`              | String   | Идентификатор посетителя для Яндекс.Метрики    |
| `site_referrer`       | String   | URI, откуда пришёл клиент                      |
| `clbvid`              | String   | Уникальный идентификатор клиента в Callibri    |
| `metrika_client_id`   | String   | ClientID в Яндекс.Метрике                      |

## Деплоймент Prefect

Определён в `connectors/callibri_loader/prefect/prefect.yaml`:

```yaml
- name: callibri-loader-clickhouse
  entrypoint: connectors/callibri_loader/prefect/flows.py:callibri_loader_flow
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
2. Deployments → `callibri-loader-clickhouse`
3. Run → Quick run (все клиенты) или укажите `site_id`

**Через Telegram-бот:**
1. Раздел Callibri → `💾 Выгрузка`
2. Укажите `site_id` и глубину (`tdelta`)

**Через админ-панель Telegram-бота:**
1. Кнопка `💾 Выгрузка callibri`
2. Укажите глубину (`tdelta`) для выгрузки всех клиентов
