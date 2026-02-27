# loader_calltouch

Проект выгружает звонки и заявки из Calltouch, нормализует их и складывает в PostgreSQL. Скрипт ориентирован на регулярную загрузку (через APScheduler), но его легко запустить и вручную. Ниже собрана вся информация, чтобы и новичок, и опытный разработчик могли быстро понять устройство пайплайна и настроить его под себя.

## Стек и зависимости

- Python 3.10+
- Библиотеки: `asyncio`, `pandas`, `numpy`, `sqlalchemy[asyncio]`, `asyncpg`, `requests`, `apscheduler`, `ast`, `logging`
- PostgreSQL (по умолчанию база `call_ct` на `localhost`)

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install pandas numpy sqlalchemy asyncpg requests apscheduler
```

> Перед запуском создайте файл `.env` (можно скопировать из `.env.example`) и заполните значения `DATABASE_URL`/`CALLTOUCH_API_TOKEN`. Секреты больше не хранятся в коде.

### Переменные окружения

- `DATABASE_URL` — строка подключения в формате SQLAlchemy (`postgresql+asyncpg://user:password@host:port/db`).  
  Вместо неё можно задать `DB_USER`, `DB_PASSWORD`, `DB_NAME` и при необходимости `DB_HOST`/`DB_PORT` — строка соберётся автоматически.
- `CALLTOUCH_API_TOKEN` — опционально, используется только в `test.ipynb` или при ручных вызовах `add_client_to_calltouch_config`.

Пример:

```bash
cp .env.example .env
echo \"DATABASE_URL=postgresql+asyncpg://postgres:password@localhost/call_ct\" >> .env
echo \"CALLTOUCH_API_TOKEN=your_calltouch_token\" >> .env
```

## Структура проекта

```
loader_calltouch/
├── base_service.py      # Работа с базой PostgreSQL
├── loader_pipe.py       # Получение данных Calltouch, обработка и планировщик
└── test.ipynb           # Черновик для экспериментов (не используется в пайплайне)
```

## Как устроен поток данных

```mermaid
flowchart LR
    subgraph Scheduler
        cron[APScheduler<br/>cron 08:05]
    end
    cron --> main
    main --> pipe[AsyncCalltouchPipeline]
    pipe --> worker{{process_data site_id, token}}
    worker --> callsAPI[Calltouch API<br/>/calls-diary/calls]
    worker --> leadsAPI[Calltouch API<br/>/requests]
    worker --> transform[Очистка, нормализация,<br/>вычисление признаков]
    transform --> db[PostgreSQL<br/>schema/table ch_site_id]
```

1. `main()` получает конфигурацию клиентов из таблицы `public.calltouch_config`.
2. `AsyncCalltouchPipeline` создаёт конкурентные задачи для каждого клиента (по умолчанию до 5 одновременно, с паузой 0.4 с).
3. Для каждого клиента `process_data` запрашивает звонки и лиды, очищает их и дополняет признаками.
4. Перед записью данные за нужный диапазон удаляются (`erase_data_in_interval`), затем вставляются пачками (`write_dataframe_to_table`).

## Подробная логика пайплайна

### 1. Планирование и конфигурация
- При старте `main()` создаёт экземпляр `AsyncCalltouchDatabase`, вызывает `get_calltouch_config_data()` и получает все пары `site_id`/`token`.
- Благодаря `create_calltouch_config_table()` таблица конфигурации будет создана автоматически, если её ещё нет.
- Логирование (`calltouch_loader.log`) фиксирует как факт запуска, так и размер полученной конфигурации.

### 2. Подготовка задач
- `AsyncCalltouchPipeline` принимает DataFrame из предыдущего шага и организует очередность выполнения.
- Ограничение параллелизма задаёт `asyncio.Semaphore(max_concurrent)`. При превышении лимита задачи ожидают освобождение семафора.
- Между созданием задач выдерживается `delay_between_requests`, что снижает риск hitting rate limit в Calltouch.

### 3. Выгрузка Calltouch → DataFrame
1. `process_data` рассчитывает рабочий диапазон: `date_to = вчера 23:59:59`, `date_from = date_to - tdelta`.
2. Через `asyncio.to_thread` параллельно вызываются `download_call_data` и `download_lead_data`, чтобы не блокировать event loop.
3. Каждая функция использует пагинацию Calltouch (параметры `page`, `limit`, `pageTotal`). Если API возвращает ошибку, `requests.raise_for_status()` поднимет исключение и зафиксирует его в логах.
4. Полученные JSON приводятся к плоскому виду `pandas.json_normalize`. Далее выполняется жёсткая фильтрация колонок:
   - в блоке звонков удаляются технические поля (`redirectNumber`, `poolType`, `utmSource` и т.д.), переименовываются ключевые атрибуты (`callTags → tags`, `duration → CallDuration`);
   - в блоке лидов сохраняются только поля сессии и UTM-метки, время нормализуется к `datetime64`.
5. Пустые результаты не ломают конвейер: вместо них создаются DataFrame с ожидаемыми столбцами, заполненными `NaN`.

### 4. Объединение и нормализация
- `combined_df = pd.concat([df_lead, df_calls], ignore_index=True)` уравнивает структуру двух источников.
- Обязательные поля добиваются через цикл `required_columns` — если колонка отсутствует, она создаётся и заполняется `NaN`.
- Комментарии (`comments`) и теги (`tags`) хранятся в виде строковых представлений списков. Две вспомогательные функции:
  - `__extract_comment__` извлекает список комментариев и берёт первый осмысленный текст;
  - `__extract_tags_info__` собирает категории/типы/названия тегов в отдельные поля `tag_category`, `tag_type`, `tag_names`.
- Для bool‑колонок используется `pd.BooleanDtype()`, чтобы отличать `False` от `NA`. Числовые идентификаторы приводятся к `Int64`, строки — к `StringDtype`.
- Из `url` извлекается домен регулярным выражением, обрезаются хвостовые слэши.

### 5. Работа с базой
- Имя таблицы формируется как `ch_{site_id}`. При первом запуске `create_table_from_dataframe` создаёт таблицу и схему (если указано `schema.table`).
- Перед записью `erase_data_in_interval` удаляет уже существующие данные в диапазоне `date_from/date_to`, чтобы избежать дублей.
- `write_dataframe_to_table`:
  1. отражает актуальную схему (`reflect_metadata`);
  2. автоматически добавляет недостающие столбцы (`add_columns_to_table`) и даже переименовывает колонки формата `i_goal_*` при изменениях именования в API;
  3. разбивает записи на чанки: максимальное число параметров PostgreSQL (`32767`) делится на количество колонок, что защищает от `Bind parameters exceeded`.
  4. выполняет до 3 повторных попыток при SQL-ошибках; между ретраями пересоздаёт движок и повторно отражает метаданные.

### 6. Контроль типов и значений
- Все `NaN`, `NaT`, пустые строки и маркеры вроде `"<не указано>"` заменяются на `pd.NA`, а перед вставкой переводятся в `None`, чтобы Postgres корректно интерпретировал значения.
- Для отсутствующих колонок в таблице, но присутствующих в БД, добавляются значения по умолчанию (0, 0.0, `False` в зависимости от типа).
- После завершения вставки в лог пишется количество успешных чанков, что помогает отслеживать прогресс при больших пачках данных.

## Таблица конфигурации

`public.calltouch_config` хранит клиентов, которых нужно выгружать.

| Поле      | Тип     | Назначение                                           |
|-----------|---------|------------------------------------------------------|
| `id`      | serial  | Первичный ключ                                       |
| `date`    | date    | Дата добавления записи (по умолчанию `current_date`) |
| `account` | text    | Условное имя/менеджер                                |
| `site_id` | int     | Идентификатор сайта в Calltouch                      |
| `token`   | text    | API токен клиента                                    |

Добавление/удаление клиентов можно выполнить из Python REPL:

```python
import asyncio
from loader_calltouch.base_service import AsyncCalltouchDatabase

async def add():
    db = AsyncCalltouchDatabase()
    await db.add_client_to_calltouch_config('demo', 12345, 'token')

asyncio.run(add())
```

## Класс `AsyncCalltouchDatabase`

| Метод | Что делает | Важные детали |
|-------|------------|---------------|
| `__init__` | создаёт асинхронный движок и сессию SQLAlchemy | пул 10 соединений, `asyncpg`, очередь записи и семафор на 3 параллельные записи |
| `init_db`, `reflect_metadata` | подтягивают актуальную схему БД | вызываются перед проверками таблиц/колонок |
| `create_calltouch_config_table` | гарантирует наличие таблицы конфигурации | используется лениво, при первом обращении |
| `add_client_to_calltouch_config`, `delete_client_by_site_id`, `get_calltouch_config_data` | CRUD-операции над конфигом | проверка уникальности `site_id` |
| `create_table_from_dataframe` | создаёт таблицу `schema.table` по DataFrame | название таблицы разбирается на `schema` + `table` (`parse_table_name`) |
| `add_columns_to_table`, `rename_column_in_table`, `delete_column_from_table` | синхронизация структуры таблиц | названия колонок чистятся от пробелов, `«»`, `:` и т.д. |
| `erase_data_in_interval` | удаляет данные по колонке `date` в указанном диапазоне | используется для перезаливки последних N дней |
| `write_dataframe_to_table` | основная вставка данных | добавляет недостающие столбцы, делит запись на чанки (`max_params // num_columns`) и ретраит до 3 раз |

Логи пишутся в `calltouch_loader.log` рядом со скриптами.

## Модуль `loader_pipe.py`

| Элемент | Описание | Ключевые параметры |
|---------|----------|--------------------|
| `download_call_data` | пагинированный REST-запрос `/calls-diary/calls` | `withCallTags`, `withComments`, фильтр по датам и страницам |
| `download_lead_data` | запрос `/requests` | подтягивает `RequestTags`, приводит даты |
| `process_data(site_id, token, tdelta=10)` | ядро пайплайна: скачивание, очистка, объединение и запись в БД | `tdelta` — сколько дней истории выгружать (по умолчанию 10) |
| `process_single_client(site_id, tdelta=10)` | вспомогательный метод для ручного запуска по одному клиенту | берет токен из `calltouch_config` |
| `AsyncCalltouchPipeline` | управляет параллелизмом выгрузки | `max_concurrent=5`, `delay_between_requests=0.4` |
| `main()` | получает конфиг из БД и запускает пайплайн | исполняется планировщиком |

### Что именно делает `process_data`

1. Определяет диапазон дат: вчерашний день — `date_to`, далее `tdelta` дней назад — `date_from`.
2. Параллельно (через `asyncio.to_thread`) запрашивает звонки и лиды.
3. Чистит колонки: удаляет лишнее, переименовывает в единый стиль (`tags`, `CallDuration`, и т.д.).
4. Вычисляет текст комментариев и атрибуты тегов (`tag_category`, `tag_type`, `tag_names`).
5. Приводит типы (булевы, `Int64`, `String`), вытягивает домен из `url`.
6. Очищает базу в нужном диапазоне и записывает данные в таблицу `ct_{site_id}`.

### Типовая временная диаграмма

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant M as main()
    participant P as AsyncCalltouchPipeline
    participant W as process_data
    participant DB as PostgreSQL
    S->>M: запуск по расписанию
    M->>P: передаёт DataFrame с конфигом
    P->>W: создаёт задачи (до 5 параллельно)
    W->>Calltouch: calls API
    W->>Calltouch: leads API
    W->>DB: erase_data_in_interval
    W->>DB: write_dataframe_to_table
```

## Порядок запуска

1. **Настройте БД.** Убедитесь, что PostgreSQL доступен и пользователь имеет права на создание схем и таблиц.
2. **Создайте/пополните `calltouch_config`.** Используйте методы из `AsyncCalltouchDatabase` или выполните SQL-вставку вручную.
3. **Проверьте зависимости и токены.** Токен Calltouch должен иметь права на API.
4. **Одноразовая проверка:**  
   ```bash
   python - <<'PY'
   import asyncio
   from loader_calltouch.loader_pipe import process_single_client
   asyncio.run(process_single_client(site_id=12345, tdelta=3))
   PY
   ```
   Это выгрузит 3 дня истории конкретного клиента и подтвердит, что таблица `ch_12345` создаётся и заполняется.
5. **Боевой запуск:**  
   ```bash
   python loader_calltouch/loader_pipe.py
   ```
   Скрипт поднимет APScheduler и будет ждать по расписанию (см. ниже).

## Планировщик и как его менять

В конце `loader_pipe.py` настроен `AsyncIOScheduler`:

```python
if __name__ == "__main__":
    scheduler = AsyncIOScheduler()
    scheduler.add_job(main, 'cron', hour=8, minute=5)
    scheduler.start()
    asyncio.get_event_loop().run_forever()
```

- Триггер `cron` запускает `main()` каждый день в 08:05 системного времени.
- Чтобы изменить расписание, замените параметры:

```python
scheduler.add_job(
    main,
    'cron',
    hour='0,12', minute=0, timezone='Europe/Moscow'
)
```

или переключитесь на интервальный запуск:

```python
scheduler.add_job(main, 'interval', hours=1)
```

- Для единичного старта без фона просто вызовите `asyncio.run(main())` (например, через небольшой обёрточный скрипт).

### Как включить/отключить scheduler

1. **Встроенный режим (по умолчанию).** Запуск `python loader_calltouch/loader_pipe.py` — планировщик стартует автоматически.
2. **Ручной режим.** Импортируйте `main` или `process_single_client` и вызывайте их через `asyncio.run` откуда угодно (например, из Airflow/Prefect):

```python
import asyncio
from loader_calltouch.loader_pipe import main
asyncio.run(main())
```

3. **Сторонний планировщик.** Если используете systemd/cron/Airflow, можно исключить блок со встроенным APScheduler (например, запуская модуль как пакет: `python -m loader_calltouch.loader_pipe_main`, где вы создадите свою точку входа).

## Настройка выгрузки

- **`tdelta`** — контролирует глубину перезаливки. Большое значение увеличит время выгрузки, но гарантирует актуальность.
- **`max_concurrent`** — ограничивает одновременные запросы к API (по умолчанию 5). Увеличивать осторожно, чтобы не упереться в лимиты Calltouch.
- **`delay_between_requests`** — пауза между созданием задач; помогает не удариться в rate limit.
- **Фильтры API.** В `download_call_data` можно управлять `attribution`, `withMapVisits`, `with_call_tags` и т.д.
- **Очистка столбцов.** Списки `*_drop_columns` и `*_rename_columns` легко расширить, если ожидаются новые поля.

## CLI-режим и аргументы

В исходном коде нет готового `argparse`, но его легко добавить внешней обёрткой, не изменяя существующие файлы. Создайте рядом с проектом файл `cli_runner.py` (или используйте сниппет в CI) и прокидывайте параметры через аргументы командной строки.

### Рекомендуемые аргументы

| Аргумент | Назначение | По умолчанию | Обязателен |
|----------|------------|--------------|------------|
| `--mode {pipeline,single,scheduler}` | Выбрать сценарий: запустить весь пайплайн (`main`), выгрузить одного клиента или поднять планировщик | `pipeline` | нет |
| `--site-id SITE_ID` | Идентификатор клиента для режима `single` | — | да, если `mode=single` |
| `--tdelta DAYS` | Глубина перезаливки в днях | `10` | нет |
| `--max-concurrent N` | Перекрывает значение, используемое в `AsyncCalltouchPipeline` | `5` | нет |
| `--delay SECONDS` | Пауза между созданием задач | `0.4` | нет |
| `--hour H`, `--minute M` | Часы и минуты для cron-триггера в режиме `scheduler` | `8`, `5` | нет |

### Пример обёртки

```python
#!/usr/bin/env python
import argparse
import asyncio
from loader_calltouch.loader_pipe import main, process_single_client, AsyncCalltouchPipeline, AsyncCalltouchDatabase
from apscheduler.schedulers.asyncio import AsyncIOScheduler

def cli():
    parser = argparse.ArgumentParser(description="CLI для loader_calltouch")
    parser.add_argument("--mode", choices=["pipeline", "single", "scheduler"], default="pipeline")
    parser.add_argument("--site-id", type=int, help="Номер сайта Calltouch для одиночной выгрузки")
    parser.add_argument("--tdelta", type=int, default=10, help="Сколько дней истории забирать")
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--delay", type=float, default=0.4)
    parser.add_argument("--hour", type=str, default="8")
    parser.add_argument("--minute", type=str, default="5")
    args = parser.parse_args()

    if args.mode == "pipeline":
        asyncio.run(main())
    elif args.mode == "single":
        if not args.site_id:
            parser.error("--site-id обязателен для режима single")
        asyncio.run(process_single_client(args.site_id, args.tdelta))
    elif args.mode == "scheduler":
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler()
        scheduler.add_job(main, "cron", hour=args.hour, minute=args.minute)
        scheduler.start()
        try:
            loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            pass

if __name__ == "__main__":
    cli()
```

> Обёртка не модифицирует исходный код, но позволяет использовать единый CLI в cron/systemd/CI. При необходимости можно добавить дополнительные аргументы (например, путь к `.env` или альтернативный DSN) и внутри функции `cli()` менять атрибуты `AsyncCalltouchPipeline` перед запуском.

### Примеры вызовов

```bash
# Полная выгрузка по расписанию (используя встроенный APScheduler)
python cli_runner.py --mode scheduler --hour 3 --minute 15

# Ручная перезаливка конкретного клиента за 30 дней
python cli_runner.py --mode single --site-id 52718 --tdelta 30

# Запустить пайплайн без фонового расписания
python cli_runner.py --mode pipeline
```

## Логирование и диагностика

- Все операции пишутся в `calltouch_loader.log`. В нём видно создание таблиц, добавление колонок, ошибки API и повторные попытки записи.
- Не забывайте закрывать соединения (`await db.close_engine()`), если пишете дополнительные утилиты.
- При проблемах с БД включите `echo=True` в `create_async_engine`, чтобы увидеть SQL.

## Частые вопросы

- **Как переиспользовать таблицы?** Каждому сайту соответствует таблица `ch_{site_id}` в схеме `public`. Можно поменять схему, указав `schema.table` в `write_table_name`.
- **Что делать с дублями?** Перед записью диапазон дат очищается, так что дублей за последние `tdelta` дней не остаётся.
- **Как сменить пользователя БД?** Исправьте строку подключения `db_url` и перезапустите скрипт.
- **Где менять имена колонок?** В функциях `calls_rename_columns` и `lead_rename_columns`, а также внутри `process_data` при формировании `combined_df`.

Готово! Теперь у вас есть полная карта проекта, описания методов и инструкции по запуску/настройке планировщика.
