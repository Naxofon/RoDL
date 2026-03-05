<p align="center">
  <img src="docs/logo.jpg" alt="RoDL" width="2000"/>
</p>

# Система загрузки рекламных и аналитических данных

Оркестрации и управления потоками данных из рекламных и аналитических систем (Яндекс.Директ, Яндекс.Метрика, Calltouch, VK Ads) в ClickHouse с использованием Prefect 3 и интеграцией Telegram-бота для администрирования.

## Содержание

- [Архитектура системы](#архитектура-системы)
- [Основные компоненты](#основные-компоненты)
- [Быстрый старт](#быстрый-старт)
- [Конфигурация](#конфигурация)
- [Администрирование через Telegram](#администрирование-через-telegram)
- [Деплойменты и расписание](#деплойменты-и-расписание)
- [Модульность системы](#модульность-системы)
- [Change Tracking](#change-tracking)
- [Добавление нового коннектора](#добавление-нового-коннектора)
- [Сообщество](#сообщество)

---

## Архитектура системы

| Слой | Компонент | Описание |
|------|-----------|----------|
| **Источники** | Яндекс.Директ, Яндекс.Метрика, Calltouch, VK Ads | Внешние API |
| **Коннекторы** | `connectors/` | Асинхронные загрузчики с rate limiting и change tracking |
| **Оркестрация** | Prefect Server + Worker | Планирование, retry-логика, мониторинг (UI на порту 4200) |
| **Хранилище** | ClickHouse | Колоночная БД; `loader.Accesses` — централизованное хранилище токенов |
| **Администрирование** | Telegram Bot | Управление клиентами, запуск задач, уведомления об ошибках |

### Поток данных

1. **Планирование**: Prefect Server по расписанию (cron) или по требованию запускает flow
2. **Change Tracking**: сравниваются агрегаты API с данными в ClickHouse, определяются дни для перезагрузки
3. **Параллельная загрузка**: worker'ы выполняют задачи с соблюдением лимитов API
4. **Запись в ClickHouse**: удаление устаревших записей за обновляемый период, затем batch-вставка

---

## Основные компоненты

### Коннекторы (`connectors/`)

Каждый коннектор содержит подробный README с описанием структуры, схемы таблиц, параметров и инструкцией по запуску:

| Коннектор | Описание | README |
|-----------|----------|--------|
| `direct_loader` | Статистика кампаний Яндекс.Директ (Reporting API), два профиля: analytics и light | [README](connectors/direct_loader/README.md) |
| `metrika_loader` | Логи визитов Яндекс.Метрики (Logs API + Reporting API), change tracking по дням | [README](connectors/metrika_loader/README.md) |
| `calltouch_loader` | Звонки и заявки Calltouch (Calls Diary API + Requests API) | [README](connectors/calltouch_loader/README.md) |
| `vk_loader` | Статистика рекламных кампаний VK Ads, агентские аккаунты | [README](connectors/vk_loader/README.md) |
| `wordstat_loader` | Динамика Яндекс.Wordstat по брендам, фразам и регионам | [README](connectors/wordstat_loader/README.md) |
| `custom_loader` | Кастомная агрегация Direct с фильтрацией по целям Метрики | [README](connectors/custom_loader/README.md) |

### Prefect (`orchestration/`)

- **prefect-server**: веб-интерфейс и API на порту 4200
- **prefect-db**: PostgreSQL для метаданных Prefect
- **prefect-bootstrap**: создаёт деплойменты при старте
- **prefect-agent**: выполняет запланированные задачи

### ClickHouse

Базы данных: `loader`, `loader_direct_analytics`, `loader_direct_light`, `loader_metrika`, `loader_calltouch`, `loader_vk`.

Таблица `loader.Accesses` централизованно хранит все токены доступа к API:

```sql
CREATE TABLE loader.Accesses (
    login     Nullable(String),   -- логин клиента или идентификатор
    token     Nullable(String),   -- OAuth-токен доступа
    container Nullable(String),   -- числовое значение, например номер контейнера/аккаунта
    type      Nullable(String)    -- тип сервиса (direct, metrika, metrika:agency, vk, ...)
) ENGINE = MergeTree
ORDER BY (type, container, login);
```

#### Пользователи ClickHouse

| Пользователь | Переменные окружения | Привилегии |
|---|---|---|
| `default_user` | `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | SELECT, INSERT, ALTER, CREATE, TRUNCATE, SHOW TABLES на всех базах данных с данными; ACCESS MANAGEMENT на уровне сервера. Доступ к таблице `Accesses` **закрыт**. |
| `access_user` | `CLICKHOUSE_ACCESS_USER` / `CLICKHOUSE_ACCESS_PASSWORD` | SELECT, INSERT, ALTER, CREATE, TRUNCATE, SHOW TABLES только на `loader.Accesses` и `loader.AdminUsers`. Доступ к базам данных с данными **закрыт**. |
| `default` (admin) | `CLICKHOUSE_ADMIN_PASSWORD` | Стандартный административный пользователь ClickHouse. Используется только при инициализации контейнера (`init.sh`). В рабочем процессе не применяется. |

### Telegram Admin Bot (`admin_bot/`)

Административный интерфейс на базе Aiogram 3:
- Управление токенами для всех коннекторов
- Запуск загрузок через Prefect API (точечно или массово)
- Автоматические уведомления о падениях flow (FAILED, CRASHED, CANCELLED)
- Система ролей: User → Alpha → Admin

---

## Быстрый старт

**1. Клонирование:**
```bash
git clone https://github.com/Naxofon/RoDL.git && cd RoDL
```

**2. Настройка окружения:**
```bash
cp .env.example .env
# Отредактируйте .env
```

**3. Запуск:**
```bash
docker compose up -d
```

Запустится ClickHouse, Prefect Server (http://localhost:4200), Prefect Worker, Bootstrap и Telegram Bot.

**4. Добавление токенов** — через Telegram-бот (раздел нужного коннектора).

**5. Первая загрузка** — через Prefect UI (Deployments → Run) или через бот.

---

## Конфигурация

### `config/loaders.yaml`

Единый файл конфигурации: статус загрузчиков и имена баз данных.

```yaml
databases:
  default: loader
  access: loader

loaders:
  direct_loader:
    enabled: true
    display_name: "💰 Яндекс.Директ"
    databases:
      direct_analytics: loader_direct_analytics
      direct_light: loader_direct_light
  metrika_loader:
    enabled: true
    display_name: "📊 Яндекс.Метрика"
    databases:
      metrika: loader_metrika
  calltouch_loader:
    enabled: true
    display_name: "📞 Calltouch"
    databases:
      calltouch: loader_calltouch
  vk_loader:
    enabled: true
    display_name: "🎯 VK Ads"
    databases:
      vk: loader_vk
  wordstat_loader:
    enabled: true
    display_name: "📈 Wordstat"
    databases:
      wordstat: loader_wordstat
  custom_loader:
    enabled: true
    display_name: "🧩 Custom AAM"
    databases:
      custom_loader: loader_custom
```

Чтобы отключить загрузчик — `enabled: false`. Имена БД настраиваются в `databases`.

### Переменные окружения (`.env`)

Скопируйте `.env.example` → `.env` и заполните значения.

#### ClickHouse

| Переменная | Описание |
|------------|----------|
| `CLICKHOUSE_USER` | Основной пользователь для работы с таблицами данных (SELECT, INSERT, ALTER, CREATE, TRUNCATE). Создаётся автоматически при старте контейнера. |
| `CLICKHOUSE_PASSWORD` | Пароль основного пользователя. |
| `CLICKHOUSE_ACCESS_USER` | Пользователь только для таблицы `loader.Accesses`. Используется ботом и коннекторами при чтении токенов. Минимальные привилегии. |
| `CLICKHOUSE_ACCESS_PASSWORD` | Пароль пользователя доступа. |
| `CLICKHOUSE_ADMIN_PASSWORD` | Пароль административного пользователя ClickHouse. Нужен только при инициализации контейнера — в работе системы не участвует. |
| `CLICKHOUSE_HOST` | Хост ClickHouse. Внутри Docker Compose — имя сервиса `clickhouse`. Для внешнего подключения — IP или hostname хоста. |
| `CLICKHOUSE_PORT` | HTTP-порт ClickHouse (по умолчанию `8123`). |

#### PostgreSQL (Prefect)

| Переменная | Описание |
|------------|----------|
| `PREFECT_DB_USER` | Пользователь PostgreSQL для Prefect. |
| `PREFECT_DB_PASSWORD` | Пароль пользователя PostgreSQL. |
| `PREFECT_DB_NAME` | Имя базы данных PostgreSQL. Хранит метаданные Prefect: flow runs, деплойменты, расписания. |

#### Система

| Переменная | Описание |
|------------|----------|
| `TZ` | Временная зона. Влияет на cron-расписания в `connectors/*/prefect/prefect.yaml`. Допустимые значения: [список TZ](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). |
| `PREFECT_API_URL` | URL Prefect API. `http://localhost:4200/api` — для обращений с хост-машины (CLI, браузер). Внутри контейнеров используется `http://prefect-server:4200/api` — это значение подставляется через `docker-compose.yml` автоматически. |

#### Telegram Bot

| Переменная | Описание |
|------------|----------|
| `ADMIN_BOT_TOKEN` | Токен Telegram-бота. Получается у [@BotFather](https://t.me/BotFather) → `/newbot`. |
| `ADMIN_REGISTRATION_CODE` | Секретный код для первичной регистрации администратора. Первый пользователь отправляет боту `/reg <код>` и получает роль Admin. Рекомендуется использовать случайную строку: `openssl rand -hex 16`. |

---

## Администрирование через Telegram

### Роли пользователей

| Роль | Доступные функции |
|------|-------------------|
| `User` | Нет доступа. После `/start` отправляется уведомление всем администраторам с Telegram ID и именем пользователя. |
| `Alpha` | Доступ только к выгрузке данных в коннекторах, где она настроена (`💾 Выгрузка`). Админ-панель и управление токенами/клиентами недоступны. |
| `Admin` | Всё, что доступно Alpha. Дополнительно: управление пользователями (добавить/снять роль Admin или Alpha), массовый запуск загрузки всех коннекторов, экспорт и импорт резервной копии таблицы `Accesses`, очистка устаревших клиентов, полный сброс базы данных. Получает автоматические уведомления об ошибках Prefect. |

**Первичная регистрация администратора:**

1. Отправить боту `/start` (создаётся запись с ролью `User`)
2. Отправить `/reg <код>` — роль повышается до `Admin`

Код берётся из переменной `ADMIN_REGISTRATION_CODE`. Последующих администраторов добавляет действующий Admin через `/a → Добавить администратора`.

### Команды

- `/start` — регистрация / вход в главное меню
- `/reg <код>` — первичная регистрация администратора
- `/a` — панель администратора (только для роли Admin; для `Alpha` команда недоступна)

### Управление токенами

Токены добавляются через раздел нужного коннектора в боте. Типы токенов и форматы ввода описаны в README каждого коннектора.

### Мониторинг

Бот автоматически опрашивает Prefect API каждые 35 минут (настраивается через `LOG_CHECK_INTERVAL`). При обнаружении flow runs в состоянии `FAILED`, `CRASHED` или `CANCELLED` отправляет уведомление всем администраторам.

---

## Деплойменты и расписание

Prefect-деплойменты определены в `connectors/<loader>/prefect/prefect.yaml` и создаются автоматически при старте `prefect-bootstrap` для включённых (`enabled: true`) коннекторов из `config/loaders.yaml`. Расписание, параметры и инструкция по ручному запуску — в README каждого коннектора.

---

## Модульность системы

Система построена вокруг модулей коннекторов: всё, что относится к конкретному источнику данных, лежит внутри `connectors/<loader>/`.

### Что входит в модуль коннектора

- `prefect/flows.py` и `prefect/prefect.yaml` — оркестрация и расписание.
- `prefect/clickhouse_utils.py` — ClickHouse-адаптер коннектора.
- `bot/` и `bot/admin/` (опционально) — пользовательские и админские хендлеры бота.
- `bot/plugin.py` — декларативное подключение коннектора в общий бот.
- `loader_service.py`, `access.py`, `tasks.py` и т.д. — доменная логика.

### Плюсы системы

- Включение/выключение коннектора через `config/loaders.yaml` без правок в ядре.
- Минимум конфликтов: каждый коннектор работает в своей папке.
- Быстрый перенос: можно делиться коннектором как отдельным модулем.
- Простое масштабирование: новые коннекторы добавляются по единому шаблону.

### Краткий гайд по новому коннектору

1. Создайте `connectors/my_loader/` и базовую бизнес-логику (`loader_service.py`, `access.py`).
2. Добавьте `connectors/my_loader/prefect/flows.py`, `connectors/my_loader/prefect/prefect.yaml`, `connectors/my_loader/prefect/clickhouse_utils.py`.
3. Добавьте bot-интеграцию: `connectors/my_loader/bot/handlers.py`, `.../keyboards.py`, `.../plugin.py` (и `bot/admin/` при необходимости).
4. Зарегистрируйте коннектор в `config/loaders.yaml` (`enabled`, `display_name`, `databases`).
5. Пересоздайте деплойменты: `docker compose restart prefect-bootstrap`.
6. Проверьте, что коннектор появился в Telegram-боте и его deployment создан в Prefect.

---

## Change Tracking

Ключевая функция системы: перезагружаются только те дни, где данные в ClickHouse расходятся с API.

**Алгоритм:**
1. Определить окно дат (сегодня − `lookback_days` ... вчера)
2. Запросить агрегаты из API и из ClickHouse
3. Сравнить построчно — если метрика отличается, день добавляется в список
4. Сгруппировать изменённые даты в непрерывные диапазоны
5. Удалить данные за эти диапазоны, загрузить свежие из API

**Параметры:**

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `lookback_days` | 60 | Глубина проверки в днях |
| `cost_tolerance` | 500 / 2 | Допустимое отклонение стоимости (только Direct: analytics / light) |
| `compare_conversions` | true | Сравнивать ли конверсии |

**Когда не использовать:** первичная загрузка исторических данных, полная перезагрузка после изменения структуры — в этих случаях задавайте явный `start_date`/`end_date`.

---

## Участие в разработке

- [CONTRIBUTING.md](./.github/CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](./.github/CODE_OF_CONDUCT.md)

## Безопасность

- [SECURITY.md](./.github/SECURITY.md)

## Лицензия

Проект распространяется по лицензии GNU AGPLv3 — [LICENSE](./LICENSE)

## Сообщество

- [Присоединяйтесь в сообщество](https://t.me/help_rodl) — актуальные вопросы, помощь и поддержка
