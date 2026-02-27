# RoDL — Система загрузки рекламных и аналитических данных

Система оркестрации и управления потоками данных из рекламных и аналитических систем (Яндекс.Директ, Яндекс.Метрика, Calltouch, VK Ads) в ClickHouse с использованием Prefect 3 и интеграцией Telegram-бота для администрирования.

## Содержание

- [Обзор проекта](#обзор-проекта)
- [Архитектура системы](#архитектура-системы)
- [Основные компоненты](#основные-компоненты)
- [Быстрый старт](#быстрый-старт)
- [Подробная конфигурация](#подробная-конфигурация)
- [Коннекторы данных](#коннекторы-данных)
- [Администрирование через Telegram](#администрирование-через-telegram)
- [Деплойменты и расписание](#деплойменты-и-расписание)
- [Управление изменениями (Change Tracking)](#управление-изменениями-change-tracking)
- [Разработка и расширение](#разработка-и-расширение)
- [Участие в разработке](#участие-в-разработке)
- [Изменения и версии](#изменения-и-версии)
- [Безопасность](#безопасность)
- [Лицензия](#лицензия)

---

## Обзор проекта

**RoDL** — система для автоматизированной загрузки, трансформации и хранения рекламных и аналитических данных. Проект решает задачу регулярной синхронизации статистики из API Яндекс.Директ, Яндекс.Метрика, Calltouch и VK Ads с внутренним хранилищем ClickHouse.

### Ключевые возможности

- **Оркестрация через Prefect 3**: управление workflow, планирование задач, мониторинг выполнения, retry-логика
- **Хранилище ClickHouse**: высокопроизводительное колоночное хранилище с оптимизированными схемами данных
- **4 коннектора данных**: Директ, Метрика, Calltouch, VK Ads — асинхронные загрузчики с batch-обработкой, rate limiting и обработкой ошибок
- **Change Tracking**: интеллектуальная система обнаружения изменений данных — перезагружаются только изменившиеся дни
- **Telegram Bot**: полнофункциональный административный интерфейс для управления клиентами, токенами и запуска задач
- **Docker Compose**: полная инфраструктура разворачивается одной командой
- **Гибкая конфигурация**: централизованное управление базами данных через YAML

### Преимущества решения

1. **Экономия ресурсов API**: благодаря change tracking загружаются только данные, которые действительно изменились
2. **Отказоустойчивость**: автоматические повторные попытки, обработка ошибок сети и квот API
3. **Масштабируемость**: параллельная обработка множества клиентов и счётчиков с контролем нагрузки
4. **Удобство управления**: веб-интерфейс Prefect + Telegram-бот для операторов
5. **Прозрачность**: детальное логирование всех операций, мониторинг через Prefect UI
6. **Гибкость**: простое добавление новых коннекторов и источников данных

---

## Архитектура системы

| Слой | Компонент | Описание |
|------|-----------|----------|
| **Источники данных** | Яндекс.Директ (Reports API) | Статистика рекламных кампаний |
| | Яндекс.Метрика (Logs/Reports API) | Логи визитов и агрегированные отчёты |
| | Calltouch API | Данные коллтрекинга |
| | VK Ads API | Статистика рекламных кампаний VK |
| **Коннекторы** | Direct Connector (`connectors/direct_loader/`) | change_tracker, rate limiters, uploader |
| | Metrika Connector (`connectors/metrika_loader/`) | change_tracker, operations, uploader |
| | Calltouch Connector (`connectors/calltouch_loader/`) | loader_service |
| | VK Connector (`connectors/vk_loader/`) | api, loader_service | |
| **Оркестрация** | Prefect Server (UI/API) | Веб-интерфейс и API на порту 4200 |
| | Prefect Worker(s) | Выполнение запланированных задач |
| | Prefect Bootstrap | Инициализация деплойментов |
| **Хранилище** | ClickHouse | БД: `loader`, `loader_direct_analytics`, `loader_direct_light`, `loader_metrika`, `loader_calltouch`, `loader_vk`  |
| | loader.Accesses | Централизованное хранилище токенов доступа |
| **Администрирование** | Telegram Bot (`admin_bot/`) | Управление клиентами, запуск задач, мониторинг логов |

### Поток данных

1. **Планирование**: Prefect Server по расписанию (cron) или по требованию запускает flow
2. **Обнаружение изменений**: Change Tracker сравнивает агрегаты API с данными в ClickHouse
3. **Определение задач**: Формируется список дат/клиентов для (пере)загрузки
4. **Параллельная загрузка**: Worker'ы выполняют задачи с соблюдением лимитов API
5. **Очистка данных**: Удаляются устаревшие записи за обновляемый период
6. **Запись в ClickHouse**: Данные записываются batch'ами с оптимизацией вставки
7. **Мониторинг**: Prefect UI и логи отображают статус выполнения

---

## Основные компоненты

### 1. Prefect 3 (Оркестратор)

Система оркестрации workflow с веб-интерфейсом, API и worker'ами.

**Сервисы:**
- **prefect-server**: веб-интерфейс (UI) и API на порту 4200
- **prefect-db**: PostgreSQL база для хранения метаданных Prefect
- **prefect-bootstrap**: инициализирует деплойменты при старте контейнеров
- **prefect-agent**: worker pool, выполняющий запланированные задачи

**Основные возможности:**
- Визуализация выполнения flow и task'ов
- Автоматические повторы при ошибках (retries)
- Мониторинг производительности и логов
- Управление расписаниями (schedule)
- REST API для интеграции (используется ботом)

### 2. ClickHouse (Хранилище данных)

Колоночная СУБД, оптимизированная для аналитических запросов.

**Базы данных:**
- `loader`: базовая БД для метаданных и служебной таблицы `Accesses`
- `loader_direct_analytics`: полная статистика Директа (множество полей, конверсии)
- `loader_direct_light`: облегчённая версия (основные метрики, обновляется чаще)
- `loader_metrika`: данные счётчиков Метрики (логи визитов, цели, измерения)
- `loader_calltouch`: данные коллтрекинга Calltouch
- `loader_vk`: статистика рекламных кампаний VK Ads

**Таблица Accesses:**
```sql
CREATE TABLE loader.Accesses (
    login Nullable(String),      -- логин клиента или идентификатор
    token Nullable(String),      -- OAuth токен доступа
    container Nullable(String),  -- группа/источник (agency, clients, ...)
    type Nullable(String)        -- тип сервиса (direct, metrika, metrika:agency, ...)
) ENGINE = MergeTree
ORDER BY (type, container, login);
```

Эта таблица централизованно хранит все токены доступа к API. Коннекторы читают токены отсюда.

**Пользователи:**
- `default_user` (или из `CLICKHOUSE_USER`): основной пользователь для чтения/записи данных
- `access_user` (или из `CLICKHOUSE_ACCESS_USER`): пользователь только для таблицы `Accesses`
- `root`/`default`: административные пользователи для инициализации

### 3. Коннекторы (connectors/)

#### Direct Connector (`connectors/direct_loader/`)

Загружает статистику рекламных кампаний из API Яндекс.Директ.

**Файлы:**
- `config.py`: профили данных (analytics/light), списки полей, настройки rate limiting
- `access.py`: получение токенов и списков клиентов из ClickHouse
- `jobs.py`: логика загрузки отчётов через API `/reports`, обработка целей
- `tasks.py`: Prefect tasks для параллельной обработки клиентов
- `uploader.py`: запись данных в ClickHouse
- `change_tracker.py`: сравнивает агрегаты API (Impressions, Clicks, Cost, SumConversion) с БД
- `report_limits.py`: контроллеры rate limiting (GlobalRateLimiter, PerAdvertiserRateLimiter, OfflineReportTracker)

**Профили данных:**
- **analytics**: полный набор полей (~25 колонок + колонки целей), включая конверсии, демографию, устройства
- **light**: упрощённый набор (12 полей), обновляется каждый час для оперативного мониторинга

**Ключевые возможности:**
- Получение списка клиентов агентского аккаунта
- Автоматическое определение целей кампаний (GoalId)
- Batch-обработка по 5 дней для больших диапазонов
- Rate limiting с учётом квот API (20 req/10s, 5 offline reports)
- Обработка ошибки 9000 (переполнение очереди) с ожиданием

#### Metrika Connector (`connectors/metrika_loader/`)

Загружает логи визитов и отчёты из API Яндекс.Метрика.

**Файлы:**
- `access.py`: получение токенов и списков счётчиков (включая избранные агентские)
- `jobs.py`: логика загрузки данных из Logs API и Reporting API
- `operations.py`: операции обработки и трансформации данных
- `uploader.py`: запись данных в ClickHouse
- `change_tracker.py`: сравнивает количество визитов и конверсий по дням
- `change_utils.py`: вспомогательные функции для change tracking

**Особенности:**
- Параллельная загрузка из двух API (Logs + Reporting), объединение по `visitID`
- Классификация целей: целевые `g_` (содержат "madd" в названии) и уникальные `u_` (все остальные)
- Автоматическое расширение схемы при появлении новых полей
- Дедупликация визитов по `visitID`
- Batch-загрузка с разделением на 5-дневные периоды

#### Calltouch Connector (`connectors/calltouch_loader/`)

Загружает данные коллтрекинга и заявок из API Calltouch.

**Файлы:**
- `loader_service.py`: загрузка звонков и заявок, нормализация, запись в ClickHouse

**Источники данных:**
- **Calls Diary API** (`/calls-diary/calls`): дневник звонков с пагинацией (по 1000 записей)
- **Requests API** (`/requests`): заявки (лиды) с сайта

Данные звонков и заявок объединяются в единую таблицу `ct_{site_id}`.

**Поля итоговой таблицы:**
- Дата, теги (категория, тип, названия), комментарии, менеджер, атрибуция
- Флаги: `uniqueCall`, `targetCall`, `uniqTargetCall`, `callbackCall`, `uniqueRequest`, `targetRequest`, `uniqTargetRequest`
- Длительность звонка (`CallDuration`), успешность (`CallSuccessful`)
- UTM-параметры: `source`, `medium`, `utmTerm`, `utmContent`, `utmCampaign`
- Сессия: `sessionId`, `yaClientId`, `city`, `browser`, `device`, `os`
- URL, домен (извлекается автоматически), реферер

**Особенности:**
- Параллельная обработка клиентов (семафор на 5 одновременных)
- Автоматическая очистка данных за обновляемый период перед записью
- Извлечение структурированных тегов и комментариев из вложенных JSON
- Нормализация типов: булевы флаги, числовые ID, строковые поля

#### VK Connector (`connectors/vk_loader/`)

Загружает статистику рекламных кампаний из VK Ads API (ads.vk.ru).

**Файлы:**
- `api.py`: клиент VK Ads API (получение токенов, рекламных планов, статистики)
- `loader_service.py`: нормализация данных и запись в ClickHouse
- `access.py`: получение агентских аккаунтов и списков активных клиентов
- `config.py`: схема данных, лимиты, списки полей

**Поля итоговой таблицы (~45 колонок):**
- Кампания: `campaign_name`, `campaign_id`, `group_name`, `group_id`, `date`
- Базовые метрики: `base_shows`, `base_clicks`, `base_spent`, `base_vk_goals`
- События: клики по внешним ссылкам, комментарии, вступления, лайки, репосты, отправки форм, голосования
- Социальные сети: вступления и сообщения в VK и ОК
- Уникальные показатели: `uniques_total`, `uniques_increment`, `uniques_frequency`
- Видео: глубина просмотра, старты, просмотры на 25/50/75/100%, паузы, звук

**Особенности:**
- Поддержка агентских аккаунтов с автоматическим обнаружением активных клиентов
- Нормализация вложенных структур VK API в плоские колонки (dict → `prefix_key`)
- Управление OAuth-токенами: получение и удаление per-client токенов через API
- Retry-логика: до 3 попыток с экспоненциальным backoff (база 2.0)
- Таймаут: 120 секунд на запрос

### 4. Telegram Admin Bot (`admin_bot/`)

Полнофункциональный административный интерфейс на базе Aiogram 3.

**Основные модули:**
- `app.py`: точка входа, инициализация dispatcher'а и роутеров
- `handlers/commands.py`: обработка команд `/start`, авторизация пользователей
- `handlers/direct.py`: управление клиентами Директа
- `handlers/metrika.py`: управление счётчиками Метрики
- `handlers/calltouch.py`: управление Calltouch
- `handlers/vk.py`: управление VK Ads
-`handlers/admin_panel.py`: админ-панель и управление пользователями
- `handlers/logs.py`: мониторинг flow runs через Prefect API с автоматическими уведомлениями
- `services/prefect_client.py`: клиент для взаимодействия с Prefect API
- `database/user.py`: ClickHouse репозиторий для управления пользователями и ролями (таблица `AdminUsers` в БД `access` из `loaders.yaml`, создаётся init.sh)

**Возможности:**
- **Управление доступами**: добавление/удаление токенов для всех коннекторов (Директ, Метрика, Calltouch, VK)
- **Запуск задач**: инициирование загрузок через Prefect API для всех клиентов или отдельного
- **Выгрузка отчётов**: генерация CSV с текущими клиентами
- **Мониторинг**: автоматическое уведомление администраторов о проблемных flow runs (FAILED, CRASHED, CANCELLED)
- **Система ролей**: User (ожидание одобрения), Alpha (основной доступ), Admin (полный доступ)

**Интеграция с Prefect:**
Бот запускает задачи через Prefect API, создавая flow runs с параметрами:
- Массовые обновления: `track_changes=True` для всех клиентов/счётчиков
- Точечные обновления: указывается конкретный `login` или `counter_id`
- Теги: `admin_bot`, `single_upload`/`bulk_upload`

### 5. Утилиты (`orchestration/clickhouse_utils/`)

Пакет модулей для работы с ClickHouse. Каждый коннектор имеет свой асинхронный класс БД.

**Модули:**
- `connection.py`: подключение к ClickHouse (с кешированием клиентов)
- `config.py`: чтение имён БД из `loaders.yaml`
- `database.py`: базовый класс `ClickhouseDatabase` с общими методами
- `schema.py`: управление схемой таблиц (создание, расширение колонок)
- `helpers.py`: вспомогательные функции (`sanitize_login`, `detect_date_column`, `detect_visits_column`)
- `direct.py`, `metrika.py`, `calltouch.py`, `vk.py`: асинхронные классы БД для каждого коннектора

**Ключевые возможности:**
- Асинхронные операции чтения/записи с автоматической очисткой по диапазону дат
- Управление токенами доступа (`upsert_accesses`, `fetch_access_tokens`, `delete_access`)
- Автоматическое определение и расширение схемы из pandas DataFrame
- Агрегация данных по дням для change tracking

---

## Быстрый старт

### Предварительные требования

- Docker и Docker Compose (версия 20.10+)
- 8 ГБ оперативной памяти (минимум 4 ГБ)
- 50 ГБ свободного места на диске

### Шаг 1: Клонирование репозитория

```bash
mkdir rodl && cd rodl
git clone <repository-url> prefect_loader
```

### Шаг 2: Настройка переменных окружения

Скопируйте пример конфигурации:

```bash
cp .env.example .env
```

Отредактируйте `.env`:

```bash
# Пользователи ClickHouse
CLICKHOUSE_USER=default_user
CLICKHOUSE_PASSWORD=your_secure_password

CLICKHOUSE_ACCESS_USER=access_user
CLICKHOUSE_ACCESS_PASSWORD=access_secure_password

# Хост ClickHouse (для docker-compose используйте "clickhouse")
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123

# Часовой пояс
TZ=Asia/Novosibirsk

# API Prefect
PREFECT_API_URL=http://localhost:4200/api

# Telegram бот
ADMIN_BOT_TOKEN=your_telegram_bot_token
ADMIN_REGISTRATION_CODE=your_secure_registration_code
```

### Шаг 3: Запуск инфраструктуры

```bash
docker compose up -d
```

Это запустит:
- ClickHouse на портах 8123 (HTTP) и 9000 (Native)
- Prefect Server UI на http://localhost:4200
- Prefect Worker (агент для выполнения задач)
- Prefect Bootstrap (автоматически создаст деплойменты)


Откройте Prefect UI: http://localhost:4200

### Шаг 4: Добавление токенов доступа

Токены добавляются через Telegram-бот — см. раздел [Администрирование через Telegram](#администрирование-через-telegram).

### Шаг 5: Запуск первой загрузки

**Prefect UI:**
1. Откройте http://localhost:4200
2. Перейдите в Deployments
3. Найдите `direct-analytics-change` или `metrika-loader-clickhouse`
4. Нажмите "Run" → "Quick run"

**Telegram Bot:**
1. Отправьте `/start` боту
2. Получите доступ от администратора
3. Выберите раздел Direct или Metrika
4. Используйте кнопку "Обновить все данные"

---

## Подробная конфигурация

### Конфигурация загрузчиков и баз данных (`config/loaders.yaml`)

Единый файл конфигурации. Читается `orchestration/loader_registry.py` (статус загрузчиков) и `orchestration/clickhouse_utils/config.py` (имена БД).

```yaml
databases:
  default: loader   # служебная БД и таблица Accesses
  access: loader

loaders:
  direct_loader:
    enabled: true
    display_name: "Яндекс.Директ"
    databases:
      direct_analytics: loader_direct_analytics
      direct_light: loader_direct_light
  metrika_loader:
    enabled: true
    display_name: "Яндекс.Метрика"
    databases:
      metrika: loader_metrika
  calltouch_loader:
    enabled: true
    display_name: "Calltouch"
    databases:
      calltouch: loader_calltouch
  vk_loader:
    enabled: true
    display_name: "VK Ads"
    databases:
      vk: loader_vk
```

Чтобы отключить загрузчик — `enabled: false`. Чтобы переименовать БД — измените значение в `databases`. Скрипт `validate_loaders.sh` проверяет конфигурацию при каждом старте контейнера.

### Переменные окружения

Все переменные задаются в `.env` (см. `.env.example`). Имена баз данных настраиваются в `config/loaders.yaml`.

#### ClickHouse

```bash
CLICKHOUSE_USER=default_user         # основной пользователь для данных
CLICKHOUSE_PASSWORD=strong_password

CLICKHOUSE_ACCESS_USER=access_user   # пользователь только для таблицы Accesses
CLICKHOUSE_ACCESS_PASSWORD=access_password

CLICKHOUSE_HOST=clickhouse           # или localhost для локальной работы
CLICKHOUSE_PORT=8123                 # HTTP интерфейс
```

#### Системные

```bash
TZ=Asia/Novosibirsk                  # часовой пояс (для корректного планирования)
PREFECT_API_URL=http://localhost:4200/api
```

#### Telegram Bot

```bash
ADMIN_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
ADMIN_REGISTRATION_CODE=your_secure_code  # код регистрации новых пользователей бота
```

### Настройка пользователей ClickHouse

Скрипт инициализации (`config/clickhouse_setup/init.sh`) автоматически создаёт:

1. **Базы данных** из `config/loaders.yaml`
2. **Таблицу Accesses** в базе `loader`
3. **Пользователей** с необходимыми правами:
   - `default_user`: полный доступ к базам данных (SELECT, INSERT, ALTER, CREATE, TRUNCATE)
   - `access_user`: доступ только к таблице `Accesses`

---

## Коннекторы данных

### Директ (Direct Connector)

#### Профили данных

**Analytics** — полный профиль для глубокой аналитики:
- Все основные метрики (показы, клики, стоимость, CTR, CPC)
- Конверсии по всем целям (до 10 целей одновременно)
- Демографические данные (пол, возраст)
- Устройства и типы кампаний
- Географические данные
- Расширенные атрибуции

**Light** — облегчённый профиль для быстрых дашбордов:
- Основные метрики (показы, клики, стоимость, конверсии)
- Минимум полей (12 колонок)
- Обновляется каждый час (быстрее аналитики)

#### Работа с токенами

Токены добавляются и удаляются через Telegram-бот — см. раздел [Управление клиентами Директ](#управление-клиентами-директ).

Поддерживаемые типы (`type` в таблице `Accesses`):
- `agency_token` — агентский токен (login = `-`, охватывает всех клиентов агентства)
- `agency_parsed` — клиент, найденный через агентский аккаунт
- `not_agency_token` — токен отдельного клиента

#### Change Tracking для Директа

Алгоритм обнаружения изменений:

1. Запрашивает агрегированные данные из API за последние 60 дней (по умолчанию)
2. Запрашивает аналогичные агрегаты из ClickHouse
3. Сравнивает по каждому дню:
   - Показы (Impressions)
   - Клики (Clicks)
   - Стоимость (Cost) с допуском `cost_tolerance`
   - Конверсии (SumConversion)
4. Формирует список дат с расхождениями
5. Группирует даты в непрерывные диапазоны
6. Удаляет данные за эти диапазоны
7. Перезагружает данные

**Параметры:**
- `lookback_days`: глубина проверки (по умолчанию 60)
- `cost_tolerance`: допустимое отклонение стоимости (500 для analytics, 2 для light)
- `compare_conversions`: сравнивать ли конверсии (по умолчанию True)

#### Rate Limiting

Direct connector использует несколько уровней ограничений:

- **API семафор**: максимум 5 одновременных запросов к API
- **Global Rate Limiter**: 19 запросов за 10 секунд глобально
- **Per-Advertiser Rate Limiter**: 20 запросов за 10 секунд на рекламодателя (требование API)
- **Offline Reports Tracker**: не более 5 оффлайн отчётов одновременно на пользователя
- **Обработка ошибки 9000**: при переполнении очереди ожидание с сохранением слота

### Метрика (Metrika Connector)

#### Источники данных

Метрика использует два API одновременно:

1. **Logs API**: детальные логи визитов
   - Поля визита: dateTime, visitID, clientID, параметры UTM, источники трафика
   - Массивы целей: goalsID[], goalsDateTime[]

2. **Reporting API**: агрегированные отчёты
   - Измерения: пол, возраст, интересы, устройства
   - Метрики: визиты, просмотры, отказы, время на сайте

Данные объединяются по `visitID` для получения полной картины.

#### Классификация целей

Цели автоматически классифицируются по регулярным выражениям:

- **`g_` (целевые)**: цели, содержащие в названии в яндекс метрики "madd"
- **`u_` (уникальные)**: уникальные достижения целей (дедупликация по визиту)

Для каждой категории считается `*_sum_goal` — сумма всех конверсий категории.

#### Работа с токенами

Токены добавляются и удаляются через Telegram-бот — см. раздел [Управление счётчиками Метрики](#управление-счётчиками-метрики).

Поддерживаемые типы (`type` в таблице `Accesses`):
- `metrika:client` — токен конкретного счётчика (login = yandex_login, counter_id в отдельном поле)
- `metrika:agency` — агентский токен; система автоматически получит список счётчиков, помеченных как «избранные» (favorite=1) через Management API

#### Change Tracking для Метрики

Алгоритм аналогичен Директу, но сравниваются:

1. Количество визитов (`visits`) по дням
2. Сумма уникальных конверсий по всем целям (`APIConversions`)

Метрика использует параллельную загрузку с ограничениями:
- Глобально: до 9 параллельных fetch-операций
- На один токен: до 3 одновременных запросов (ограничение API)

---

## Администрирование через Telegram

Telegram Bot предоставляет удобный интерфейс для управления системой без доступа к серверу.

### Запуск бота

```bash
# Убедитесь, что ADMIN_BOT_TOKEN установлен в .env
docker compose up -d admin-bot
```

### Система ролей

- **User**: новый пользователь, ожидает одобрения администратора
- **Alpha**: базовый доступ к функциям (просмотр данных, запуск задач для своих клиентов)
- **Admin**: полный доступ (управление пользователями, массовые операции, просмотр логов)

### Основные команды

- `/start` — регистрация/авторизация
- `/a` — админ-панель (только для Admin)
- `/reg <код>` — регистрация по коду (`ADMIN_REGISTRATION_CODE`)

### Управление клиентами Директ

1. **Добавить агентский токен**:
   - Раздел "Директ" → "Добавить агентский токен"
   - Формат: `login token [container]` (login можно указать как `-`)
   - Токен сохраняется с типом `agency_token`

2. **Добавить клиентский токен**:
   - Раздел "Директ" → "Добавить клиентский токен"
   - Формат: `login token [container]`
   - Токен сохраняется с типом `not_agency_token`

3. **Удалить клиента**:
   - Раздел "Директ" → "Удалить клиента"
   - Введите login клиента

4. **Обновить данные**:
   - Раздел "Директ" → "Обновить данные"
   - Выберите профиль (Аналитическая БД / Легкая БД)
   - Для всех: выберите "Все клиенты"
   - Для одного: введите login

### Управление счётчиками Метрики

1. **Добавить счётчик**:
   - Раздел "Метрика" → "Добавить счётчик"
   - Формат: `yandex_login 12345678 y0_AgAAA...`

2. **Удалить счётчик**:
   - Раздел "Метрика" → "Удалить счётчик"
   - Введите counter_id

3. **Обновить данные**:
   - Раздел "Метрика" → "Обновить данные"
   - Для всех: выберите "Все счётчики"
   - Для одного: введите counter_id

### Мониторинг

Бот автоматически мониторит выполнение flow runs через Prefect API:
- Проверка каждые 35 минут (настраивается через `LOG_CHECK_INTERVAL`)
- При обнаружении flow runs в состояниях `FAILED`, `CRASHED`, `CANCELLED` отправляет уведомление администраторам

---

## Деплойменты и расписание

Prefect деплойменты определены в `orchestration/prefect.yaml` и автоматически создаются при запуске `prefect-bootstrap`.

Описание деплойментов каждого коннектора (расписание, параметры, запуск вручную) находится в README соответствующего коннектора в `connectors/`.

---

## Управление изменениями (Change Tracking)

Change Tracking — ключевая функция системы, позволяющая экономить ресурсы API и ускорять обновление данных.

### Принцип работы

1. Определить окно дат (сегодня − `lookback_days` ... вчера)
2. Запросить агрегаты из API (по дням: показы, клики, стоимость, конверсии)
3. Запросить аналогичные агрегаты из ClickHouse
4. Сравнить построчно — если хотя бы одна метрика отличается, день добавляется в список
5. Сгруппировать изменённые даты в непрерывные диапазоны
6. Удалить данные за эти диапазоны из ClickHouse
7. Загрузить свежие данные из API

### Настройка параметров

**Глубина проверки (`lookback_days`)**:
- По умолчанию: 60 дней
- Рекомендуется: 30-90 дней в зависимости от частоты изменений
- Чем больше значение, тем больше нагрузка на API при каждом запуске

**Допуск стоимости (`cost_tolerance`)** — только для Direct:
- По умолчанию: 500 (analytics), 2 (light)
- Игнорирует расхождения в стоимости ниже порога

**Сравнение конверсий (`compare_conversions`)**:
- По умолчанию: True
- Если False, сравниваются только показы/клики/стоимость (быстрее)

### Когда НЕ использовать Change Tracking

- Первичная загрузка исторических данных (укажите явный `start_date`/`end_date`)
- Полная перезагрузка после изменения структуры данных
- Принудительное обновление (например, после ручных правок в БД)

---

## Разработка и расширение

### Добавление нового коннектора

#### Шаг 1: Создайте структуру коннектора

```bash
mkdir -p connectors/my_connector
touch connectors/my_connector/__init__.py
touch connectors/my_connector/loader_service.py
touch connectors/my_connector/change_tracker.py
```

#### Шаг 2: Реализуйте базовые функции

**`loader_service.py`:**

```python
import asyncio
from prefect_loader.clickhouse_utils import get_client, write_dataframe
import pandas as pd

async def fetch_data(start_date: str, end_date: str, client_id: str) -> pd.DataFrame:
    """Получить данные из API."""
    # Ваша логика получения данных
    pass

async def upload_data(client_id: str, start_date: str, end_date: str):
    """Загрузить данные в ClickHouse."""
    client = get_client(database='loader_my_connector')
    df = await fetch_data(start_date, end_date, client_id)

    await write_dataframe(
        client=client,
        database='loader_my_connector',
        table=f'client_{client_id}',
        df=df,
        start_date=start_date,
        end_date=end_date
    )
```

**`change_tracker.py`:**

```python
async def detect_changes(client_id: str, lookback_days: int = 60):
    """Обнаружить изменения за период."""
    # 1. Получить агрегаты из API
    # 2. Получить агрегаты из ClickHouse
    # 3. Сравнить
    # 4. Вернуть список дат для обновления
    return {
        'changes_detected': True,
        'days_to_update': ['2024-12-01', '2024-12-02']
    }
```

#### Шаг 3: Добавьте конфигурацию в `config/loaders.yaml`

```yaml
loaders:
  my_connector:
    enabled: true
    display_name: "My Connector"
    databases:
      my_connector: loader_my_connector
```

#### Шаг 4: Создайте Prefect flow

В `orchestration/flows/my_connector.py`:

```python
from prefect_loader.connectors.my_connector.loader_service import upload_data
from prefect_loader.connectors.my_connector.change_tracker import detect_changes

@flow(name="my-connector-flow")
async def my_connector_flow(
    client_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    track_changes: bool = True
):
    logger = get_run_logger()

    if track_changes:
        changes = await detect_changes(client_id)
        if not changes['changes_detected']:
            logger.info("No changes detected")
            return
        # Загрузить только изменённые даты
        for date in changes['days_to_update']:
            await upload_data(client_id, date, date)
    else:
        await upload_data(client_id, start_date, end_date)
```

#### Шаг 5: Добавьте деплоймент

В `orchestration/prefect.yaml`:

```yaml
deployments:
  - name: my-connector-daily
    entrypoint: orchestration/flows/my_connector.py:my_connector_flow
    schedule:
      cron: "0 8 * * *"
      timezone: Asia/Novosibirsk
    parameters:
      track_changes: true
```

#### Шаг 6: Пересоздайте деплойменты

```bash
docker compose restart prefect-bootstrap
```

## Участие в разработке

Правила вклада и требования к Pull Request:

- [CONTRIBUTING.md](./.github/CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](./.github/CODE_OF_CONDUCT.md)

## Изменения и версии

- [CHANGELOG.md](./CHANGELOG.md)
- [Release Notes v0.1.0](./RELEASE_NOTES_0.1.0.md)

## Безопасность

Политика ответственного раскрытия уязвимостей:

- [SECURITY.md](./.github/SECURITY.md)

## Лицензия

Проект распространяется по лицензии GNU AGPLv3:

- [LICENSE](./LICENSE)
