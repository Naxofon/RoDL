# Добавление клиента в Accesses

Этот коннектор нужен только для ручного добавления токена клиента в таблицу
ClickHouse `Accesses` через Prefect UI.


Откройте Prefect UI -> Deployments -> `add-client-access` -> Run -> Custom Run.

## Параметры

- `login`: логин клиента, site id или другой id конкретного
  коннектора.
- `token`: API-токен.
- `service_type`: имя коннектора, например `metrika`, `direct`, `callibri`.
- `container`: обязательное поле для `service_type` = `metrika`, или необязательное поле для группировки.
- `type_value`: необязательный subtype. Здесь не нужно повторять `service_type`.

Повторный запуск с тем же `login`, `container`, `service_type` и `type_value`
перезаписывает существующую запись: старая строка удаляется и вставляется одна
актуальная строка. Отдельный параметр `replace` не используется, чтобы случайно
не удалить другие доступы с тем же `container` и типом.

При добавлении через этот flow поле `analytics_enabled` всегда записывается как
`1`. Если для клиента нужно отключить аналитику, это можно сделать отдельно
через Telegram-бота.

`service_type` и `type_value` вместе сохраняются в поле `Accesses.type`.
Например, `service_type: direct` и `type_value: not_agency_token` будут
сохранены как `direct:not_agency_token`.

## Примеры

### Клиент Метрики

```yaml
login: "login_ym"
token: "oauth-token"
service_type: "metrika"
container: "12345567"
type_value: "client"
```

### Клиентский токен Директа

```yaml
login: "client-login"
token: "oauth-token"
service_type: "direct"
container: "agency-name"
type_value: "not_agency_token"
```

### Агентский токен Директ

```yaml
login: "greenarts"
token: "oauth-token"
service_type: "direct"
container: null
type_value: "agency_token"
```

### Агентский токен Метрика

```yaml
login: "greenarts"
token: "oauth-token"
service_type: "metrika"
container: null
type_value: "agency"
```

### Calltouch

```yaml
login: "site_id"
token: "oauth-token"
service_type: calltouch
container: "login"
type_value: null
```

### Roistat

```yaml
login: "site_id"
token: "oauth-token"
service_type: roistat:analytics=1;calls=1;visits=0
container: "name_client"
type_value: null
```

### VK

```yaml
login: "login_id"
token: "oauth-token"
service_type: vk
container: false
type_value: null
```
