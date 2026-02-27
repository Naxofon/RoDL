# VK Ads Connector for Prefect Loader

This connector enables automatic data loading from VK Ads into ClickHouse database using Prefect workflows.

## Features

- **Multi-account support**: Manages multiple VK advertising accounts (both agency and client accounts)
- **Agency client auto-discovery**: Automatically fetches and tracks active clients from agency accounts
- **ClickHouse integration**: Efficient data storage in ClickHouse database
- **Prefect orchestration**: Reliable workflow management with retries and error handling
- **Admin bot integration**: Easy management through Telegram bot interface

## Architecture

The connector follows the modern architecture pattern used by other connectors in this project:

```
connectors/vk_loader/
├── __init__.py
├── config.py           # Configuration and constants
├── api.py              # VK Ads API client
├── access.py           # Access token management
├── loader_service.py   # Main data upload logic
└── README.md           # This file

orchestration/clickhouse_utils/
└── vk.py               # AsyncVkDatabase wrapper

orchestration/flows/    # Prefect tasks and flows
admin_bot/              # Telegram bot integration
```

## Setup

### 1. Environment Variables

Set the following environment variables:

```bash
# Required: VK API credentials
export VK_CLIENT_ID="your_vk_client_id"
export VK_CLIENT_SECRET="your_vk_client_secret"

# Optional: Database configuration
export CLICKHOUSE_HOST="clickhouse"
export CLICKHOUSE_PORT="8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="your_password"
```

### 2. Get VK API Credentials

1. Go to VK Ads API documentation: https://ads.vk.ru/api
2. Create a new application or use an existing one
3. Obtain your `client_id` and `client_secret`

### 3. Database Setup

The connector automatically creates the required database and tables. The default database name is `loader_vk` but can be customized via configuration files.

## Usage

### Via Prefect Tasks

```python
from prefect import flow
from prefect_loader.orchestration.flows import run_vk_all, run_vk_single

# Upload data for all users (last 10 days)
@flow
async def my_vk_flow():
    await run_vk_all(lookback_days=10)

# Upload data for a specific user
@flow
async def my_vk_single_flow():
    await run_vk_single(
        user_id="12345678",
        start_date="2025-12-01",
        end_date="2025-12-10"
    )
```

### Via Admin Bot

1. Start the admin bot
2. Navigate to: Main Menu → 🎯 VK Ads
3. Available operations:
   - **📋 Клиенты VK**: View all configured VK clients
   - **➕ Агентский**: Add agency-level token
   - **➕ Клиентский**: Add client-level token
   - **⛔ Удалить клиента**: Remove a client
   - **🏢 Удалить агентство**: Remove an agency
   - **💾 Выгрузка**: Trigger data upload

### Programmatic Usage

```python
from connectors.vk_loader.loader_service import upload_data_for_all_users
from connectors.vk_loader.access import get_vk_credentials
from connectors.vk_loader.api import VkApiClient
from orchestration.clickhouse_utils import AsyncVkDatabase

# Upload data for all users
result = await upload_data_for_all_users(
    start_date="2025-12-01",
    end_date="2025-12-10",
)

# Work with API client
client_id, client_secret = get_vk_credentials()
api_client = VkApiClient(client_id, client_secret)

# Get agency clients
token = await api_client.get_access_token()
clients_df = await api_client.get_agency_clients(token)

# Work with database
db = AsyncVkDatabase()
await db.init_db()

# Add agency token
await db.add_agency_token(
    agency_user_id="12345678",
    token="your_token",
    container="my_agency"
)

# Get user tokens
user_tokens = await db.get_user_id_token_dictionary()
```

## Data Schema

The connector creates tables with the following structure:

### Table Naming

Tables are named `vk_{user_id}` where `user_id` is the VK user ID.

### Columns

- **date** (DateTime): Date of the statistics
- **campaign_name** (String): Campaign name
- **campaign_id** (String): Campaign ID
- **group_name** (String): Ad group name
- **group_id** (String): Ad group ID

**Base Metrics:**
- `base_shows` (Int64): Impressions count
- `base_clicks` (Int64): Clicks count
- `base_spent` (Float64): Amount spent
- `base_vk_goals` (Int64): VK goals achieved

**Event Metrics:**
- `events_clicks_on_external_url` (Int64)
- `events_comments` (Int64)
- `events_joinings` (Int64)
- `events_launching_video` (Int64)
- `events_likes` (Int64)
- `events_moving_into_group` (Int64)
- `events_opening_app` (Int64)
- `events_opening_post` (Int64)
- `events_sending_form` (Int64)
- `events_shares` (Int64)
- `events_votings` (Int64)

**Social Network Metrics:**
- `social_network_ok_message` (Int64)
- `social_network_result_join` (Int64)
- `social_network_result_message` (Int64)
- `social_network_vk_join` (Int64)
- `social_network_vk_message` (Int64)
- `social_network_vk_subscribe` (Int64)

**Unique Metrics:**
- `uniques_frequency` (Float64)
- `uniques_increment` (Int64)
- `uniques_total` (Int64)
- `uniques_video_depth_of_view` (Float64)
- `uniques_video_started` (Int64)
- `uniques_video_viewed_25_percent` (Int64)
- `uniques_video_viewed_10_seconds` (Int64)
- `uniques_video_viewed_50_percent` (Int64)
- `uniques_video_viewed_75_percent` (Int64)
- `uniques_video_viewed_100_percent` (Int64)

**Video Metrics:**
- `video_depth_of_view` (Float64)
- `video_first_second` (Int64)
- `video_paused` (Int64)
- `video_resumed_after_pause` (Int64)
- `video_sound_turned_off` (Int64)
- `video_sound_turned_on` (Int64)
- `video_started` (Int64)
- `video_started_cost` (Float64)
- `video_viewed_10_seconds` (Int64)
- `video_viewed_100_percent` (Int64)
- `video_viewed_75_percent` (Int64)
- `video_viewed_50_percent` (Int64)
- `video_viewed_25_percent` (Int64)

## Access Management

The connector uses the centralized `Accesses` table in ClickHouse to manage API tokens.

### Access Types

Stored in the `type` column as "vk:{subtype}":

- **vk:agency_token**: Agency-level token (can access all agency clients)
- **vk:agency_parsed**: Client token parsed from agency (auto-discovered)
- **vk:not_agency_token**: Direct client token (manually added)

### Priority System

When multiple tokens exist for the same user ID, priority is:
1. **not_agency_token** (highest) - Direct client tokens
2. **agency_parsed** - Agency-discovered clients
3. **null/unknown** (lowest) - Legacy or unspecified

## How It Works

### Data Upload Flow

1. **Token Collection**:
   - Fetches all VK tokens from the Accesses table
   - Refreshes agency clients if agency tokens exist
   - Filters for active clients with recent activity

2. **Data Fetching**:
   - For each user:
     - Fetches ad plans (campaigns) and ad groups
     - Requests daily statistics via VK Ads API
     - Flattens nested metric structures

3. **Data Processing**:
   - Normalizes DataFrame to match expected schema
   - Converts data types (Int64, Float64, String)
   - Fills missing columns with default values

4. **Data Storage**:
   - Deletes existing data in the date range
   - Inserts new data into ClickHouse
   - Creates table if doesn't exist

### Agency Client Discovery

1. Resets agency token
2. Gets new agency token
3. Fetches all agency clients
4. Filters for active clients
5. Checks each client for recent activity (impressions in last 10 days)
6. Updates Accesses table with active clients

## Configuration

### config.py

Key configuration constants:

```python
# API settings
VK_API_BASE_URL = "https://ads.vk.ru/api/v2"
VK_TIMEOUT_SECONDS = 120
VK_MAX_RETRIES = 3

# Default lookback days
VK_DEFAULT_LOOKBACK_DAYS = 10

# Access types
VK_TYPE_AGENCY_TOKEN = "agency_token"
VK_TYPE_AGENCY_PARSED = "agency_parsed"
VK_TYPE_NOT_AGENCY = "not_agency_token"
```

### Database Configuration

Database name can be customized in `config/loaders.yaml` under the `vk_loader` section:

```yaml
loaders:
  vk_loader:
    databases:
      vk: "my_custom_vk_database"
```

Or via environment variable pointing to a custom `loaders.yaml`:

```bash
export LOADER_CONFIG_PATH="/path/to/loaders.yaml"
```

## Error Handling

- **API errors**: Exponential backoff retry (max 3 attempts)
- **Token errors**: Logged and skipped, processing continues
- **Database errors**: Retried at task level by Prefect
- **Missing data**: Empty DataFrames handled gracefully

## Logging

Logs are available through:
- Prefect logger (when running as Prefect task)
- Module logger (when used standalone)

Example log messages:
```
INFO: 12345678: Starting data upload (2025-12-01 → 2025-12-10)
INFO: 12345678: Fetching ad plans and groups
INFO: 12345678: Fetching statistics for 5 plans
INFO: 12345678: Writing 250 rows to table vk_12345678
INFO: 12345678: Upload completed successfully
```

## Migration from Legacy Code

If you were using an older PostgreSQL-based VK loader:

### Migration Steps

1. **Environment Variables**: Update to use new naming
   - Old: `VK_DB_USER`, `VK_DB_PASSWORD`, `VK_DB_HOST`, `VK_DB_NAME`
   - New: Use standard `CLICKHOUSE_*` variables + `VK_CLIENT_ID` and `VK_CLIENT_SECRET`

2. **Database**: Migrate from PostgreSQL to ClickHouse
   - Old data was in PostgreSQL tables named `vk_*`
   - New data goes to ClickHouse database `loader_vk` with same table naming pattern
   - No automatic data migration - re-pull historical data if needed

3. **Scheduling**: Replace APScheduler with Prefect
   - Old: Cron-based scheduling in `loader_pipe.py` (8:30 AM daily)
   - New: Prefect flows with flexible scheduling via Prefect UI

4. **Access Management**: Use centralized Accesses table
   - Old: Hardcoded credentials in `.env` file
   - New: Tokens managed via admin bot or programmatically through AsyncVkDatabase

## Troubleshooting

### No VK tokens found

**Error**: "No VK tokens found in Accesses; nothing to upload."

**Solution**: Add tokens via admin bot or programmatically:
```python
db = AsyncVkDatabase()
await db.init_db()
await db.add_agency_token("12345678", "your_token", "my_agency")
```

### VK credentials not configured

**Error**: "VK_CLIENT_ID and VK_CLIENT_SECRET must be set"

**Solution**: Set environment variables:
```bash
export VK_CLIENT_ID="your_id"
export VK_CLIENT_SECRET="your_secret"
```

### API authentication errors

**Error**: HTTP 401 or 403

**Solutions**:
- Verify VK_CLIENT_ID and VK_CLIENT_SECRET are correct
- Check if API token has expired (tokens refresh automatically for agency)
- Ensure API application has required permissions

### No statistics data

**Possible causes**:
- No active campaigns in the date range
- No ad groups configured
- Client has no spending/activity

**Debug**:
```python
# Check ad plans
api_client = VkApiClient(client_id, client_secret)
plans = await api_client.get_ad_plans(token)
print(f"Found {len(plans)} ad plans")
```

## Performance

- **Concurrent requests**: Limited by semaphore (default: 5)
- **Batch size**: 100 ad groups per API request
- **Retry logic**: Exponential backoff for transient errors
- **Database writes**: Bulk inserts for efficiency

## Security

- **Tokens**: Stored encrypted in ClickHouse Accesses table
- **API credentials**: Environment variables only (never hardcoded)
- **Access control**: Managed via admin bot with user permissions

## Development

### Adding New Metrics

1. Add column names to `VK_REQUIRED_COLUMNS` in `config.py`
2. Add to appropriate type list (`VK_INT_COLUMNS`, `VK_FLOAT_COLUMNS`, etc.)
3. Update this README's Data Schema section

### Testing

```python
# Test API client
api_client = VkApiClient(client_id, client_secret)
token = await api_client.get_access_token()
assert token

# Test database
db = AsyncVkDatabase()
await db.init_db()
exists = await db.table_exists("test_table")

# Test data upload
result = await upload_vk_data_for_user(
    "12345678",
    "token",
    "2025-12-01",
    "2025-12-10"
)
assert result["success"]
```

## Support

For issues or questions:
1. Check this README
2. Review logs for error messages
3. Check Prefect UI for task execution details
4. Verify ClickHouse connectivity and permissions

## License

Same as the main project.
