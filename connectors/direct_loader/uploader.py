import asyncio
import io
import json
import random as rnd
import time
from datetime import datetime, timedelta

import aiohttp
import pandas as pd

from prefect_loader.orchestration.clickhouse_utils import AsyncDirectDatabase
from .config import (
    DATABASE_PROFILES,
    DEFAULT_DB_PROFILE,
    DIRECT_TIMEOUT,
    get_report_queue_semaphore,
    global_rate_limiter,
    offline_report_tracker,
)
from .logging_utils import get_logger
from .shared_utils import (
    direct_post_json,
    get_goal_ids_by_client,
    normalize_login_to_api,
    normalize_login_to_db,
    process_conversion_columns,
)


class YaStatUploader:
    """Yandex Direct statistics uploader for fetching and loading report data."""

    def __init__(
        self,
        login,
        token,
        time_to,
        time_from,
        *,
        profile: str = DEFAULT_DB_PROFILE,
    ):
        """Initialize the uploader with client credentials and date range.

        Args:
            login: Client login identifier
            token: Yandex Direct API token
            time_to: End date for data collection
            time_from: Start date for data collection
            profile: Database profile to use (default: DEFAULT_DB_PROFILE)

        Raises:
            ValueError: If profile is not recognized
        """
        self.login = normalize_login_to_db(login)
        self.original_login = login
        self.token = token
        self.time_to = time_to
        self.time_from = time_from
        if profile not in DATABASE_PROFILES:
            raise ValueError(f"Unknown database profile '{profile}'")
        self.profile = profile
        self.profile_config = DATABASE_PROFILES[profile]
        self.db = AsyncDirectDatabase(self.profile_config.get('database'))

    @property
    def api_login(self) -> str:
        return normalize_login_to_api(self.login)

    @property
    def table_name(self) -> str:
        return self.login

    def _prepare_export_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep the DataFrame untouched so conversion columns remain identical
        to the analytics pipeline.
        """
        return df

    async def get_client_list(self):
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"Archived": "NO"},
                "FieldNames": ["Login"],
                "Page": {"Limit": 10000, "Offset": 0}
            }
        }
        clients: list[str] = []
        agency_url = "https://api.direct.yandex.com/json/v5/agencyclients"
        async with aiohttp.ClientSession(timeout=DIRECT_TIMEOUT) as session:
            while True:
                result = await direct_post_json(
                    session,
                    agency_url,
                    body,
                    token=self.token,
                )
                if not isinstance(result, dict) or "result" not in result:
                    get_logger().error(f"Unexpected agencyclients response: {result}")
                    break
                clients.extend(normalize_login_to_db(client["Login"]) for client in result["result"].get("Clients", []))
                limited_by = result["result"].get("LimitedBy")
                if limited_by:
                    body["params"]["Page"]["Offset"] = limited_by
                else:
                    break
        return clients

    async def upload_data(self):
        date_from = datetime.strptime(self.time_from, '%Y-%m-%d')
        date_to = datetime.strptime(self.time_to, '%Y-%m-%d')
        selection_criteria = {
            "DateFrom": date_from.strftime('%Y-%m-%d'),
            "DateTo": date_to.strftime('%Y-%m-%d')
        }
        field_names = list(self.profile_config['field_names'])
        if 'Date' not in field_names:
            field_names.insert(0, 'Date')
        goal_ids = await get_goal_ids_by_client(self.token, self.login)
        df_list = []
        if len(goal_ids) <= 10:
            goal_batches = [goal_ids]
        else:
            goal_batches = [goal_ids[i:i + 10] for i in range(0, len(goal_ids), 10)]
        async with aiohttp.ClientSession(timeout=DIRECT_TIMEOUT) as session:
            headers = {
                "Authorization": "Bearer " + self.token,
                "Client-Login": self.api_login,
                "Accept-Language": "ru",
                "processingMode": "auto",
                "returnMoneyInMicros": "false",
                "skipReportHeader": "true",
                "skipReportSummary": "true",
            }
            report_owner = self.api_login or "agency"
            for goal_batch in goal_batches:
                report_name = f"ACCOUNT {self.login} - {self.time_to} - {self.time_from} - {rnd.randint(1,999999)}"
                params = {
                    "SelectionCriteria": selection_criteria,
                    "FieldNames": field_names,
                    "ReportName": report_name,
                    "ReportType": "CUSTOM_REPORT",
                    "DateRangeType": "CUSTOM_DATE",
                    "Format": "TSV",
                    "IncludeVAT": "YES",
                    "IncludeDiscount": "NO",
                }
                if goal_batch:
                    params["Goals"] = goal_batch
                    params['AttributionModels'] = ['LSC']
                body = json.dumps({"method": "get", "params": params}, indent=4).encode('utf8')
                report_ready = False
                retry_attempts = 0
                max_retries = 6
                split_attempts = 0
                window_started = time.monotonic()

                async def fetch_daily_slices():
                    """Fallback: fetch the same goal batch day by day."""
                    for i in range((date_to - date_from).days + 1):
                        day_start = date_from + timedelta(days=i)
                        day_end = day_start
                        params_day = {
                            "SelectionCriteria": {
                                "DateFrom": day_start.strftime("%Y-%m-%d"),
                                "DateTo": day_end.strftime("%Y-%m-%d"),
                            },
                            "FieldNames": field_names,
                            "ReportName": f"ACCOUNT {self.login} - {day_end:%Y-%m-%d} - {day_start:%Y-%m-%d} - {rnd.randint(1,999999)}",
                            "ReportType": "CUSTOM_REPORT",
                            "DateRangeType": "CUSTOM_DATE",
                            "Format": "TSV",
                            "IncludeVAT": "YES",
                            "IncludeDiscount": "NO",
                        }
                        if goal_batch:
                            params_day["Goals"] = goal_batch
                            params_day["AttributionModels"] = ["LSC"]
                        body_day = json.dumps({"method": "get", "params": params_day}, indent=4).encode("utf8")
                        try:
                            async with get_report_queue_semaphore():
                                async with session.post(
                                    reports_url,
                                    data=body_day,
                                    headers=headers,
                                ) as resp_day:
                                    if resp_day.status == 200:
                                        pda_day = await resp_day.read()
                                        df_day = pd.read_table(
                                            io.StringIO(pda_day.decode("utf-8")),
                                            delimiter="\t",
                                            index_col="Date",
                                            low_memory=False,
                                        )
                                        df_day = df_day.replace("--", 0).infer_objects(copy=False)
                                        df_day.reset_index(inplace=True)
                                        df_day["Date"] = pd.to_datetime(df_day["Date"])
                                        if column_types:
                                            df_day = df_day.astype(column_types)
                                        df_day = process_conversion_columns(df_day)
                                        df_day["Date"] = pd.to_datetime(df_day["Date"])
                                        df_list.append(df_day)
                                    else:
                                        get_logger().warning(
                                            f">>> {self.login}: daily slice {day_start:%Y-%m-%d} "
                                            f"failed with status {resp_day.status}: {await resp_day.text()}"
                                        )
                        except Exception as day_exc:
                            get_logger().warning(
                                f">>> {self.login}: daily slice {day_start:%Y-%m-%d} failed with "
                                f"{type(day_exc).__name__}: {day_exc}"
                            )
                async with offline_report_tracker.reserve(self.token, owner=report_owner):
                    while not report_ready and retry_attempts < max_retries:
                        if (time.monotonic() - window_started) > 600 and (date_to - date_from).days > 0 and split_attempts < 2:
                            split_attempts += 1
                            get_logger().warning(
                                f">>> {self.login}: window exceeded 10m; switching to daily slices "
                                f"(attempt {split_attempts}) for window {self.time_from}..{self.time_to}"
                            )
                            await fetch_daily_slices()
                            report_ready = True
                            break
                        try:
                            waited = await global_rate_limiter.acquire(report_owner)
                            if waited:
                                limit = global_rate_limiter.max_requests
                                window = global_rate_limiter.window
                                get_logger().debug(
                                    "%s: waited %.2fs to respect GLOBAL Reports %d/%ss limit",
                                    self.login,
                                    waited,
                                    limit,
                                    window,
                                )
                            reports_url = "https://api.direct.yandex.com/json/v5/reports"
                            async with get_report_queue_semaphore():
                                async with session.post(reports_url, data=body,
                                                        headers=headers) as response:
                                    if response.status == 400:
                                        txt = await response.text()
                                        try:
                                            err = json.loads(txt)
                                            if err.get("error", {}).get("error_code") == "9000":
                                                get_logger().info(
                                                    f"{self.login}: queue full — "
                                                    "sleep 60 s and retry"
                                                )
                                                await asyncio.sleep(60)
                                                continue
                                        except json.JSONDecodeError:
                                            pass
                                        get_logger().error(">>> Invalid request parameters "
                                                    "or report queue limit reached")
                                        get_logger().error(f">>> Server response JSON:\n{txt}")
                                        report_ready = True
                                        break
                                    elif response.status == 200:
                                        pda = await response.read()
                                        df = pd.read_table(io.StringIO(pda.decode('utf-8')), delimiter='\t', index_col='Date', low_memory=False)
                                        df = df.replace('--', 0).infer_objects(copy=False)
                                        df.reset_index(inplace=True)
                                        df['Date'] = pd.to_datetime(df['Date'])
                                        column_types = {
                                            col: dtype
                                            for col, dtype in self.profile_config['column_types'].items()
                                            if col in df.columns
                                        }
                                        if column_types:
                                            df = df.astype(column_types)
                                        df = process_conversion_columns(df)
                                        df_list.append(df)
                                        report_ready = True
                                        break
                                    elif response.status in (201, 202):
                                        retry_in = 10
                                        await asyncio.sleep(retry_in)
                                        continue
                                    else:
                                        get_logger().error(f">>> Access check required, stats unavailable. Client - {self.login}, code - {response.status}")
                                        get_logger().error(f"Server response JSON: \n{await response.text()}")
                                        if retry_attempts < max_retries:
                                            retry_attempts += 1
                                            delay = min(300, 5 * (2 ** (retry_attempts - 1)))
                                            get_logger().info(
                                                f">>> Retrying... Attempt {retry_attempts}/{max_retries} in {delay}s"
                                            )
                                            await asyncio.sleep(delay)
                                            continue
                                        else:
                                            get_logger().error(f">>> Max retries reached for client {self.login}.")
                                            report_ready = True
                                            break
                        except Exception as e:
                            get_logger().exception(
                                f">>> Error during report request: Client - {self.login}, "
                                f"type={type(e).__name__}, error={e!r}"
                            )
                            is_stream_cut = isinstance(e, aiohttp.ClientPayloadError) and (
                                "transfer length" in str(e).lower()
                                or "Response payload is not completed" in str(e)
                                or "Connection reset" in str(e)
                            )
                            if is_stream_cut and (date_to - date_from).days > 0 and split_attempts < 2:
                                split_attempts += 1
                                get_logger().warning(
                                    f">>> {self.login}: stream cut; switching to daily slices "
                                    f"(attempt {split_attempts}) for window {self.time_from}..{self.time_to}"
                                )
                                await fetch_daily_slices()
                                report_ready = True
                                break

                            if retry_attempts < max_retries:
                                retry_attempts += 1
                                delay = min(300, 5 * (2 ** (retry_attempts - 1)))
                                get_logger().info(
                                    f">>> Retrying... Attempt {retry_attempts}/{max_retries} in {delay}s"
                                )
                                await asyncio.sleep(delay)
                                continue
                            else:
                                get_logger().error(f">>> Max retries reached for client {self.login}.")
                                report_ready = True
        if df_list:
            df = pd.concat(df_list, axis=1, join='outer')
            df = df.loc[:,~df.columns.duplicated()]
            df = df.dropna(subset=['Date'])
            try:
                df = process_conversion_columns(df, add_sum=True)
            except Exception as e:
                get_logger().warning(
                    f"Error processing conversion columns for {self.login}: {e}"
                )
            export_df = self._prepare_export_dataframe(df)
            await self.db.write_dataframe_to_table_fast(
                export_df, self.table_name
            )
            get_logger().info(
                f"{self.login}: successfully wrote {len(export_df)} rows to table '{self.table_name}' "
                f"for period {self.time_from} to {self.time_to}"
            )
        else:
            get_logger().error(f"Error uploading report for {self.login}")

    async def unload_data_by_days(self):
        try:
            date_from = datetime.strptime(self.time_from, '%Y-%m-%d')
            date_to = datetime.strptime(self.time_to, '%Y-%m-%d')
            date_ranges = [(date_from + timedelta(days=i), min(date_from + timedelta(days=i + 4), date_to)) for i in range(0, (date_to - date_from).days + 1, 5)]
            for date_range in date_ranges:
                uploader = YaStatUploader(
                    self.login,
                    self.token,
                    date_range[1].strftime('%Y-%m-%d'),
                    date_range[0].strftime('%Y-%m-%d'),
                    profile=self.profile,
                )
                await uploader.upload_data()
            get_logger().info(
                f"Data successfully uploaded, client {self.login}"
            )
        except Exception as e:
            get_logger().error(f"Error in unload_data_by_days - {e}")
