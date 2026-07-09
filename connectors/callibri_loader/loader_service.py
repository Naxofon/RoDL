import asyncio
import ast

from datetime import datetime, timedelta, time
from typing import Optional

import aiohttp
import pandas as pd
import numpy as np
import requests
import json

from prefect import get_run_logger
from prefect_loader.orchestration.clickhouse_utils import (
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_PASSWORD,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_DB_CALLIBRI,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    ClickhouseDatabase,
)

pd.set_option('future.no_silent_downcasting', True)



async def get_static(
    session: aiohttp.ClientSession,
    user_email: str,
    api_token: str,
    site_id: int,
    date_from: str,
    date_to: str
) -> pd.DataFrame:
    logger = get_run_logger()

    date_from = date_from.replace('-', '.')
    date_to = date_to.replace('-', '.')

    date_from = datetime.strptime(date_from, '%d.%m.%Y')
    date_to = datetime.strptime(date_to, '%d.%m.%Y')

    date_interval = []
    cur_start = date_from

    while cur_start <= date_to:
        current_end = cur_start + timedelta(days=6)

        if current_end > date_to:
            current_end = date_to

        date_interval.append(
            (
                cur_start.strftime('%d.%m.%Y'),
                current_end.strftime('%d.%m.%Y')
            )
        )

        cur_start = current_end + timedelta(days=1)

    res_df = []
    url_metod_stat = 'https://api.callibri.ru/site_get_statistics'

    needed_cols = [
        'id',
        'date',
        'source',
        'is_lid',
        'region',
        'name_type',
        'traffic_type',
        'landing_page',
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_content',
        'utm_term',
        'conversations_number',
        'device',
        'status',
        'accurately',
        'ym_uid',
        'duration',
        'billsec',
        'site_referrer',
        'clbvid',
        'metrika_client_id'
    ]

    for i, (start, end) in enumerate(date_interval):
        params_stat = {
            'user_email': user_email,
            'user_token': api_token,
            'site_id': site_id,
            'date1': start,
            'date2': end
        }

        logger.info(f"Requesting site_id={site_id}, period {start} - {end}")

        data = None
        for attempt in range(5):
            async with session.get(url_metod_stat, params=params_stat) as resp:
                if resp.status == 429:
                    body = await resp.text()
                    wait_time = 5 * (attempt + 1)
                    logger.warning(
                        f"429 для периода {start} - {end}. "
                        f"Попытка {attempt + 1}/5. Повтор через {wait_time} сек. Ответ: {body}"
                    )
                    await asyncio.sleep(wait_time)
                    continue

                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Ошибка запроса: {resp.status} для периода {start} - {end}")
                    logger.warning(body)
                    break

                if resp.status == 401:
                    body = await resp.text()
                    logger.error(f"401 Неверные данные для site_id={site_id}. Ответ: {body}")
                    break

                data = await resp.json(content_type=None)
                break

        if data is None:
            raise RuntimeError(f"Не удалось получить данные за период {start} - {end}")

        calls = []

        if isinstance(data, dict):
            channels_statistics = data.get('channels_statistics', [])

            if isinstance(channels_statistics, list):
                for channel in channels_statistics:
                    if isinstance(channel, dict):
                        channel_calls = channel.get('calls', []) or []
                        if isinstance(channel_calls, list):
                            calls.extend(channel_calls)

        logger.info(f"Period {start} - {end}: calls returned = {len(calls)}")

        if calls:
            stat = pd.json_normalize(calls)
            stat = stat.reindex(columns=needed_cols)
            res_df.append(stat)

        if i < len(date_interval) - 1:
            await asyncio.sleep(2)

    if res_df:
        result = pd.concat(res_df, ignore_index=True)
        logger.info(f"Total rows loaded from API for site_id={site_id}: {len(result)}")
        return result

    return pd.DataFrame(columns=needed_cols)


def prepare_callibri_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    needed_cols = [
        'id',
        'date',
        'source',
        'is_lid',
        'region',
        'name_type',
        'traffic_type',
        'landing_page',
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_content',
        'utm_term',
        'conversations_number',
        'device',
        'status',
        'accurately',
        'ym_uid',
        'duration',
        'billsec',
        'site_referrer',
        'clbvid',
        'metrika_client_id'
    ]

    df = df.reindex(columns=needed_cols)

    dtype_map = {
        'id': 'Int64',
        'source': 'string',
        'is_lid': 'boolean',
        'region': 'string',
        'name_type': 'string',
        'traffic_type': 'string',
        'landing_page': 'string',
        'utm_source': 'string',
        'utm_medium': 'string',
        'utm_campaign': 'string',
        'utm_content': 'string',
        'utm_term': 'string',
        'conversations_number': 'Int64',
        'device': 'string',
        'status': 'string',
        'accurately': 'boolean',
        'ym_uid': 'string',
        'duration': 'Int64',
        'billsec': 'Int64',
        'site_referrer': 'string',
        'clbvid': 'string',
        'metrika_client_id': 'string',
    }

    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')

    for col, dtype in dtype_map.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    df = df.where(pd.notna(df), None)

    return df


async def process_data( user_email:str,
                        api_token: str,
                        site_id: int,
                        tdelta: int = 10,
                        db: Optional[ClickhouseDatabase] = None):
    """
    Process Callibri data for a single client.
    """
    logger = get_run_logger()

    today = datetime.today().date()
    date_to_day = today - timedelta(days=1)
    date_to = datetime.combine(date_to_day, time(23, 59, 59))

    date_from_day = date_to_day - timedelta(days=tdelta)
    date_from = datetime.combine(date_from_day, time.min)

    logger.info(f"Processing site_id={site_id} from {date_from.date()} to {date_to.date()}")

    logger.info(
        f"BEFORE get_static: site_id={site_id!r}, "
        f"user_email={user_email!r}, "
        f"type(site_id)={type(site_id).__name__}"
    )

    async with aiohttp.ClientSession() as session:
        df_calls = await get_static(
            session,
            user_email,
            api_token,
            site_id,
            date_from.strftime("%d.%m.%Y"),
            date_to.strftime("%d.%m.%Y")
        )



    _frames = [df for df in [df_calls] if not df.empty]
    combined_df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()


    write_table_name = f'callibri_{site_id}'

    if db is None:
        db = ClickhouseDatabase(database=CLICKHOUSE_DB_CALLIBRI, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

    try:
        await db.delete_between_dates(write_table_name, date_from, date_to, date_column='date')
        logger.info(f"Deleted existing data for {write_table_name} from {date_from.date()} to {date_to.date()}")
    except Exception as e:
        logger.warning(f"Failed to delete records for {write_table_name}: {e}")

    if not combined_df.empty:
        combined_df = prepare_callibri_df(combined_df)

        logger.info(
            f"Ready to write {len(combined_df)} rows to {write_table_name}. "
            f"Date range in df: {combined_df['date'].min()} - {combined_df['date'].max()}"
        )

        unique_days = (
            combined_df['date']
            .dt.strftime('%Y-%m-%d')
            .dropna()
            .sort_values()
            .unique()
            .tolist()
        )
        logger.info(f"Days to write for {write_table_name}: {unique_days}")

    try:
        await db.write_dataframe(write_table_name, combined_df, order=['date'])
        logger.info(f"Successfully wrote {len(combined_df)} rows to {write_table_name}")
    except Exception as e:
        logger.error(f"Failed to write records for {write_table_name}: {e}")
        raise


async def process_single_client(site_id: int, tdelta: int = 10, api_token: Optional[str] = None):
    """
    Process a single Callibri client.

    Args:
        site_id: Callibri site ID
        tdelta: Number of days to look back
        api_token: Optional API token (if None, fetches from Accesses)
    """

    logger = get_run_logger()

    if api_token is None:
        access_db = ClickhouseDatabase(database=CLICKHOUSE_ACCESS_DATABASE, user=CLICKHOUSE_ACCESS_USER, password=CLICKHOUSE_ACCESS_PASSWORD)

        rows = await access_db.fetch_access_rows(service_type='callibri')
        site_row = None
        for row in rows:
            if row.get('login') == str(site_id):
                site_row = row
                break

        if not site_row:
            raise ValueError(f"Site ID {site_id} not found in Accesses table")

        user_email = str(site_row.get('account') or site_row.get('container') or '')

        api_token = site_row.get('token')
        if not api_token:
            raise ValueError(f"No token found for site_id {site_id}")

    await process_data(user_email, api_token, site_id, tdelta)
    logger.info(f"Completed processing for site_id={site_id}")


async def process_all_clients(tdelta: int = 10):
    """Process all Callibri clients configured in the Accesses table."""
    logger = get_run_logger()

    access_db = ClickhouseDatabase(database=CLICKHOUSE_ACCESS_DATABASE, user=CLICKHOUSE_ACCESS_USER, password=CLICKHOUSE_ACCESS_PASSWORD)

    rows = await access_db.fetch_access_rows(service_type='callibri')

    if not rows:
        logger.warning("No Callibri clients found in Accesses table")
        return

    logger.info(f"Found {len(rows)} Callibri clients to process")

    semaphore = asyncio.Semaphore(5)

    async def process_with_semaphore(row):
        async with semaphore:
            site_id = row.get('login')
            token = row.get('token')
            user_email = row.get('account') or row.get('container') or ''

            if not site_id or not token:
                logger.warning(f"Skipping invalid row: {row}")
                return

            try:
                site_id_int = int(site_id)
                await process_data(user_email, token, site_id_int, tdelta)
            except Exception as e:
                logger.error(f"Error processing site_id={site_id}: {e}")

    tasks = [process_with_semaphore(row) for row in rows]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Completed processing all CallCallibritouch clients")