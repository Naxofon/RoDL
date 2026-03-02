import asyncio
import ast

from datetime import datetime, timedelta, time
from typing import Optional

import aiohttp
import pandas as pd
import numpy as np

from prefect import get_run_logger
from prefect_loader.orchestration.clickhouse_utils import (
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_PASSWORD,
    CLICKHOUSE_ACCESS_USER,
    CLICKHOUSE_DB_CALLTOUCH,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    ClickhouseDatabase,
)

pd.set_option('future.no_silent_downcasting', True)


def _normalize_query_params(params: dict) -> dict:
    """
    Normalize query parameters for aiohttp/yarl.
    Drops None values, casts bools to ints, and casts other non-primitive objects to strings.
    """
    cleaned = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            cleaned[key] = int(value)
        elif isinstance(value, (str, int, float)):
            cleaned[key] = value
        else:
            cleaned[key] = str(value)
    return cleaned


async def download_call_data(
    session: aiohttp.ClientSession,
    api_token: str,
    site_id: int,
    date_from: str,
    date_to: str,
    attribution: Optional[str] = None,
    page: int = 1,
    limit: int = 1000,
    with_call_tags: bool = True,
    with_map_visits: bool = False,
    with_comments: bool = True
) -> pd.DataFrame:
    """
    Download call data from Calltouch API with pagination.

    Args:
        session: aiohttp session
        api_token: Calltouch API token
        site_id: Site identifier
        date_from: Start date in DD/MM/YYYY format
        date_to: End date in DD/MM/YYYY format

    Returns:
        DataFrame with call data
    """
    url = f"https://api.calltouch.ru/calls-service/RestAPI/{site_id}/calls-diary/calls"

    params = {
        "clientApiId": api_token,
        "dateFrom": date_from,
        "dateTo": date_to,
        "page": page,
        "limit": limit,
        "attribution": attribution,
        "withCallTags": with_call_tags,
        "withMapVisits": with_map_visits,
        'withComments': with_comments,
        'timeFrom': '00:00:00',
        'timeTo': '23:59:59'
    }

    params = _normalize_query_params(params)

    all_calls_data = []

    while True:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            all_calls_data.extend(data.get("records", []))

            if data["page"] >= data["pageTotal"]:
                break
            else:
                page += 1
                params["page"] = page

    df = pd.json_normalize(all_calls_data)

    if df.empty:
        df = pd.DataFrame(columns=['date', 'callTags', 'duration', 'successful', 'keyword', 'comments'])
    else:
        df['date'] = pd.to_datetime(df['date'], format="%d/%m/%Y %H:%M:%S")
        df = df.sort_values('date', ascending=True)

        df['dt'] = df['date'].dt.date
        df['dt'] = pd.to_datetime(df['dt'])

        timestamp_to = pd.to_datetime(date_to, format='%d/%m/%Y')
        timestamp_from = pd.to_datetime(date_from, format='%d/%m/%Y')

        df = df[(df['dt'] >= timestamp_from) & (df['dt'] <= timestamp_to)]
        df = df.drop(columns=['dt'])

    return df


async def download_lead_data(
    session: aiohttp.ClientSession,
    api_token: str,
    site_id: int,
    date_from: str,
    date_to: str
) -> pd.DataFrame:
    """
    Download lead/request data from Calltouch API.

    Args:
        session: aiohttp session
        api_token: Calltouch API token
        site_id: Site identifier
        date_from: Start date in MM/DD/YYYY format
        date_to: End date in MM/DD/YYYY format

    Returns:
        DataFrame with lead data
    """
    url = "https://api.calltouch.ru/calls-service/RestAPI/requests"

    params = {
        "clientApiId": api_token,
        "siteId": site_id,
        "dateFrom": date_from,
        "dateTo": date_to,
        "withRequestTags": True
    }

    params = _normalize_query_params(params)

    async with session.get(url, params=params) as response:
        response.raise_for_status()
        data = await response.json()

        df = pd.json_normalize(data)
        if df.empty:
            df = pd.DataFrame(columns=['dateStr', 'RequestTags', 'comments'])
        else:
            df['dateStr'] = pd.to_datetime(df['dateStr'], format="%d/%m/%Y %H:%M:%S")
            df['dt'] = df['dateStr'].dt.date
            df['dt'] = pd.to_datetime(df['dt'])

            timestamp_to = pd.to_datetime(date_to, format='%m/%d/%Y')
            timestamp_from = pd.to_datetime(date_from, format='%m/%d/%Y')

            df = df[(df['dt'] >= timestamp_from) & (df['dt'] <= timestamp_to)]
            df = df.drop(columns=['dt'])

        return df


def extract_comment(comments_str) -> list:
    """Extract comment text from comments field."""
    try:
        if isinstance(comments_str, str):
            comments = ast.literal_eval(comments_str)
        else:
            comments = comments_str

        if isinstance(comments, list) and comments:
            return [item.get('comment', '') for item in comments]
        return []
    except Exception:
        return []


def extract_tags_info(tags_str) -> dict:
    """Extract category, type, and names from tags field."""
    try:
        if isinstance(tags_str, str):
            tags = ast.literal_eval(tags_str)
        else:
            tags = tags_str

        result = {
            'category': [],
            'type': [],
            'names': []
        }

        if isinstance(tags, list) and tags:
            for tag in tags:
                if 'category' in tag:
                    result['category'].append(tag['category'])
                if 'type' in tag:
                    result['type'].append(tag['type'])
                if 'names' in tag:
                    result['names'].extend(tag['names'])

        result['category'] = ', '.join(filter(None, result['category']))
        result['type'] = ', '.join(filter(None, result['type']))
        result['names'] = ', '.join(filter(None, result['names']))

        return result
    except Exception:
        return {'category': '', 'type': '', 'names': ''}


async def process_data(site_id: int, api_token: str, tdelta: int = 10, db: Optional[ClickhouseDatabase] = None):
    """
    Process Calltouch data for a single client.

    Args:
        site_id: Calltouch site ID
        api_token: API token for authentication
        tdelta: Number of days to look back from yesterday
        db: ClickhouseDatabase instance (if None, creates one)
    """
    logger = get_run_logger()

    today = datetime.today().date()
    date_to_day = today - timedelta(days=1)
    date_to = datetime.combine(date_to_day, time(23, 59, 59))

    date_from_day = date_to_day - timedelta(days=tdelta)
    date_from = datetime.combine(date_from_day, time.min)

    logger.info(f"Processing site_id={site_id} from {date_from.date()} to {date_to.date()}")

    async with aiohttp.ClientSession() as session:
        df_calls = await download_call_data(
            session, api_token, site_id,
            date_from.strftime("%d/%m/%Y"), date_to.strftime("%d/%m/%Y")
        )
        df_lead = await download_lead_data(
            session, api_token, site_id,
            date_from.strftime("%m/%d/%Y"), date_to.strftime("%m/%d/%Y")
        )

    calls_drop_columns = [
        'redirectNumber', 'callphase', 'callerNumber',
        'redirectNumber', 'phoneNumber', 'customFields',
        'ctGlobalId', 'phonesInText', 'dcm', 'callbackInfo', 'siteId',
        'yandexDirect', 'googleAdWords', 'timestamp', 'mapVisits',
        'attrs', 'ip', 'callReferenceId', 'orders', 'order', 'sipCallId',
        'userAgent', 'poolType', 'ctCallerId', 'callUrl', 'utmSource', 'utmMedium',
        'phrases', 'callClientUniqueId', 'subPoolName', 'clientId', 'ctClientId',
        'statusDetails', 'sessionDate', 'siteName', 'hostname', 'waitingConnect'
    ]
    calls_rename_columns = {
        'callTags': 'tags',
        'duration': 'CallDuration',
        'successful': 'CallSuccessful',
        'keyword': 'keywords'
    }

    if not df_calls.empty:
        df_calls = df_calls.drop(columns=[col for col in calls_drop_columns if col in df_calls.columns])
        df_calls = df_calls.rename(columns=calls_rename_columns)
    else:
        expected_calls_columns = ['date', 'tags', 'CallDuration', 'CallSuccessful', 'keywords', 'comments']
        df_calls = pd.DataFrame(columns=expected_calls_columns)

    lead_drop_columns = [
        'date', 'siteId', 'mapVisits', 'dcm',
        'ctGlobalId', 'widgetInfo', 'customFields',
        'client.fio', 'client.phones', 'client.contacts',
        'session.ip', 'session.attrs', 'yandexDirect',
        'googleAdWords', 'order', 'requestUrl', 'ctClientId',
        'orders', 'status', 'session.utmSource', 'session.utmMedium',
        'session.sessionDate', 'session.guaClientId', 'session.browser',
        'session.ctGlobalId', 'client.clientId', 'hostname', 'requestNumber', 'requestType'
    ]
    lead_rename_columns = {
        'dateStr': 'date',
        'RequestTags': 'tags',
        'session.sessionId': 'sessionId',
        'session.keywords': 'keywords',
        'session.city': 'city',
        'session.source': 'source',
        'session.medium': 'medium',
        'session.ref': 'ref',
        'session.url': 'url',
        'session.utmTerm': 'utmTerm',
        'session.utmContent': 'utmContent',
        'session.utmCampaign': 'utmCampaign',
        'session.attribution': 'attribution',
        'session.additionalTags': 'additionalTags',
        'session.yaClientId': 'yaClientId',
        'session.device': 'device',
        'session.os': 'os',
        'session.browserName': 'browser'
    }

    if not df_lead.empty:
        df_lead = df_lead.drop(columns=[col for col in lead_drop_columns if col in df_lead.columns])
        df_lead = df_lead.rename(columns=lead_rename_columns)
    else:
        expected_lead_columns = [
            'date', 'tags', 'comments', 'sessionId', 'keywords',
            'city', 'source', 'medium', 'ref', 'url', 'utmTerm',
            'utmContent', 'utmCampaign', 'attribution', 'additionalTags', 'yaClientId',
            'device', 'os', 'browser'
        ]
        df_lead = pd.DataFrame(columns=expected_lead_columns)

    _frames = [df for df in [df_lead, df_calls] if not df.empty]
    combined_df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

    required_columns = [
        'date', 'comments', 'tags', 'manager', 'attribution',
        'uniqTargetRequest', 'uniqueRequest', 'targetRequest',
        'uniqueCall', 'targetCall', 'uniqTargetCall', 'callbackCall',
        'CallSuccessful', 'CallDuration', 'subject',
        'requestId', 'sessionId', 'callId', 'yaClientId',
        'city', 'browser', 'device', 'os',
        'keywords', 'ref', 'url',
        'source', 'medium', 'utmTerm', 'utmContent', 'utmCampaign'
    ]

    for col in required_columns:
        if col not in combined_df.columns:
            combined_df[col] = np.nan

    combined_df['extracted_comments'] = combined_df['comments'].apply(extract_comment)
    combined_df['comment_text'] = combined_df['extracted_comments'].apply(lambda x: x[0] if x else '')

    tags_info = combined_df['tags'].apply(extract_tags_info)
    combined_df['tag_category'] = tags_info.apply(lambda x: x['category'])
    combined_df['tag_type'] = tags_info.apply(lambda x: x['type'])
    combined_df['tag_names'] = tags_info.apply(lambda x: x['names'])

    combined_df = combined_df[[
        'date', 'tag_category', 'tag_type', 'tag_names', 'additionalTags', 'comment_text', 'manager', 'attribution',
        'uniqTargetRequest', 'uniqueRequest', 'targetRequest',
        'uniqueCall', 'targetCall', 'uniqTargetCall', 'callbackCall',
        'CallSuccessful', 'CallDuration', 'subject',
        'requestId', 'sessionId', 'callId', 'yaClientId',
        'city', 'browser', 'device', 'os',
        'keywords', 'ref', 'url',
        'source', 'medium', 'utmTerm', 'utmContent', 'utmCampaign'
    ]]

    combined_df = combined_df.rename(columns={'date': 'Date'})

    combined_df['domain'] = combined_df['url'].str.extract(r'^https?://(?:www\.)?([^/]+)')
    combined_df['domain'] = combined_df['domain'].str.rstrip('/')

    bool_cols = ['uniqTargetRequest', 'targetRequest', 'uniqueRequest', 'uniqueCall', 'targetCall', 'uniqTargetCall', 'callbackCall']
    combined_df[bool_cols] = combined_df[bool_cols].infer_objects(copy=False).fillna(False).astype('bool')
    combined_df['CallSuccessful'] = combined_df['CallSuccessful'].astype('bool')

    replace_values = ["None", "<не заполнено>", "<не указано>", "NaN", "", pd.NA, '(not set)', '(none)']
    combined_df = combined_df.replace(replace_values, np.nan)

    numeric_columns = ['attribution', 'requestId', 'sessionId', 'callId', 'CallDuration']
    for col in numeric_columns:
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').astype('Int64')

    string_columns = [
        'tag_category', 'tag_type', 'tag_names',
        'additionalTags', 'comment_text', 'manager',
        'subject', 'yaClientId', 'city', 'browser', 'device', 'os', 'keywords',
        'ref', 'url', 'source', 'medium', 'utmTerm', 'utmContent', 'utmCampaign', 'domain'
    ]

    for col in string_columns:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].astype(str)
            combined_df[col] = combined_df[col].replace('nan', np.nan)

    write_table_name = f'ct_{site_id}'

    if db is None:
        db = ClickhouseDatabase(database=CLICKHOUSE_DB_CALLTOUCH, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

    try:
        await db.delete_between_dates(write_table_name, date_from, date_to, date_column='Date')
        logger.info(f"Deleted existing data for {write_table_name} from {date_from.date()} to {date_to.date()}")
    except Exception as e:
        logger.warning(f"Failed to delete records for {write_table_name}: {e}")

    try:
        await db.write_dataframe(write_table_name, combined_df, order=['Date'])
        logger.info(f"Successfully wrote {len(combined_df)} rows to {write_table_name}")
    except Exception as e:
        logger.error(f"Failed to write records for {write_table_name}: {e}")
        raise


async def process_single_client(site_id: int, tdelta: int = 10, api_token: Optional[str] = None):
    """
    Process a single Calltouch client.

    Args:
        site_id: Calltouch site ID
        tdelta: Number of days to look back
        api_token: Optional API token (if None, fetches from Accesses)
    """

    logger = get_run_logger()

    if api_token is None:
        access_db = ClickhouseDatabase(database=CLICKHOUSE_ACCESS_DATABASE, user=CLICKHOUSE_ACCESS_USER, password=CLICKHOUSE_ACCESS_PASSWORD)

        rows = await access_db.fetch_access_rows(service_type='calltouch')
        site_row = None
        for row in rows:
            if row.get('login') == str(site_id):
                site_row = row
                break

        if not site_row:
            raise ValueError(f"Site ID {site_id} not found in Accesses table")

        api_token = site_row.get('token')
        if not api_token:
            raise ValueError(f"No token found for site_id {site_id}")

    await process_data(site_id, api_token, tdelta)
    logger.info(f"Completed processing for site_id={site_id}")


async def process_all_clients(tdelta: int = 10):
    """Process all Calltouch clients configured in the Accesses table."""
    logger = get_run_logger()

    access_db = ClickhouseDatabase(database=CLICKHOUSE_ACCESS_DATABASE, user=CLICKHOUSE_ACCESS_USER, password=CLICKHOUSE_ACCESS_PASSWORD)

    rows = await access_db.fetch_access_rows(service_type='calltouch')

    if not rows:
        logger.warning("No Calltouch clients found in Accesses table")
        return

    logger.info(f"Found {len(rows)} Calltouch clients to process")

    semaphore = asyncio.Semaphore(5)

    async def process_with_semaphore(row):
        async with semaphore:
            site_id = row.get('login')
            token = row.get('token')

            if not site_id or not token:
                logger.warning(f"Skipping invalid row: {row}")
                return

            try:
                site_id_int = int(site_id)
                await process_data(site_id_int, token, tdelta)
            except Exception as e:
                logger.error(f"Error processing site_id={site_id}: {e}")

    tasks = [process_with_semaphore(row) for row in rows]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Completed processing all Calltouch clients")