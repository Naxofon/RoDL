import asyncio
import ast
from datetime import datetime, timedelta
from io import BytesIO

import aiohttp
import numpy as np
import pandas as pd
import requests
from prefect import get_run_logger

from prefect_loader.orchestration.clickhouse_utils import AsyncMetrikaDatabase

from .change_utils import AsyncRequestLimiter, format_auth_fingerprint

class YaMetrikaUploader:
    """Yandex Metrika data uploader for fetching and loading analytics data."""

    def __init__(self, counter, start, end, domain_name, token, login=None):
        """Initialize the uploader with counter configuration.

        Args:
            counter: Metrika counter ID
            start: Start date for data collection
            end: End date for data collection
            domain_name: Domain name associated with the counter
            token: Yandex Metrika API token
        """
        self.counter = counter
        self.start = start
        self.end = end
        self.domain_name = domain_name
        self.token = token
        self.login = login
        self.semaphore = asyncio.Semaphore(3)

    def preprocess_data(self, df):
        """
        Preprocesses the combined DataFrame by cleaning and transforming various columns.

        Parameters:
            df (pd.DataFrame): Combined DataFrame from Yandex Metrika APIs.

        Returns:
            pd.DataFrame: Preprocessed DataFrame.
        """

        df = df.copy()
        df = df.replace({pd.NA: np.nan})

        df.columns = df.columns.str.replace('^ym:s:', '', regex=True)

        device_category_mapping = {
            1: "десктоп",
            2: "мобильные телефоны",
            3: "планшеты",
            4: "TV"
        }

        columns_to_replace = [
            'ageInterval', 'gender', 'regionArea', 'screenFormat'
        ]

        df['goalsDateTime'] = df['goalsDateTime'].apply(
            lambda x: x.replace("\\'", "'") if isinstance(x, str) else x
        )

        df['goalsID'] = df['goalsID'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )
        df['goalsDateTime'] = df['goalsDateTime'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        goal_counts = df['goalsID'].apply(
            lambda goals: (
                {goal: goals.count(goal) for goal in set(goals)}
                if isinstance(goals, list)
                else {}
            )
        )
        df['goal_counts'] = goal_counts

        for idx, row in df.iterrows():
            goal_count_dict = row['goal_counts']

            for goal, count in goal_count_dict.items():
                goal_column = f'goal_{goal}'
                if goal_column not in df.columns:
                    df[goal_column] = 0
                df.at[idx, goal_column] = count

                datetime_column = f'd_goal_{goal}'
                if datetime_column not in df.columns:
                    df[datetime_column] = None

                goals_dt = row.get('goalsDateTime')
                goals_id = row.get('goalsID', [])
                if isinstance(goals_dt, list) and goal in goals_id:
                    try:
                        goal_idx = row['goalsID'].index(goal)
                        first_datetime = row['goalsDateTime'][goal_idx]
                        first_datetime = datetime.strptime(
                            first_datetime, '%Y-%m-%d %H:%M:%S'
                        )
                        df.at[idx, datetime_column] = first_datetime
                    except (ValueError, IndexError, AttributeError):
                        df.at[idx, datetime_column] = None
                else:
                    df.at[idx, datetime_column] = None

        df = df.drop(columns=['goalsID', 'goalsDateTime', 'goal_counts'])

        df["deviceCategory"] = df["deviceCategory"].replace(
            device_category_mapping
        )

        df['interest'] = df['interest'].replace(
            r'^\s*$', np.nan, regex=True
        )

        df[columns_to_replace] = df[columns_to_replace].replace(
            {'undefined': np.nan, 'Не определено': np.nan}
        )

        df['dateTimeUTC'] = pd.to_datetime(df['dateTimeUTC'], errors='coerce')
        df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')

        df['isRobot'] = df['isRobot'].map({
            'Люди': False,
            'Роботы': True
        })
        for col in ['isRobot', 'bounce', 'isNewUser']:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors='coerce')
                    .fillna(0)
                    .astype('uint8')
                )

        df.replace(
            {'nan': np.nan, 'NaN': np.nan, 'None': np.nan}, inplace=True
        )
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.dropna(subset=['visitDuration', 'pageViews'], inplace=True)

        integer_columns = ['userVisitsPeriod']

        for col in integer_columns:
            if col in df.columns:
                df[col] = df[col].fillna(-1)

        for col in integer_columns:
            if col in df.columns:
                df[col] = df[col].astype(int)

        for col in integer_columns:
            if col in df.columns:
                df[col] = df[col].replace(-1, np.nan)

        goal_columns = df.filter(regex='^goal_').columns.tolist()
        df['sum_goal'] = df[goal_columns].sum(axis=1)

        df = df.astype(
            {
                'sum_goal': 'int',
                'visitID': 'str',
                'gender': 'str',
                'interest': 'str',
                'isRobot': 'uint8',
                'regionArea': 'str',
                'userVisits': 'int',
                'ageInterval': 'str',
                'visits': 'int',
                'lastsignSearchPhrase': 'str',
                'lastsignSourceEngine': 'str',
                'clientID': 'str',
                'counterUserIDHash': 'str',
                'lastsignTrafficSource': 'str',
                'lastsignAdvEngine': 'str',
                'lastsignReferalSource': 'str',
                'lastsignSearchEngineRoot': 'str',
                'lastsignSearchEngine': 'str',
                'ipAddress': 'str',
                'bounce': 'uint8',
                'lastsignSocialNetwork': 'str',
                'visitDuration': 'int',
                'screenFormat': 'str',
                'pageViews': 'int',
                'startURL': 'str',
                'endURL': 'str',
                'mobilePhone': 'str',
                'mobilePhoneModel': 'str',
                'operatingSystemRoot': 'str',
                'operatingSystem': 'str',
                'browser': 'str',
                'browserMajorVersion': 'int',
                'isNewUser': 'uint8',
                'regionCountry': 'str',
                'browserLanguage': 'str',
                'lastsignRecommendationSystem': 'str',
                'lastsignMessenger': 'str',
                'regionCity': 'str',
                'deviceCategory': 'str',
                'clientTimeZone': 'int',
                'UTMCampaign': 'str',
                'UTMContent': 'str',
                'UTMMedium': 'str',
                'UTMSource': 'str',
                'UTMTerm': 'str',
                'referer': 'str',
                'parsedParamsKey1': 'str',
                'parsedParamsKey2': 'str',
                'lastsignDirectBannerGroup': 'int',
                'lastsignDirectClickBanner': 'int',
                'lastsignDirectClickOrderName': 'str',
                'lastsignClickBannerGroupName': 'str',
                'lastsignDirectClickBannerName': 'str',
                'lastsignDirectPhraseOrCond': 'str',
                'lastsignDirectPlatformType': 'str',
                'lastsignDirectPlatform': 'str',
                'lastsignDirectConditionType': 'str',
                'offlineCallTalkDuration': 'str',
                'offlineCallHoldDuration': 'str',
                'offlineCallMissed': 'str',
                'offlineCallTag': 'str',
                'offlineCallFirstTimeCaller': 'str',
                'offlineCallURL': 'str',
                'browserCountry': 'str',
                'screenOrientationName': 'str',
                'screenWidth': 'str',
                'screenHeight': 'str',
                'physicalScreenWidth': 'str',
                'physicalScreenHeight': 'str',
                'windowClientWidth': 'str',
                'windowClientHeight': 'str',
                'browserMinorVersion': 'str',
                'browserEngine': 'str',
                'browserEngineVersion1': 'str',
                'browserEngineVersion2': 'str',
                'browserEngineVersion3': 'str',
                'browserEngineVersion4': 'str',
            }
        )

        df = df.replace({'nan': np.nan, 'NaN': np.nan, 'None': np.nan})

        string_columns = [
            'clientID', 'visitID', 'counterUserIDHash', 'gender', 'interest', 'regionArea', 'regionCountry', 'ageInterval', 
            'lastsignTrafficSource', 'lastsignAdvEngine', 'lastsignReferalSource',
            'lastsignSearchEngineRoot', 'lastsignSearchEngine', 'ipAddress', 'lastsignSocialNetwork',
            'screenFormat', 'startURL', 'endURL', 'mobilePhone', 'mobilePhoneModel',
            'operatingSystemRoot', 'operatingSystem', 'browser', 'browserLanguage',
            'lastsignRecommendationSystem', 'lastsignMessenger', 'regionCity',
            'deviceCategory', 'UTMCampaign', 'UTMContent', 'UTMMedium', 'UTMSource',
            'UTMTerm', 'referer', 'parsedParamsKey1', 'parsedParamsKey2', 'lastsignDirectClickOrderName',
            'lastsignClickBannerGroupName', 'lastsignDirectClickBannerName', 'lastsignSearchPhrase', 'lastsignSourceEngine',
            'lastsignDirectPhraseOrCond', 'lastsignDirectPlatformType', 'lastsignDirectPlatform',
            'lastsignDirectConditionType', 'offlineCallTalkDuration', 'offlineCallHoldDuration',
            'offlineCallMissed', 'offlineCallTag', 'offlineCallFirstTimeCaller', 'offlineCallURL', 'screenOrientationName',
            'screenWidth', 'screenHeight', 'physicalScreenWidth', 'physicalScreenHeight', 'windowClientWidth',
            'windowClientHeight', 'browserMinorVersion', 'browserEngine', 'browserEngineVersion1',
            'browserEngineVersion2', 'browserEngineVersion3', 'browserEngineVersion4'
        ]

        bool_columns = [
            'isRobot', 'bounce', 'isNewUser'
        ]

        date_time_columns = df.filter(regex='^d_').columns.tolist()

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype('string')

        for col in bool_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('uint8')

        for col in date_time_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].where(df[col].notnull(), None)

        return df


    async def load_metrika(self, counter_id, token, start_date, end_date):
        """
        Downloads data from Yandex Metrika Logs API and Reporting API concurrently using asynchronous programming,
        merges the data on 'ym:s:visitID', and returns the combined DataFrame.

        Parameters:
            counter_id (str): Counter ID.
            token (str): OAuth token.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: Combined DataFrame containing data from both APIs.
        """

        logger = get_run_logger()
        logger.info(
            "%s: Metrika API upload fetch start for counter=%s, range=%s-%s (%s)",
            self.domain_name,
            counter_id,
            start_date,
            end_date,
            format_auth_fingerprint(self.login, token),
        )

        df_logs = None
        df_report = None

        def _goal_modification(df, metrika_id, token):

            def __get_goal_ids__(counter_id, token):
                url = f"https://api-metrika.yandex.net/management/v1/counter/{int(counter_id)}/goals"
                headers = {
                    "Authorization": f"OAuth {token}"
                }

                params = {
                    "useDeleted": "0"
                }

                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    goals = response.json().get('goals', [])
                    if goals:
                        df = pd.json_normalize(goals)
                        if 'id' in df.columns and 'name' in df.columns:
                            df = df[['id', 'name']]
                            return df
                        else:
                            raise ValueError("Expected columns 'id' and 'name' not found in goals data.")
                    else:
                        raise ValueError("No goals found for the given counter.")
                else:
                    response.raise_for_status()

            def __rename_goal_columns__(column_name):
                if column_name.startswith('goal_'):
                    try:
                        id_part = int(column_name.split('_')[1])
                        identifier = id_to_identifier.get(id_part, None)
                        if identifier:
                            return f"{identifier}_{column_name}"
                        else:
                            logger.info(f"No identifier found for goal ID {id_part}. Keeping original column name.")
                            return column_name
                    except (IndexError, ValueError):
                        logger.warning(f"Invalid goal column format: {column_name}. Keeping original column name.")
                        return column_name
                else:
                    return column_name

            def __count_uniquique_goal__(d):
                try:
                    goal_columns = d.filter(regex='_goal_').columns.tolist()
                    goal_columns = [
                        record
                        for record in goal_columns
                        if not record.startswith('d_')
                    ]

                    for col in goal_columns:
                        if pd.api.types.is_numeric_dtype(d[col]):
                            goal_id = col.split('_', 1)[1]
                            u_col = f'u_{goal_id}'
                            d[u_col] = (d[col] > 0).astype(int)
                except Exception:
                    pass

                return d

            def __add_missing_goal_columns__(df, goal_column_list):
                """
                Ensures that all goal columns from the goal_column list are present in the DataFrame.
                If a goal column is missing, it adds an empty column with the corresponding goal name.

                Parameters:
                    df (pd.DataFrame): The DataFrame that is being processed.
                    goal_column_list (list): A list of goal columns (goal_column) that should exist in the DataFrame.

                Returns:
                    pd.DataFrame: The DataFrame with all required goal columns (including empty ones for missing goals).
                """

                for goal_column in goal_column_list:
                    if goal_column not in df.columns:
                        df[goal_column] = 0

                return df

            goals_df = __get_goal_ids__(metrika_id, token)

            goals_df['name'] = goals_df['name'].fillna("").astype(str).str.lower()
            goals_df["id"] = pd.to_numeric(goals_df["id"], errors="coerce")
            goals_df = goals_df.dropna(subset=["id"])
            goals_df["id"] = goals_df["id"].astype(int)

            add_condition = (
                goals_df['name'].str.contains('madd')
            )

            goals_df.loc[add_condition, 'identifier'] = 'g'
            goals_df["identifier"] = goals_df["identifier"].fillna("").astype(str)
            goal_prefix_map = {int(row.id): ("g" if str(row.identifier) == "g" else "u") for row in goals_df.itertuples(index=False)}

            g_goal_ids = {gid for gid, prefix in goal_prefix_map.items() if prefix == "g"}
            id_to_identifier = goals_df.set_index('id')['identifier'].to_dict()

            df_original = df.copy()
            raw_goal_values: dict[int, pd.Series] = {}
            for goal_id in goal_prefix_map.keys():
                raw_col = f"goal_{goal_id}"
                if raw_col in df_original.columns:
                    raw_goal_values[goal_id] = pd.to_numeric(df_original[raw_col], errors="coerce").fillna(0)
                else:
                    raw_goal_values[goal_id] = pd.Series(0, index=df_original.index, dtype="int64")

            df_renamed = df.copy()

            for goal_id, raw_series in raw_goal_values.items():
                u_col = f"u_goal_{goal_id}"
                df_renamed[u_col] = (raw_series > 0).astype(int)
                if goal_id in g_goal_ids:
                    g_col = f"g_goal_{goal_id}"
                    df_renamed[g_col] = (raw_series > 0).astype(int)

            stale_g_cols = []
            for col in df_renamed.columns:
                if not col.startswith("g_goal_"):
                    continue
                try:
                    gid = int(col.split("_", 2)[2])
                except (IndexError, ValueError):
                    continue
                if gid not in g_goal_ids:
                    stale_g_cols.append(col)
            if stale_g_cols:
                df_renamed = df_renamed.drop(columns=stale_g_cols)

            goal_cols_prefixed = df_renamed.filter(regex="^(u_|g_)goal_\\d+$").columns.tolist()
            for col in goal_cols_prefixed:
                df_renamed[col] = pd.to_numeric(df_renamed[col], errors="coerce").fillna(0)
                df_renamed[col] = (df_renamed[col] > 0).astype(int)

            g_goal_columns = [c for c in goal_cols_prefixed if c.startswith("g_")]
            u_goal_columns = [c for c in goal_cols_prefixed if c.startswith("u_")]

            df_renamed['g_sum_goal'] = df_renamed[g_goal_columns].sum(axis=1) if g_goal_columns else 0
            df_renamed['u_sum_goal'] = df_renamed[u_goal_columns].sum(axis=1) if u_goal_columns else 0

            columns_to_convert = ['g_sum_goal', 'u_sum_goal']
            df_renamed[columns_to_convert] = df_renamed[columns_to_convert].astype(int)

            legacy_cols = [
                col for col in df_renamed.columns
                if col.startswith("goal_") or col == "sum_goal"
            ]
            if legacy_cols:
                df_renamed = df_renamed.drop(columns=legacy_cols, errors="ignore")

            df_renamed.attrs["goal_prefix_map"] = goal_prefix_map

            df_renamed.columns = df_renamed.columns.map(str)

            return df_renamed

        async def _download_metrica_logs():
            nonlocal df_logs
            """
            Downloads logs from the Yandex Metrika Logs API and stores the DataFrame in df_logs.
            """

            max_create_retries = 3
            max_status_retries = 3
            max_download_retries = 3

            fields_list = [
                'ym:s:dateTimeUTC', 'ym:s:dateTime',
                'ym:s:goalsID', 'ym:s:goalsDateTime',
                'ym:s:visitID', 'ym:s:clientID', 'ym:s:counterUserIDHash',
                'ym:s:lastsignTrafficSource','ym:s:lastsignAdvEngine',
                'ym:s:lastsignReferalSource','ym:s:lastsignSearchEngineRoot',
                'ym:s:lastsignSearchEngine', 'ym:s:ipAddress', 'ym:s:bounce',
                'ym:s:lastsignSocialNetwork', 'ym:s:visitDuration', 'ym:s:screenFormat',
                'ym:s:pageViews', 'ym:s:startURL', 'ym:s:endURL', 'ym:s:mobilePhone', 'ym:s:mobilePhoneModel',
                'ym:s:operatingSystemRoot', 'ym:s:operatingSystem', 'ym:s:browser', 'ym:s:browserMajorVersion',
                'ym:s:isNewUser', 'ym:s:regionCountry', 'ym:s:browserLanguage', 'ym:s:lastsignRecommendationSystem', 'ym:s:lastsignMessenger',
                'ym:s:regionCity', 'ym:s:deviceCategory', 'ym:s:clientTimeZone',
                'ym:s:UTMCampaign', 'ym:s:UTMContent', 'ym:s:UTMMedium',
                'ym:s:UTMSource', 'ym:s:UTMTerm', 'ym:s:referer', 'ym:s:parsedParamsKey1', 'ym:s:parsedParamsKey2',
                'ym:s:lastsignDirectClickOrder', 'ym:s:lastsignDirectBannerGroup',
                'ym:s:lastsignDirectClickBanner', 'ym:s:lastsignDirectClickOrderName',
                'ym:s:lastsignClickBannerGroupName', 'ym:s:lastsignDirectClickBannerName',
                'ym:s:lastsignDirectPhraseOrCond', 'ym:s:lastsignDirectPlatformType',
                'ym:s:lastsignDirectPlatform', 'ym:s:lastsignDirectConditionType',
                'ym:s:offlineCallTalkDuration', 'ym:s:offlineCallHoldDuration', 'ym:s:offlineCallMissed',
                'ym:s:offlineCallTag', 'ym:s:offlineCallFirstTimeCaller', 'ym:s:offlineCallURL', 'ym:s:screenOrientationName',
                'ym:s:screenWidth', 'ym:s:screenHeight', 'ym:s:physicalScreenWidth', 'ym:s:physicalScreenHeight', 'ym:s:windowClientWidth',
                'ym:s:windowClientHeight', 'ym:s:browserMinorVersion', 'ym:s:browserEngine', 'ym:s:browserEngineVersion1',
                'ym:s:browserEngineVersion2', 'ym:s:browserEngineVersion3', 'ym:s:browserEngineVersion4', 'ym:s:browserCountry'
            ]

            headers = {'Authorization': f'OAuth {token}'}
            fields = ','.join(fields_list)

            session = aiohttp.ClientSession(headers=headers)

            try:
                create_retries = 0
                while create_retries < max_create_retries:
                    try:
                        create_url = f'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequests'
                        params = {
                            'date1': start_date,
                            'date2': end_date,
                            'fields': fields,
                            'source': 'visits'
                        }
                        async with session.post(create_url, params=params) as response:
                            response.raise_for_status()
                            resp_json = await response.json()
                            log_id = resp_json['log_request']['request_id']
                            logger.debug(f'Log id is {log_id}')
                            break
                    except aiohttp.ClientResponseError as e:
                        create_retries += 1
                        if create_retries >= max_create_retries:
                            logger.error(f'Max retries reached for creating log request. Giving up.')
                            return
                        logger.error(f'Error creating log request (HTTP {e.status}): {e}. Retrying ({create_retries}/{max_create_retries}) after 5 seconds...')
                        await asyncio.sleep(5)
                    except KeyError:
                        create_retries += 1
                        if create_retries >= max_create_retries:
                            logger.error(f'Max retries reached for creating log request. Giving up.')
                            return
                        else:
                            logger.error('Unexpected response format when creating log request.')
                            logger.error(f'Retrying creation ({create_retries}/{max_create_retries}) after 5 seconds...')
                            await asyncio.sleep(5)

                if not locals().get('log_id'):
                    return

                status_retries = 0
                while True:
                    try:
                        status_url = f'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequest/{log_id}'
                        async with session.get(status_url) as response:
                            response.raise_for_status()
                            status_data = await response.json()
                            status = status_data['log_request']['status']
                            if status == 'processed':
                                num_parts = len(status_data['log_request']['parts'])
                                logger.debug(f'Log is processed. Number of parts: {num_parts}')
                                break
                            else:
                                logger.debug(f'Log status: {status}')
                        await asyncio.sleep(5)
                    except aiohttp.ClientResponseError as e:
                        status_retries += 1
                        if status_retries >= max_status_retries:
                            logger.error(f'Max retries reached for getting log status. Giving up.')
                            return
                        else:
                            logger.error(f'Error getting log status: {e}. Retrying ({status_retries}/{max_status_retries}) after 5 seconds...')
                            await asyncio.sleep(5)
                    except KeyError:
                        status_retries += 1
                        if status_retries >= max_status_retries:
                            logger.error(f'Max retries reached for getting log status. Giving up.')
                            return
                        else:
                            logger.error('Unexpected response format when checking log status.')
                            await asyncio.sleep(5)

                df_list = []
                for i in range(num_parts):
                    download_retries = 0
                    while download_retries < max_download_retries:
                        try:
                            await asyncio.sleep(1)
                            download_url = f'https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequest/{log_id}/part/{i}/download'
                            async with session.get(download_url) as response:
                                response.raise_for_status()
                                content = await response.read()

                                df = pd.read_csv(BytesIO(content), 
                                                sep='\t', 
                                                dtype={'ym:s:clientID': 'str', 'ym:s:counterUserIDHash': 'str', 'ym:s:visitID': 'str'},
                                                low_memory=False)
                                df_list.append(df)
                                logger.debug(f'Part {i} downloaded and read into DataFrame')
                                break
                        except aiohttp.ClientResponseError as e:
                            if e.status == 429:
                                download_retries += 1
                                if download_retries >= max_download_retries:
                                    logger.error(f'Max retries reached for downloading part {i}. Giving up on this part.')
                                    break
                                else:
                                    logger.error(f'Error downloading part {i}: {e}. Retrying ({download_retries}/{max_download_retries}) after 5 seconds...')
                                    await asyncio.sleep(5)
                            else:
                                download_retries += 1
                                if download_retries >= max_download_retries:
                                    logger.error(f'Max retries reached for downloading part {i}. Giving up on this part.')
                                    break
                                else:
                                    logger.error(f'Unexpected error downloading part {i}: {e}. Retrying ({download_retries}/{max_download_retries}) after 5 seconds...')
                                    await asyncio.sleep(5)

                if df_list:
                    result_df = pd.concat(df_list, ignore_index=True)
                    logger.debug('All parts concatenated into a single DataFrame')
                else:
                    logger.error('No data downloaded.')
                    return

                try:
                    clean_url = f'https://api-metrika.yandex.ru/management/v1/counter/{int(counter_id)}/logrequest/{log_id}/clean'
                    async with session.post(clean_url) as response:
                        response.raise_for_status()
                        logger.debug(f'Log {log_id} cleaned up.')
                except aiohttp.ClientResponseError as e:
                    logger.error(f'Error cleaning up log: {e}')
                except Exception as e:
                    logger.error(f'Unexpected error cleaning up log: {e}')

                df_logs = result_df

            finally:
                await session.close()

        async def _report_metrika():
            nonlocal df_report
            """
            Downloads data from the Yandex Metrika Reporting API and stores the DataFrame in df_report.
            """

            met = [
                'ym:s:visits'
            ]

            dim = [
                'ym:s:visitID', 'ym:s:gender', 'ym:s:interest', 
                'ym:s:isRobot', 'ym:s:regionArea',  'ym:s:userVisits', 
                'ym:s:userVisitsPeriod', 'ym:s:ageInterval',
                'ym:s:lastsignSearchPhrase', 'ym:s:lastsignSourceEngine'
            ]

            limit = 100000
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            delta = timedelta(days=5)

            all_data = []

            headers = {'Authorization': f'OAuth {token}'}
            report_limiters: dict[str, AsyncRequestLimiter] = getattr(self, "_report_limiters", {})
            report_limiters_lock: asyncio.Lock = getattr(self, "_report_limiters_lock", asyncio.Lock())

            async def _get_report_limiter() -> AsyncRequestLimiter:
                limiter = report_limiters.get(token)
                if limiter is not None:
                    return limiter
                async with report_limiters_lock:
                    limiter = report_limiters.get(token)
                    if limiter is None:
                        limiter = AsyncRequestLimiter(max_concurrent=3, min_interval=0.12)
                        report_limiters[token] = limiter
                        self._report_limiters = report_limiters
                        self._report_limiters_lock = report_limiters_lock
                return limiter

            async with aiohttp.ClientSession(headers=headers) as session:
                current_start = start_date_obj
                while current_start <= end_date_obj:
                    current_end = min(current_start + delta - timedelta(days=1), end_date_obj)
                    params = {
                        'ids': int(counter_id),
                        'metrics': ','.join(met),
                        'dimensions': ','.join(dim),
                        'date1': current_start.strftime("%Y-%m-%d"),
                        'date2': current_end.strftime("%Y-%m-%d"),
                        'accuracy': '1',
                        'proposed_accuracy': 'true',
                        'include_undefined': 'false',
                        'lang': 'ru',
                        'limit': str(limit)
                    }
                    url = 'https://api-metrika.yandex.net/stat/v1/data'
                    try:
                        limiter = await _get_report_limiter()
                        async with limiter:
                            async with session.get(url, params=params) as response:
                                response.raise_for_status()
                                resp_json = await response.json()
                                data = resp_json.get('data', [])
                                if not data:
                                    logger.debug(f'No data for period {current_start.strftime("%Y-%m-%d")} - {current_end.strftime("%Y-%m-%d")}')
                                else:
                                    records = []
                                    for item in data:
                                        dimensions = item['dimensions']
                                        metrics = item['metrics']
                                        record = {}
                                        for idx, dim_item in enumerate(dimensions):
                                            record[dim[idx]] = dim_item.get('name') or dim_item.get('id')
                                        for idx, metric_value in enumerate(metrics):
                                            record[met[idx]] = metric_value
                                        records.append(record)
                                    df = pd.DataFrame(records)
                                    all_data.append(df)
                                    logger.debug(f'Data retrieved for period {current_start.strftime("%Y-%m-%d")} - {current_end.strftime("%Y-%m-%d")}')
                    except aiohttp.ClientResponseError as e:
                        if e.status in (429, 420):
                            logger.warning(
                                "Rate limit hit for counter %s (%s-%s), HTTP %s. Sleeping 3s.",
                                counter_id,
                                current_start.strftime("%Y-%m-%d"),
                                current_end.strftime("%Y-%m-%d"),
                                e.status,
                            )
                            await asyncio.sleep(3)
                            continue
                        logger.error(f'Error retrieving data for period {current_start.strftime("%Y-%m-%d")} - {current_end.strftime("%Y-%m-%d")}: {e}')
                        return
                    except Exception as e:
                        logger.error(f'Unexpected error retrieving data: {e}')
                        return

                    current_start += delta

                if all_data:
                    final_df = pd.concat(all_data, ignore_index=True)
                else:
                    logger.debug('No data retrieved from Reports API.')
                    return None

                agg_functions = {
                    'ym:s:gender': 'first',
                    'ym:s:interest': lambda x: ', '.join(x.dropna().unique()),
                    'ym:s:isRobot': 'first',
                    'ym:s:regionArea': 'first',
                    'ym:s:userVisits': 'first',
                    'ym:s:userVisitsPeriod': 'first',
                    'ym:s:ageInterval': 'first',
                    'ym:s:visits': 'first',
                    'ym:s:lastsignSearchPhrase': 'first',
                    'ym:s:lastsignSourceEngine': 'first'
                }

                final_df = final_df.groupby('ym:s:visitID', as_index=False).agg(agg_functions)
                final_df['ym:s:visitID'] = final_df['ym:s:visitID'].astype(str)

                df_report = final_df

        await asyncio.gather(
            _download_metrica_logs(),
            _report_metrika()
        )

        if df_logs is None or df_report is None:
            logger.debug('Failed to retrieve data from one or both APIs.')
            return pd.DataFrame()

        df_logs['ym:s:visitID'] = df_logs['ym:s:visitID'].astype(str)
        df_report['ym:s:visitID'] = df_report['ym:s:visitID'].astype(str)

        final_df = df_report.merge(df_logs, on='ym:s:visitID', how='left')

        final_df = self.preprocess_data(final_df)
        final_df = _goal_modification(final_df, counter_id, token)
        final_df = final_df.replace({pd.NA: np.nan})

        return final_df

    async def upload_data(self):

        df = await self.load_metrika(
            counter_id=int(self.counter),
            token=self.token, 
            start_date=self.start,
            end_date=self.end
        )

        if df.empty:
            get_run_logger().warning(f"No data retrieved for counter {self.counter} from {self.start} to {self.end}")
            return

        async_db = AsyncMetrikaDatabase()
        await async_db.init_db()

        if not df.empty:
            await async_db.write_dataframe_to_table(df, self.domain_name)
        else:
            get_run_logger().warning(f"Skipped writing empty DataFrame for counter {self.counter}")

    def split_date_range(self, start_date, end_date, chunk_size):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        date_chunks = []

        while start_date < end_date:
            chunk_end_date = min(
                start_date + timedelta(days=chunk_size), end_date
            )
            date_chunks.append((
                start_date.strftime("%Y-%m-%d"),
                chunk_end_date.strftime("%Y-%m-%d"),
            ))
            start_date = chunk_end_date + timedelta(days=1)

        return date_chunks


__all__ = ["YaMetrikaUploader"]