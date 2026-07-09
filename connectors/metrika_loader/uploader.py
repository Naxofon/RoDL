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
        self.attribution = 'cross_device_last_significant'

    @staticmethod
    def _coerce_param_array(value) -> list:
        if isinstance(value, (list, tuple)):
            return list(value)
        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped or stripped in {'[]', 'nan', 'NaN', 'None'}:
                return []
            try:
                parsed = ast.literal_eval(stripped)
            except (SyntaxError, ValueError):
                return []
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
            return []

        try:
            if pd.isna(value):
                return []
        except (TypeError, ValueError):
            return []

        return []

    @classmethod
    def _extract_param_value(cls, keys, values, target_key="yclid"):
        target = str(target_key).strip().lower()
        keys_list = cls._coerce_param_array(keys)
        values_list = cls._coerce_param_array(values)

        for index, key in enumerate(keys_list):
            try:
                if pd.isna(key):
                    continue
            except (TypeError, ValueError):
                continue

            if str(key).strip().lower() != target:
                continue
            if index >= len(values_list):
                continue

            value = values_list[index]
            try:
                if pd.isna(value):
                    continue
            except (TypeError, ValueError):
                continue

            value_text = str(value).strip()
            if not value_text or value_text in {'nan', 'NaN', 'None'}:
                continue
            return value_text

        return None

    @staticmethod
    def _filter_logs_by_report_keys(df_logs: pd.DataFrame, df_report: pd.DataFrame) -> pd.DataFrame:
        """Keep only Logs API rows that have an exact clientID + dateTime match in Reports API."""
        if df_logs is None or df_logs.empty:
            return pd.DataFrame() if df_logs is None else df_logs.copy()
        if df_report is None or df_report.empty:
            return df_logs.iloc[0:0].copy()

        key_columns = ["ym:s:clientID", "ym:s:dateTime"]
        missing_logs_columns = [column for column in key_columns if column not in df_logs.columns]
        missing_report_columns = [column for column in key_columns if column not in df_report.columns]
        if missing_logs_columns:
            raise KeyError(f"Logs API dataframe is missing key columns: {missing_logs_columns}")
        if missing_report_columns:
            raise KeyError(f"Reports API dataframe is missing key columns: {missing_report_columns}")

        logs = df_logs.copy()
        logs["_logs_order"] = range(len(logs))
        reports = df_report[key_columns].copy()

        for frame in (logs, reports):
            frame["ym:s:clientID"] = frame["ym:s:clientID"].astype("string")
            frame["ym:s:dateTime"] = pd.to_datetime(frame["ym:s:dateTime"], errors="coerce")

        logs = logs.dropna(subset=key_columns)
        zero_client_logs = logs[logs["ym:s:clientID"] == "0"].copy()
        logs_for_match = logs[logs["ym:s:clientID"] != "0"].copy()
        reports = reports.dropna(subset=key_columns).drop_duplicates()
        if reports.empty:
            if zero_client_logs.empty:
                return logs.iloc[0:0].drop(columns=["_logs_order"], errors="ignore")
            return (
                zero_client_logs
                .sort_values("_logs_order")
                .drop(columns=["_logs_order"])
                .reset_index(drop=True)
            )

        filtered = logs_for_match.merge(reports, on=key_columns, how="inner")
        if not zero_client_logs.empty:
            filtered = pd.concat([zero_client_logs, filtered], ignore_index=True)
        filtered = filtered.sort_values("_logs_order").drop(columns=["_logs_order"])
        return filtered.reset_index(drop=True)

    def preprocess_data(self, df):
        """
        Preprocesses the Logs API DataFrame by cleaning and transforming various columns.

        Parameters:
            df (pd.DataFrame): DataFrame returned by Yandex Metrika Logs API.

        Returns:
            pd.DataFrame: Preprocessed DataFrame.
        """

        df = df.copy()
        df = df.replace({pd.NA: np.nan})

        df.columns = df.columns.str.replace('^ym:s:', '', regex=True)

        def _normalize_string_null_markers(frame: pd.DataFrame) -> pd.DataFrame:
            string_like_columns = frame.select_dtypes(
                include=['object', 'string']
            ).columns
            if len(string_like_columns) == 0:
                return frame
            normalized = frame.copy()
            normalized[string_like_columns] = normalized[string_like_columns].mask(
                normalized[string_like_columns].isin(['nan', 'NaN', 'None']),
                np.nan,
            )
            return normalized

        device_category_mapping = {
            1: "десктоп",
            2: "мобильные телефоны",
            3: "планшеты",
            4: "TV"
        }

        columns_to_replace = ['screenFormat']

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

        if 'deviceCategory' in df.columns:
            df["deviceCategory"] = df["deviceCategory"].replace(
                device_category_mapping
            )

        existing_columns_to_replace = [
            column for column in columns_to_replace if column in df.columns
        ]
        if existing_columns_to_replace:
            df[existing_columns_to_replace] = df[existing_columns_to_replace].replace(
                {'undefined': np.nan, 'Не определено': np.nan}
            )

        if 'dateTimeUTC' in df.columns:
            df['dateTimeUTC'] = pd.to_datetime(df['dateTimeUTC'], errors='coerce')
        if 'dateTime' in df.columns:
            df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')

        for col in ['bounce', 'isNewUser']:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors='coerce')
                    .fillna(0)
                    .astype('uint8')
                )

        df = _normalize_string_null_markers(df)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        if {'parsedParamsKey2', 'parsedParamsKey3'}.issubset(df.columns):
            df['yclid'] = df.apply(
                lambda row: self._extract_param_value(
                    row['parsedParamsKey2'],
                    row['parsedParamsKey3'],
                    'yclid',
                ),
                axis=1,
            )
        else:
            df['yclid'] = None

        required_metric_columns = [
            column for column in ['visitDuration', 'pageViews'] if column in df.columns
        ]
        for column in required_metric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)

        if 'visitID' in df.columns:
            df['visits'] = df['visitID'].notna().astype('int64')

        goal_columns = df.filter(regex='^goal_').columns.tolist()
        df['sum_goal'] = df[goal_columns].sum(axis=1)

        astype_map = {
            'sum_goal': 'int',
            'visitID': 'str',
            'visits': 'int',
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
            'parsedParamsKey3': 'str',
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
        df = df.astype({column: dtype for column, dtype in astype_map.items() if column in df.columns})

        df = _normalize_string_null_markers(df)

        string_columns = [
            'clientID', 'visitID', 'counterUserIDHash', 'regionCountry',
            'lastsignTrafficSource', 'lastsignAdvEngine', 'lastsignReferalSource',
            'lastsignSearchEngineRoot', 'lastsignSearchEngine', 'ipAddress', 'lastsignSocialNetwork',
            'screenFormat', 'startURL', 'endURL', 'mobilePhone', 'mobilePhoneModel',
            'operatingSystemRoot', 'operatingSystem', 'browser', 'browserLanguage',
            'lastsignRecommendationSystem', 'lastsignMessenger', 'regionCity',
            'deviceCategory', 'UTMCampaign', 'UTMContent', 'UTMMedium', 'UTMSource',
            'UTMTerm', 'referer', 'parsedParamsKey1', 'parsedParamsKey2', 'parsedParamsKey3', 'lastsignDirectClickOrderName',
            'lastsignClickBannerGroupName', 'lastsignDirectClickBannerName',
            'lastsignDirectPhraseOrCond', 'lastsignDirectPlatformType', 'lastsignDirectPlatform',
            'lastsignDirectConditionType', 'offlineCallTalkDuration', 'offlineCallHoldDuration',
            'offlineCallMissed', 'offlineCallTag', 'offlineCallFirstTimeCaller', 'offlineCallURL', 'screenOrientationName',
            'screenWidth', 'screenHeight', 'physicalScreenWidth', 'physicalScreenHeight', 'windowClientWidth',
            'windowClientHeight', 'browserMinorVersion', 'browserEngine', 'browserEngineVersion1',
            'browserEngineVersion2', 'browserEngineVersion3', 'browserEngineVersion4'
        ]

        bool_columns = [
            'bounce', 'isNewUser'
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
        Downloads data from Yandex Metrika Logs API, filters it by Reports API keys,
        and returns the processed DataFrame.

        Parameters:
            counter_id (str): Counter ID.
            token (str): OAuth token.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: Processed DataFrame built from Logs API data.
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
                        f'ym:s:{self.attribution}TrafficSource',f'ym:s:{self.attribution}AdvEngine',
                        f'ym:s:{self.attribution}ReferalSource',f'ym:s:{self.attribution}SearchEngineRoot',
                        f'ym:s:{self.attribution}SearchEngine', 'ym:s:ipAddress', 'ym:s:bounce',
                        f'ym:s:{self.attribution}SocialNetwork', 'ym:s:visitDuration', 'ym:s:screenFormat',
                        'ym:s:pageViews', 'ym:s:startURL', 'ym:s:endURL', 'ym:s:mobilePhone', 'ym:s:mobilePhoneModel',
                        'ym:s:operatingSystemRoot', 'ym:s:operatingSystem', 'ym:s:browser', 'ym:s:browserMajorVersion',
                        'ym:s:isNewUser', 'ym:s:regionCountry', 'ym:s:browserLanguage', f'ym:s:{self.attribution}RecommendationSystem', f'ym:s:{self.attribution}Messenger',
                        'ym:s:regionCity', 'ym:s:deviceCategory', 'ym:s:clientTimeZone',
                        'ym:s:UTMCampaign', 'ym:s:UTMContent', 'ym:s:UTMMedium',
                        'ym:s:UTMSource', 'ym:s:UTMTerm', 'ym:s:referer', 'ym:s:parsedParamsKey1', 'ym:s:parsedParamsKey2','ym:s:parsedParamsKey3',
                        f'ym:s:{self.attribution}DirectClickOrder', f'ym:s:{self.attribution}DirectBannerGroup',
                        f'ym:s:{self.attribution}DirectClickBanner', f'ym:s:{self.attribution}DirectClickOrderName',
                        f'ym:s:{self.attribution}ClickBannerGroupName', f'ym:s:{self.attribution}DirectClickBannerName',
                        f'ym:s:{self.attribution}DirectPhraseOrCond', f'ym:s:{self.attribution}DirectPlatformType',
                        f'ym:s:{self.attribution}DirectPlatform', f'ym:s:{self.attribution}DirectConditionType',
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
                            'source': 'visits',
                            'attribution': self.attribution
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
            Downloads minimal visit keys from the Yandex Metrika Reporting API and stores them in df_report.
            """

            metrics = ["ym:s:visits"]
            dimensions = ["ym:s:clientID", "ym:s:dateTime"]
            limit = 100000
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            delta = timedelta(days=5)
            all_data = []

            headers = {"Authorization": f"OAuth {token}"}
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
                    offset = 1

                    while True:
                        params = {
                            "ids": int(counter_id),
                            "metrics": ",".join(metrics),
                            "dimensions": ",".join(dimensions),
                            "date1": current_start.strftime("%Y-%m-%d"),
                            "date2": current_end.strftime("%Y-%m-%d"),
                            "accuracy": "1",
                            "proposed_accuracy": "true",
                            "include_undefined": "false",
                            "lang": "ru",
                            "limit": str(limit),
                            "offset": str(offset),
                        }
                        url = "https://api-metrika.yandex.net/stat/v1/data"

                        try:
                            limiter = await _get_report_limiter()
                            async with limiter:
                                async with session.get(url, params=params) as response:
                                    response.raise_for_status()
                                    resp_json = await response.json()
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
                            logger.error(
                                "Error retrieving reports data for period %s - %s: %s",
                                current_start.strftime("%Y-%m-%d"),
                                current_end.strftime("%Y-%m-%d"),
                                e,
                            )
                            return
                        except Exception as e:
                            logger.error("Unexpected error retrieving reports data: %s", e)
                            return

                        data = resp_json.get("data", [])
                        if data:
                            records = []
                            for item in data:
                                dims = item.get("dimensions", [])
                                record = {}
                                for idx, dim_item in enumerate(dims):
                                    record[dimensions[idx]] = dim_item.get("name") or dim_item.get("id")
                                for idx, metric_value in enumerate(item.get("metrics", [])):
                                    record[metrics[idx]] = metric_value
                                records.append(record)
                            all_data.append(pd.DataFrame(records))

                        total_rows = int(resp_json.get("total_rows", 0) or 0)
                        if offset + limit > total_rows or not data:
                            break
                        offset += limit

                    current_start += delta

            if not all_data:
                logger.debug("No data retrieved from Reports API.")
                return

            report_keys = pd.concat(all_data, ignore_index=True)
            report_keys = report_keys[dimensions].copy()
            report_keys["ym:s:clientID"] = report_keys["ym:s:clientID"].astype("string")
            report_keys["ym:s:dateTime"] = pd.to_datetime(report_keys["ym:s:dateTime"], errors="coerce")
            report_keys = report_keys.dropna(subset=dimensions).drop_duplicates().reset_index(drop=True)
            df_report = report_keys

        await asyncio.gather(
            _download_metrica_logs(),
            _report_metrika(),
        )

        if df_logs is None or df_report is None:
            logger.debug("Failed to retrieve data from one or both Metrika APIs.")
            return pd.DataFrame()

        df_logs['ym:s:visitID'] = df_logs['ym:s:visitID'].astype(str)
        rename_cols = {}
        for i in df_logs.columns:
            if 'cross_device_last_significant' in i:
                rename_cols[i] = i.replace('cross_device_last_significant', 'lastsign')
                df_logs = df_logs.rename(columns=rename_cols)
        df_logs['attribution'] = self.attribution


        final_df = self._filter_logs_by_report_keys(df_logs, df_report)
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
