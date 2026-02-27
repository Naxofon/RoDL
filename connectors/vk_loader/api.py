import asyncio
import logging
import math
from datetime import date
from typing import Any

import aiohttp
import pandas as pd

from .config import (
    VK_API_BASE_URL,
    VK_BACKOFF_BASE,
    VK_MAX_RETRIES,
    VK_TIMEOUT_SECONDS,
    get_request_semaphore,
)


def get_logger():
    """Get Prefect logger if available, otherwise use module logger."""
    try:
        from prefect import get_run_logger
        from prefect.context import MissingContextError
        return get_run_logger()
    except (ImportError, MissingContextError, RuntimeError):
        return logging.getLogger(__name__)


class VkApiClient:
    """VK Ads API client with async support."""

    def __init__(self, client_id: str, client_secret: str):
        """
        Initialize VK API client.

        Args:
            client_id: VK application client ID
            client_secret: VK application client secret
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = VK_API_BASE_URL
        self.timeout = aiohttp.ClientTimeout(total=VK_TIMEOUT_SECONDS)

    async def _request_with_retry(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        **kwargs,
    ) -> dict[str, Any]:
        """Make an HTTP request with exponential backoff retry."""
        delay = 1.0
        for attempt in range(1, VK_MAX_RETRIES + 1):
            try:
                async with session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as exc:
                if attempt >= VK_MAX_RETRIES:
                    raise
                if hasattr(exc, "status") and exc.status in (401, 403):
                    raise
                await asyncio.sleep(delay)
                delay *= VK_BACKOFF_BASE
            except asyncio.TimeoutError:
                if attempt >= VK_MAX_RETRIES:
                    raise
                await asyncio.sleep(delay)
                delay *= VK_BACKOFF_BASE

    async def get_access_token(self, agency_client_id: str | None = None) -> str:
        """
        Get OAuth access token from VK Ads API.

        Args:
            agency_client_id: Optional client ID for agency client credentials

        Returns:
            Access token string
        """
        url = f"{self.base_url}/oauth2/token.json"

        if agency_client_id:
            data = {
                "grant_type": "agency_client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "agency_client_id": agency_client_id,
            }
        else:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            result = await self._request_with_retry(session, "POST", url, data=data)
            return result["access_token"]

    async def delete_access_token(self, user_id: str = "") -> None:
        """
        Delete access token from VK Ads API.

        Args:
            user_id: User ID to delete token for (empty string for all tokens)
        """
        url = f"{self.base_url}/oauth2/token/delete.json"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "user_id": user_id,
        }

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            await self._request_with_retry(session, "POST", url, data=data)

    async def get_agency_clients(self, token: str) -> pd.DataFrame:
        """
        Fetch agency clients from VK Ads API.

        Args:
            token: API authentication token

        Returns:
            DataFrame containing clients data
        """
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        url = f"{self.base_url}/agency/clients.json"
        items = []
        offset = 0
        limit = 50

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            while True:
                params = {"limit": limit, "offset": offset}
                data = await self._request_with_retry(
                    session, "GET", url, headers=headers, params=params
                )
                batch_items = data.get("items", [])
                if not batch_items:
                    break
                items.extend(batch_items)
                offset += len(batch_items)
                if offset >= data.get("count", 0):
                    break

        if not items:
            return pd.DataFrame()

        df = pd.json_normalize(items)
        return df

    async def get_user_daily_statistics(
        self,
        token: str,
        start_date: str | date,
        end_date: str | date,
    ) -> pd.DataFrame:
        """
        Fetch daily statistics for the authenticated user.

        Args:
            token: API authentication token
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with daily statistics
        """
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y-%m-%d")

        url = f"{self.base_url}/statistics/users/day.json"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        params = {
            "date_from": start_date,
            "date_to": end_date,
            "metrics": "base",
            "attribution": "conversion",
        }

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            data = await self._request_with_retry(
                session, "GET", url, headers=headers, params=params
            )

            items_data = data.get("items", [])
            if not items_data:
                return pd.DataFrame()

            rows_list = []
            for item in items_data:
                obj_id = item.get("id")
                daily_rows = item.get("rows", [])

                for row in daily_rows:
                    date_val = row.get("date")
                    if not date_val:
                        continue

                    flat_row_data = {"id": obj_id, "date": date_val}
                    for metric_group_key, metric_data in row.items():
                        if metric_group_key == "date":
                            continue
                        if isinstance(metric_data, dict):
                            for k, v in metric_data.items():
                                flat_row_data[f"{metric_group_key}_{k}"] = v
                        else:
                            flat_row_data[metric_group_key] = metric_data

                    rows_list.append(flat_row_data)

            return pd.DataFrame(rows_list) if rows_list else pd.DataFrame()

    async def get_ad_plans(self, token: str) -> dict[int, dict[str, Any]]:
        """
        Fetch ad plans with their IDs.

        Args:
            token: API authentication token

        Returns:
            Dictionary mapping plan_id -> {"name": plan_name, "ad_groups": {}}
        """
        url = f"{self.base_url}/ad_plans.json"
        headers = {"Authorization": f"Bearer {token}"}
        plans = {}
        offset = 0
        limit = 100

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            while True:
                params = {"limit": limit, "offset": offset, "fields": "id,name"}
                data = await self._request_with_retry(
                    session, "GET", url, headers=headers, params=params
                )
                plan_items = data.get("items", [])
                if not plan_items:
                    break

                for plan_item in plan_items:
                    plan_id = plan_item.get("id")
                    plan_name = plan_item.get("name", "Unnamed Plan")
                    if plan_id is not None:
                        plans[plan_id] = {"name": plan_name, "ad_groups": {}}

                offset += len(plan_items)
                if offset >= data.get("count", 0):
                    break

        return plans

    async def get_ad_groups_for_plan(
        self, token: str, plan_id: int
    ) -> dict[int, dict[str, str]]:
        """
        Fetch ad groups for a specific plan.

        Args:
            token: API authentication token
            plan_id: Ad plan ID

        Returns:
            Dictionary mapping group_id -> {"name": group_name}
        """
        url = f"{self.base_url}/ad_groups.json"
        headers = {"Authorization": f"Bearer {token}"}
        groups = {}
        offset = 0
        limit = 100

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            while True:
                params = {
                    "_ad_plan_id": plan_id,
                    "limit": limit,
                    "offset": offset,
                    "fields": "id,name",
                }
                data = await self._request_with_retry(
                    session, "GET", url, headers=headers, params=params
                )
                group_items = data.get("items", [])
                if not group_items:
                    break

                for group_item in group_items:
                    group_id = group_item.get("id")
                    group_name = group_item.get("name", "Unnamed Group")
                    if group_id is not None:
                        groups[group_id] = {"name": group_name}

                offset += len(group_items)

        return groups

    async def get_ad_plans_with_groups(self, token: str) -> dict[int, dict[str, Any]]:
        """
        Fetch all ad plans with their ad groups.

        Args:
            token: API authentication token

        Returns:
            Dictionary mapping plan_id -> {
                "name": plan_name,
                "ad_groups": {group_id: {"name": group_name}, ...}
            }
        """

        plans = await self.get_ad_plans(token)

        if not plans:
            return {}

        sem = get_request_semaphore()

        async def fetch_groups_for_plan(plan_id: int):
            async with sem:
                try:
                    groups = await self.get_ad_groups_for_plan(token, plan_id)
                    return plan_id, groups
                except Exception as exc:
                    get_logger().error(f"Failed to fetch groups for plan {plan_id}: {exc}")
                    return plan_id, {"error": str(exc)}

        tasks = [fetch_groups_for_plan(plan_id) for plan_id in plans.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                continue
            plan_id, groups = result
            if plan_id in plans:
                plans[plan_id]["ad_groups"] = groups

        return plans

    async def get_ad_group_daily_statistics(
        self,
        token: str,
        campaign_sources: dict[int, Any],
        start_date: str | date,
        end_date: str | date,
        metrics: str = "all",
        attribution: str = "conversion",
    ) -> pd.DataFrame:
        """
        Fetch daily statistics for ad groups.

        Args:
            token: API authentication token
            campaign_sources: Dictionary mapping campaign_id -> campaign details with ad_groups
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            metrics: Metrics to fetch
            attribution: Attribution model

        Returns:
            DataFrame with statistics
        """
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y-%m-%d")

        url = f"{self.base_url}/statistics/ad_groups/day.json"
        headers = {"Authorization": f"Bearer {token}"}

        group_id_to_details = {}
        all_group_ids = []

        for campaign_id, campaign_details in campaign_sources.items():
            if not isinstance(campaign_id, int) or not isinstance(campaign_details, dict):
                continue

            campaign_name = campaign_details.get("name")
            ad_groups = campaign_details.get("ad_groups", {})

            if not isinstance(ad_groups, dict):
                continue

            for group_id, group_data in ad_groups.items():
                if not isinstance(group_id, int) or not isinstance(group_data, dict):
                    continue

                group_name = group_data.get("name")
                if not group_name:
                    continue

                group_id_str = str(group_id)
                group_id_to_details[group_id_str] = {
                    "campaign_name": campaign_name,
                    "campaign_id": campaign_id,
                    "group_name": group_name,
                }
                all_group_ids.append(group_id_str)

        if not all_group_ids:
            return pd.DataFrame()

        unique_group_ids = sorted(list(set(all_group_ids)))
        batch_size = 100
        num_batches = math.ceil(len(unique_group_ids) / batch_size)
        all_stats_data = []

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            for i in range(num_batches):
                batch_ids = unique_group_ids[i * batch_size : (i + 1) * batch_size]
                batch_ids_str = ",".join(batch_ids)

                params = {
                    "date_from": start_date,
                    "date_to": end_date,
                    "id": batch_ids_str,
                    "metrics": metrics,
                    "attribution": attribution,
                }

                try:
                    data = await self._request_with_retry(
                        session, "GET", url, headers=headers, params=params
                    )
                    items = data.get("items", [])

                    for item in items:
                        item_group_id_str = str(item.get("id"))
                        if item_group_id_str not in group_id_to_details:
                            continue

                        group_details = group_id_to_details[item_group_id_str]

                        if "rows" in item and isinstance(item["rows"], list):
                            for row in item["rows"]:
                                row_data = {
                                    "campaign_name": group_details["campaign_name"],
                                    "campaign_id": group_details["campaign_id"],
                                    "group_name": group_details["group_name"],
                                    "group_id": int(item_group_id_str),
                                }
                                row_data.update(row)
                                all_stats_data.append(row_data)

                except Exception as exc:
                    get_logger().error(f"Error fetching batch {i + 1}/{num_batches}: {exc}")

                await asyncio.sleep(0.1)

        if not all_stats_data:
            return pd.DataFrame()

        return pd.DataFrame(all_stats_data)


__all__ = ["VkApiClient"]
