import logging

from prefect_loader.orchestration.clickhouse_utils import AsyncVkDatabase

from .api import VkApiClient


def get_logger():
    """Get Prefect logger if available, otherwise use module logger."""
    try:
        from prefect import get_run_logger
        from prefect.context import MissingContextError
        return get_run_logger()
    except (ImportError, MissingContextError, RuntimeError):
        return logging.getLogger(__name__)


async def get_vk_agencies(
    access_db: AsyncVkDatabase,
) -> dict[str, dict[str, str]]:
    """
    Get all VK agency credentials from Accesses table.

    Args:
        access_db: VK database instance

    Returns:
        Dictionary mapping agency_id -> {"client_id": ..., "client_secret": ..., "container": ...}
    """
    rows = await access_db.fetch_vk_access_rows(include_null_type=True)

    agencies = {}
    for row in rows:
        client_id = row.get("login")
        client_secret = row.get("token")
        container = row.get("container") or None
        service = row.get("service")

        if not client_id or not client_secret:
            continue

        if service and service != "vk":
            continue

        agencies[client_id] = {
            "client_id": client_id,
            "client_secret": client_secret,
            "container": container,
        }

    return agencies


async def get_agency_clients_with_activity(
    client_id: str,
    client_secret: str,
    *,
    lookback_days: int = 10,
) -> list[dict[str, str]]:
    """
    Get active agency clients with recent activity.

    Args:
        client_id: VK agency client ID
        client_secret: VK agency client secret
        lookback_days: Days to check for activity (default: 10)

    Returns:
        List of dicts with user_id and activity info
    """
    from datetime import date, timedelta

    logger = get_logger()
    api_client = VkApiClient(client_id, client_secret)

    try:
        await api_client.delete_access_token("")
        logger.info("Agency tokens reset for %s", client_id)

        agency_token = await api_client.get_access_token()

        clients_df = await api_client.get_agency_clients(agency_token)

        if clients_df.empty:
            logger.info("%s: No clients found", client_id)
            return []

        if "user.status" in clients_df.columns:
            clients_df = clients_df[clients_df["user.status"] == "active"]

        if clients_df.empty:
            logger.info("%s: No active clients found", client_id)
            return []

        if "user.id" not in clients_df.columns:
            logger.warning("%s: No user.id column in clients data", client_id)
            return []

        active_clients = []
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=lookback_days)

        for _, row in clients_df.iterrows():
            user_id = str(row["user.id"])

            try:
                client_token = await api_client.get_access_token(user_id)

                stats_df = await api_client.get_user_daily_statistics(
                    client_token, start_date, end_date
                )

                if not stats_df.empty and "base_shows" in stats_df.columns:
                    total_shows = stats_df["base_shows"].sum()
                    if total_shows > 0:
                        active_clients.append({
                            "user_id": user_id,
                            "shows": int(total_shows),
                        })
                        logger.debug("%s: Client %s has %d shows", client_id, user_id, total_shows)

                await api_client.delete_access_token(user_id)

            except Exception as exc:
                logger.warning("%s: Error checking client %s: %s", client_id, user_id, exc)
                continue

        logger.info(
            "%s: Found %d active clients (out of %d total)",
            client_id,
            len(active_clients),
            len(clients_df),
        )

        return active_clients

    except Exception as exc:
        logger.error("%s: Failed to fetch agency clients: %s", client_id, exc)
        return []


__all__ = [
    "get_vk_agencies",
    "get_agency_clients_with_activity",
]
