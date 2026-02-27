import html
import logging

from datetime import datetime, timedelta, timezone

from aiogram import Bot
import httpx

from database.user import UserDatabaseConnector
from config.settings import settings


logger = logging.getLogger(__name__)

_seen_failed_runs: set[str] = set()
_recent_alerts: list[str] = []
_last_checked: datetime | None = None

_PREFECT_API_URL = settings.PREFECT_API_URL.rstrip("/")
_ERROR_STATES = {"FAILED", "CRASHED", "CANCELLED"}
_IGNORE_TAG = "admin_bot"


def _parse_state(payload: dict) -> tuple[str, str | None]:
    """
    Extract normalized Prefect state type and message from flow_run payload.
    """
    state_obj = payload.get("state") or {}
    state_type = (payload.get("state_type") or state_obj.get("type") or "").upper() or "UNKNOWN"
    state_details = state_obj.get("state_details") or payload.get("state_details") or {}
    state_message = state_obj.get("message") or state_details.get("error") or state_details.get("message")
    return state_type, state_message


async def _fetch_failed_flow_runs(since_dt: datetime, limit: int = 50) -> list[dict]:
    """
    Query Prefect for flow runs that failed since `since_dt`,
    results are filtered later to exclude admin_bot-tagged runs.
    """
    iso_since = since_dt.astimezone(timezone.utc).isoformat()
    query = {
        "flow_runs": {
            "state": {"type": {"any_": list(_ERROR_STATES)}},
            "start_time": {"after_": iso_since},
        },
        "limit": limit,
        "sort": "START_TIME_DESC",
    }

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(f"{_PREFECT_API_URL}/flow_runs/filter", json=query)
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "Prefect API %s response: %s",
                exc.response.status_code,
                exc.response.text,
            )
            raise
        return resp.json() or []


async def _send_alert(bot: Bot, admin_ids: list[str], text: str) -> None:
    """Send alert message to all admin Telegram users."""
    for admin_id in admin_ids:
        try:
            await bot.send_message(chat_id=int(admin_id), text=text, parse_mode="HTML")
        except Exception as exc:
            logger.error("Failed to notify admin %s: %s", admin_id, exc)


async def _get_admin_ids() -> list[str]:
    """Load admin user IDs from the database."""
    connector = UserDatabaseConnector(settings.DB_NAME)
    await connector.initialize()
    return await connector.get_admin_user_ids()


async def check_logs(bot: Bot):
    """
    Check Prefect for failed automatic runs (no admin_bot tag) and notify admins.
    Intended for periodic scheduler use.
    """
    global _last_checked

    check_start = datetime.now(timezone.utc)
    since = _last_checked or (check_start - timedelta(minutes=30))
    _last_checked = check_start

    try:
        failed_runs = await _fetch_failed_flow_runs(since)
    except Exception as exc:
        logger.error("Prefect API error while fetching failed runs: %s", exc)
        return

    if not failed_runs:
        return

    new_runs = [
        run for run in failed_runs
        if run.get("id") not in _seen_failed_runs
        and _IGNORE_TAG not in (run.get("tags") or [])
    ]
    if not new_runs:
        return

    try:
        admin_ids = await _get_admin_ids()
    except Exception as exc:
        logger.error("Could not load admin ids: %s", exc)
        return

    for run in new_runs:
        run_id = run.get("id")
        _seen_failed_runs.add(run_id)

        state_type, state_message = _parse_state(run)
        tags = ", ".join(run.get("tags") or [])
        deployment_label = run.get("deployment_name") or run.get("deployment_id") or "unknown deployment"
        flow_name = run.get("name") or run_id or "unknown run"
        start_time = run.get("start_time") or "unknown"

        body = (
            "🚨 Prefect запуск упал\n"
            f"Деплоймент: {html.escape(str(deployment_label))}\n"
            f"Flow run: {html.escape(str(flow_name))}\n"
            f"Старт: {html.escape(str(start_time))}\n"
            f"Статус: {html.escape(state_type)}"
        )
        if state_message:
            body += f"\nДетали: {html.escape(str(state_message))}"
        if tags:
            body += f"\nТеги: {html.escape(tags)}"
        body += f"\nRun ID: <code>{html.escape(str(run_id))}</code>"

        _recent_alerts.append(body)
        _recent_alerts[:] = _recent_alerts[-20:]

        await _send_alert(bot, admin_ids, body)
