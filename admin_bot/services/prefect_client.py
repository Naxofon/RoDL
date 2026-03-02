import asyncio
import html
import os
import time
from typing import Iterable

import httpx
from aiogram import types

_TERMINAL_SUCCESS = {"COMPLETED", "SUCCESS"}
_TERMINAL_FAILED = {"FAILED", "CRASHED", "CANCELLED"}
_TERMINAL_ALL = _TERMINAL_SUCCESS | _TERMINAL_FAILED


def get_env(name: str, default: str | None = None) -> str | None:
    """Wrapper to make getenv testable and allow defaults."""
    return os.getenv(name, default)


async def trigger_prefect_run(
    *,
    deployment_name: str,
    parameters: dict | None = None,
    tags: Iterable[str] | None = None,
) -> str:
    """
    Create a Prefect flow run by calling the HTTP API directly.
    This avoids pydantic/StateCreate issues in some Prefect versions.
    """
    if not deployment_name:
        raise ValueError("deployment_name is required")

    api_url = get_env("PREFECT_API_URL", "http://prefect-server:4200/api").rstrip("/")
    dep_url = f"{api_url}/deployments/name/{deployment_name}"

    async with httpx.AsyncClient(timeout=30) as client:
        dep_resp = await client.get(dep_url)
        dep_resp.raise_for_status()
        dep_json = dep_resp.json()
        deployment_id = dep_json.get("id") or dep_json.get("deployment_id")
        if not deployment_id:
            raise RuntimeError("Deployment found but id is missing in response")

        run_url = f"{api_url}/deployments/{deployment_id}/create_flow_run"
        run_resp = await client.post(
            run_url,
            json={
                "parameters": parameters or {},
                "tags": list(tags) if tags else [],
            },
        )
        run_resp.raise_for_status()
        run_json = run_resp.json()
        run_id = run_json.get("id") or run_json.get("flow_run_id")
        if not run_id:
            raise RuntimeError("Flow run created but id missing in response")
        return str(run_id)


def _parse_state(payload: dict) -> tuple[str, str, str | None]:
    """
    Extract normalized state fields from Prefect API response.
    Handles both Prefect 2.x and 3.x payload shapes.
    """
    state_obj = payload.get("state") or {}
    state_type = (payload.get("state_type") or state_obj.get("type") or "").upper()
    state_name = payload.get("state_name") or state_obj.get("name") or state_type or "UNKNOWN"
    state_details = state_obj.get("state_details") or payload.get("state_details") or {}
    state_message = state_obj.get("message") or state_details.get("error") or state_details.get("message")
    return state_type, state_name, state_message


async def wait_for_prefect_flow_run(
    run_id: str,
    *,
    deployment_name: str,
    notify_message: types.Message,
    poll_interval: int = 20,
    max_wait_seconds: int = 60 * 60 * 6,
) -> None:
    """
    Poll Prefect for run status and send a final message to the user when it finishes.
    Sends a warning if status cannot be fetched repeatedly or times out.
    """
    api_url = get_env("PREFECT_API_URL", "http://prefect-server:4200/api").rstrip("/")
    flow_run_url = f"{api_url}/flow_runs/{run_id}"
    deadline = time.monotonic() + max_wait_seconds
    errors = 0
    last_state = "UNKNOWN"

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            if time.monotonic() >= deadline:
                await notify_message.answer(
                    "⚠️ Prefect run не завершился за ожидаемое время.\n"
                    f"Деплоймент: {deployment_name}\n"
                    f"Run ID: <code>{run_id}</code>\n"
                    f"Последний статус: {html.escape(last_state)}",
                )
                return

            try:
                resp = await client.get(flow_run_url)
                resp.raise_for_status()
                payload = resp.json()
                errors = 0
            except Exception as exc:
                errors += 1
                if errors >= 3:
                    await notify_message.answer(
                        "⚠️ Не удалось получить статус Prefect run.\n"
                        f"Деплоймент: {deployment_name}\n"
                        f"Run ID: <code>{run_id}</code>\n"
                        f"Ошибка: {html.escape(str(exc))}",
                    )
                    return
                await asyncio.sleep(poll_interval)
                continue

            state_type, state_name, state_message = _parse_state(payload)
            last_state = state_name
            normalized_state = (state_type or state_name).upper()

            if normalized_state in _TERMINAL_ALL or state_name.upper() in _TERMINAL_ALL:
                if normalized_state in _TERMINAL_SUCCESS or state_name.upper() in _TERMINAL_SUCCESS:
                    await notify_message.answer(
                        "✅ Prefect run завершён.\n"
                        f"Деплоймент: {deployment_name}\n"
                        f"Run ID: <code>{run_id}</code>\n"
                        f"Статус: {html.escape(state_name)}",
                    )
                else:
                    detail_text = f"\nДетали: {html.escape(str(state_message))}" if state_message else ""
                    await notify_message.answer(
                        "❌ Prefect run завершился с ошибкой.\n"
                        f"Деплоймент: {deployment_name}\n"
                        f"Run ID: <code>{run_id}</code>\n"
                        f"Статус: {html.escape(state_name)}"
                        f"{detail_text}",
                    )
                return

            await asyncio.sleep(poll_interval)