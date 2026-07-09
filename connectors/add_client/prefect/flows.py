import asyncio
import sys
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ROOT_PATH = PROJECT_ROOT.parent.as_posix()
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from prefect_loader.orchestration.clickhouse_utils.config import (
    CLICKHOUSE_ACCESS_DATABASE,
    CLICKHOUSE_ACCESS_PASSWORD,
    CLICKHOUSE_ACCESS_USER,
)
from prefect_loader.orchestration.clickhouse_utils.database import ClickhouseDatabase
from prefect_loader.orchestration.clickhouse_utils.helpers import (
    _compose_type,
    _quote_nullable,
)
from prefect_loader.orchestration.clickhouse_utils.schema import insert_dataframe


def _exact_access_where(login: str, container: str | None, stored_type: str | None) -> str:
    clauses = [f"login = {_quote_nullable(login)}"]
    if container is None:
        clauses.append("container IS NULL")
    else:
        clauses.append(f"container = {_quote_nullable(container)}")
    if stored_type is None:
        clauses.append("type IS NULL")
    else:
        clauses.append(f"type = {_quote_nullable(stored_type)}")
    return " AND ".join(clauses)


@flow(name="add-client-access")
async def add_client_access_flow(
    login: str,
    token: str,
    service_type: str,
    container: str | None = None,
    type_value: str | None = None,
) -> dict:
    """
    Add or update one row in ClickHouse Accesses.

    `service_type` is the connector name, for example "metrika" or "direct".
    `type_value` is only the subtype, for example "client" or
    "not_agency_token"; it is stored together with service_type as
    "<service_type>:<type_value>".
    """
    logger = get_run_logger()

    access_db = ClickhouseDatabase(
        database=CLICKHOUSE_ACCESS_DATABASE,
        user=CLICKHOUSE_ACCESS_USER,
        password=CLICKHOUSE_ACCESS_PASSWORD,
    )

    stored_type = _compose_type(service_type, type_value)
    await access_db._prepare_accesses()

    def _replace_exact_access() -> int:
        where = _exact_access_where(login, container, stored_type)
        analytics_enabled = 1

        access_db.client.command(
            f"ALTER TABLE {access_db.database}.Accesses DELETE WHERE {where}",
            settings={"mutations_sync": 1},
        )
        insert_dataframe(
            access_db.client,
            "Accesses",
            pd.DataFrame(
                [
                    {
                        "login": login,
                        "token": token,
                        "container": container,
                        "type": stored_type,
                        "analytics_enabled": analytics_enabled,
                    }
                ]
            ),
        )
        return analytics_enabled

    analytics_enabled = await asyncio.to_thread(_replace_exact_access)

    logger.info(
        "Access upserted: login=%s service_type=%s container=%s type=%s analytics_enabled=%s",
        login,
        service_type,
        container,
        stored_type,
        analytics_enabled,
    )

    return {
        "login": login,
        "service_type": service_type,
        "container": container,
        "type_value": type_value,
        "type": stored_type,
        "analytics_enabled": analytics_enabled,
    }
