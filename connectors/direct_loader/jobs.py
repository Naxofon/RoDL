import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Any, Optional

from prefect_loader.orchestration.clickhouse_utils import AsyncDirectDatabase

from .access import collect_direct_login_tokens
from .change_tracker import DirectChangeTracker
from .config import DATABASE_PROFILES, DEFAULT_DB_PROFILE
from .shared_utils import convert_days_to_date_ranges


@dataclass(frozen=True)
class DirectReloadJob:
    """Configuration for a Direct data reload job.

    Attributes:
        login: Client login identifier
        token: API authentication token
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        profile: Database profile (default: "analytics")
        source: Source identifier (default: "unknown")
    """

    login: str
    token: str
    start_date: str
    end_date: str
    profile: str = DEFAULT_DB_PROFILE
    source: str = "unknown"

    def as_dict(self) -> dict[str, Any]:
        """Serialise this job to a plain dictionary."""
        return {
            "login": self.login,
            "token": self.token,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "profile": self.profile,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DirectReloadJob":
        """Construct a DirectReloadJob from a plain dictionary."""
        return cls(
            login=str(payload["login"]),
            token=str(payload["token"]),
            start_date=str(payload["start_date"]),
            end_date=str(payload["end_date"]),
            profile=str(payload.get("profile", DEFAULT_DB_PROFILE)),
            source=str(payload.get("source", "unknown")),
        )


async def plan_direct_reload_jobs(
    *,
    lookback_days: int = 60,
    login: Optional[str] = None,
    profile: str = DEFAULT_DB_PROFILE,
    db: AsyncDirectDatabase | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[list[DirectReloadJob], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Build a list of reload jobs (login/range/token) for change tracker runs.
    Returns a tuple (jobs, clients_without_changes, failed_clients).
    """
    profile_config = DATABASE_PROFILES[profile]

    target_db = db or AsyncDirectDatabase(profile_config.get('database'))
    created_db = db is None
    if created_db:
        await target_db.init_db()

    access_db = AsyncDirectDatabase()
    await access_db.init_db()

    jobs: list[DirectReloadJob] = []
    clients_without_changes: list[dict[str, Any]] = []
    failed_clients: list[dict[str, Any]] = []

    try:
        login_tokens = await collect_direct_login_tokens(access_db, profile=profile)
        if not login_tokens:
            logging.warning("No Direct tokens found in Accesses; nothing to reload.")
            return jobs, clients_without_changes, failed_clients

        if login is not None:
            normalized_login = AsyncDirectDatabase._normalize_login(login)
            if normalized_login not in login_tokens:
                logging.warning("Login %s not found in access list; nothing to process.", login)
                return jobs, clients_without_changes, failed_clients
            login_tokens = {normalized_login: login_tokens[normalized_login]}

        try:
            from prefect_loader.orchestration.flows import MAX_CONCURRENT_LOGINS
        except ImportError:
            MAX_CONCURRENT_LOGINS = 20

        change_detection_sem = asyncio.Semaphore(MAX_CONCURRENT_LOGINS)

        async def detect_changes_for_client(client_login: str, token: str):
            """Run change detection for a single client with concurrency control."""
            async with change_detection_sem:
                source = "direct_api"

                logging.info("%s: start detect_changes using profile=%s", client_login, profile)
                effective_lookback = lookback_days
                if start_date:
                    try:
                        end_boundary = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else datetime.now().date() - timedelta(days=1)
                        start_boundary = datetime.strptime(start_date, "%Y-%m-%d").date()
                        delta_days = (end_boundary - start_boundary).days + 1
                        if delta_days > 0:
                            effective_lookback = delta_days
                    except Exception:
                        pass

                client_db = AsyncDirectDatabase(profile_config.get('database'))
                await client_db.init_db()

                try:
                    tracker = DirectChangeTracker(
                        token=token,
                        db=client_db,
                        lookback_days=effective_lookback,
                        compare_conversions=profile_config['compare_conversions'],
                        cost_tolerance=profile_config['cost_tolerance'],
                        skip_cost_check_clients=profile_config['skip_cost_check_clients'],
                    )

                    try:
                        changes_info = await tracker.detect_changes(client_login)
                    except Exception as exc:
                        error_text = str(exc)
                        logging.error("%s: change detection failed — %s", client_login, error_text)
                        return {
                            "type": "failed",
                            "login": client_login,
                            "profile": profile,
                            "source": source,
                            "error": error_text,
                        }

                    days_to_update = changes_info.get("days_to_update") or []
                    if not days_to_update:
                        return {
                            "type": "no_changes",
                            "login": client_login,
                            "profile": profile,
                            "source": source,
                        }

                    client_jobs = []
                    ranges = convert_days_to_date_ranges(days_to_update)
                    for start_str, end_str in ranges:
                        try:
                            datetime.strptime(start_str, "%Y-%m-%d")
                            datetime.strptime(end_str, "%Y-%m-%d")
                        except ValueError:
                            logging.warning("%s: invalid date range %s-%s, skipping.", client_login, start_str, end_str)
                            continue
                        client_jobs.append(
                            DirectReloadJob(
                                login=client_login,
                                token=token,
                                start_date=start_str,
                                end_date=end_str,
                                profile=profile,
                                source=source,
                            )
                        )

                    return {
                        "type": "jobs",
                        "jobs": client_jobs,
                    }
                finally:
                    await client_db.close_engine()

        logging.info(
            "Starting parallel change detection for %d clients (max %d concurrent)",
            len(login_tokens),
            MAX_CONCURRENT_LOGINS,
        )

        tasks = [
            detect_changes_for_client(client_login, token)
            for client_login, token in login_tokens.items()
        ]
        results = await asyncio.gather(*tasks)

        for result in results:
            if result["type"] == "failed":
                failed_clients.append({
                    "login": result["login"],
                    "profile": result["profile"],
                    "source": result["source"],
                    "error": result["error"],
                })
            elif result["type"] == "no_changes":
                clients_without_changes.append({
                    "login": result["login"],
                    "profile": result["profile"],
                    "source": result["source"],
                })
            elif result["type"] == "jobs":
                jobs.extend(result["jobs"])

        logging.info(
            "Change tracker planned %d reload jobs; clients without changes: %s",
            len(jobs),
            [item["login"] for item in clients_without_changes],
        )
        return jobs, clients_without_changes, failed_clients
    finally:
        await access_db.close_engine()
        if created_db:
            await target_db.close_engine()


__all__ = ["DirectReloadJob", "plan_direct_reload_jobs"]
