import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

LOG_PATH = Path(__file__).with_name("change_tracker.log")


def get_change_tracker_logger() -> logging.Logger:
    logger = logging.getLogger("metrika.change_tracker")
    if logger.handlers:
        return logger

    handler = logging.FileHandler(LOG_PATH)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class AsyncRequestLimiter:
    """Enforce both concurrency and minimal interval between HTTP calls."""

    def __init__(self, max_concurrent: int, min_interval: float) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._min_interval = float(min_interval)
        self._lock = asyncio.Lock()
        self._last_request: float = 0.0

    async def __aenter__(self) -> "AsyncRequestLimiter":
        await self._semaphore.acquire()
        await self._wait_for_slot()
        return self

    async def __aexit__(self, _exc_type, _exc, _tb) -> None:
        self._semaphore.release()

    async def _wait_for_slot(self) -> None:
        while True:
            async with self._lock:
                now = asyncio.get_running_loop().time()
                elapsed = now - self._last_request
                if elapsed >= self._min_interval:
                    self._last_request = now
                    return
                wait_for = self._min_interval - elapsed
            await asyncio.sleep(wait_for)


@dataclass(frozen=True)
class GoalMetadata:
    goal_id: int
    identifier: str


def classify_goals(goals: list[dict]) -> list[GoalMetadata]:
    if not goals:
        return []

    try:
        goals_df = pd.json_normalize(goals)
    except Exception as exc:
        logging.getLogger("metrika.change_tracker").error("Failed to normalise goals payload: %s", exc)
        return []

    required_cols = {"id", "name"}
    if not required_cols.issubset(goals_df.columns):
        logging.getLogger("metrika.change_tracker").warning(
            "Goals payload missing required columns %s. Available: %s",
            required_cols,
            list(goals_df.columns),
        )
        return []

    goals_df = goals_df[["id", "name"]].copy()
    goals_df["id"] = pd.to_numeric(goals_df["id"], errors="coerce")
    goals_df = goals_df.dropna(subset=["id"])
    if goals_df.empty:
        return []

    goals_df["id"] = goals_df["id"].astype(int)
    goals_df["name"] = goals_df["name"].astype(str).str.lower()
    goals_df["name"] = goals_df["name"].fillna("")

    add_condition = goals_df["name"].str.contains("madd")
    goals_df["identifier"] = ""
    goals_df.loc[add_condition, "identifier"] = "g"

    metadata: list[GoalMetadata] = []
    for row in goals_df.itertuples(index=False):
        identifier = getattr(row, "identifier", "") or ""
        identifier = identifier if identifier in ("g",) else "u"
        metadata.append(GoalMetadata(goal_id=int(row.id), identifier=identifier))

    return metadata


__all__ = [
    "AsyncRequestLimiter",
    "GoalMetadata",
    "classify_goals",
    "get_change_tracker_logger",
]
