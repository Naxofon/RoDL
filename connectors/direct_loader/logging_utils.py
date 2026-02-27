import logging
import os
from pathlib import Path

import pandas as pd

current_dir = Path(__file__).resolve().parent
file_logger = logging.getLogger("direct_loader")
file_handler = logging.FileHandler(os.path.join(current_dir, "loader.log"))
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
file_logger.addHandler(file_handler)
file_logger.setLevel(logging.INFO)

pd.set_option("future.no_silent_downcasting", True)


def get_logger():
    """Get Prefect logger if available, otherwise use file logger."""
    try:
        from prefect import get_run_logger
        from prefect.context import MissingContextError

        return get_run_logger()
    except (ImportError, MissingContextError, RuntimeError):
        return file_logger


__all__ = ["get_logger", "file_logger"]