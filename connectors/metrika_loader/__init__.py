import warnings

import pandas as pd

from .access import collect_metrika_access_data, counters_from_access
from .jobs import MetrikaReloadJob, plan_metrika_reload_jobs
from .logging_config import configure_logging
from .operations import (
    continue_upload_data_for_counters,
    erase_data_interval_for_all_counters,
    fetch_metrika_job_data,
    refresh_data_with_change_tracker,
    upload_data_for_all_counters,
    upload_data_for_single_counter,
    write_metrika_job_data,
)
from .uploader import YaMetrikaUploader

configure_logging()
pd.set_option("future.no_silent_downcasting", True)
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*DataFrame is highly fragmented.*")

__all__ = [
    "MetrikaReloadJob",
    "YaMetrikaUploader",
    "collect_metrika_access_data",
    "configure_logging",
    "counters_from_access",
    "plan_metrika_reload_jobs",
    "fetch_metrika_job_data",
    "write_metrika_job_data",
    "refresh_data_with_change_tracker",
    "upload_data_for_all_counters",
    "upload_data_for_single_counter",
    "continue_upload_data_for_counters",
    "erase_data_interval_for_all_counters",
]
