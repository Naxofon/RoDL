import pandas as pd

from prefect import get_run_logger

pd.set_option("future.no_silent_downcasting", True)


def get_logger():
    return get_run_logger()


__all__ = ["get_logger"]