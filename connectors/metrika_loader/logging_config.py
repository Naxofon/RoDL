import logging
from pathlib import Path

LOG_FILE = Path(__file__).with_name("metrika_loader.log")


def configure_logging() -> None:
    """Ensure root logging writes to the connector log file."""
    if logging.getLogger().handlers:
        return

    logging.basicConfig(
        filename=str(LOG_FILE),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


__all__ = ["configure_logging", "LOG_FILE"]

configure_logging()