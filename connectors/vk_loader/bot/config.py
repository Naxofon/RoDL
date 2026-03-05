import os


DEFAULT_DEPLOYMENT_VK = "vk-loader-clickhouse/vk-loader-daily"

PREFECT_DEPLOYMENT_VK = os.getenv(
    "PREFECT_DEPLOYMENT_VK",
    DEFAULT_DEPLOYMENT_VK,
)
