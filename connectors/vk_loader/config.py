import asyncio

VK_API_BASE_URL = "https://ads.vk.ru/api/v2"
VK_TIMEOUT_SECONDS = 120
VK_MAX_RETRIES = 3
VK_BACKOFF_BASE = 2.0

VK_DEFAULT_LOOKBACK_DAYS = 10


VK_REQUIRED_COLUMNS = [
    'date', 'campaign_name', 'campaign_id', 'group_name', 'group_id',
    'base_shows', 'base_clicks', 'base_spent',
    'base_vk_goals', 'events_clicks_on_external_url', 'events_comments',
    'events_joinings', 'events_launching_video',
    'events_likes', 'events_moving_into_group',
    'events_opening_app', 'events_opening_post',
    'events_sending_form', 'events_shares', 'events_votings',
    'social_network_ok_message', 'social_network_result_join', 'social_network_result_message',
    'social_network_vk_join', 'social_network_vk_message', 'social_network_vk_subscribe',
    'uniques_frequency', 'uniques_increment', 'uniques_total',
    'uniques_video_depth_of_view', 'uniques_video_started',
    'uniques_video_viewed_25_percent', 'uniques_video_viewed_10_seconds',
    'uniques_video_viewed_50_percent', 'uniques_video_viewed_75_percent',
    'uniques_video_viewed_100_percent', 'video_depth_of_view',
    'video_first_second', 'video_paused', 'video_resumed_after_pause',
    'video_sound_turned_off', 'video_sound_turned_on',
    'video_started', 'video_started_cost',
    'video_viewed_10_seconds', 'video_viewed_100_percent',
    'video_viewed_75_percent', 'video_viewed_50_percent',
    'video_viewed_25_percent'
]

VK_INT_COLUMNS = [
    'base_shows', 'base_clicks', 'base_vk_goals',
    'events_clicks_on_external_url', 'events_comments', 'events_joinings',
    'events_launching_video', 'events_likes', 'events_moving_into_group',
    'events_opening_app', 'events_opening_post', 'events_sending_form',
    'events_shares', 'events_votings', 'social_network_ok_message',
    'social_network_result_join', 'social_network_result_message',
    'social_network_vk_join', 'social_network_vk_message',
    'social_network_vk_subscribe', 'uniques_increment', 'uniques_total',
    'video_first_second', 'video_paused', 'video_resumed_after_pause',
    'video_sound_turned_off', 'video_sound_turned_on', 'video_started',
    'video_viewed_10_seconds', 'video_viewed_100_percent', 'video_viewed_75_percent',
    'video_viewed_50_percent', 'video_viewed_25_percent'
]

VK_FLOAT_COLUMNS = [
    'base_spent', 'uniques_video_depth_of_view', 'uniques_frequency',
    'video_depth_of_view', 'video_started_cost'
]

VK_STRING_COLUMNS = [
    'campaign_name', 'campaign_id', 'group_name', 'group_id'
]

_REQUEST_SEM: asyncio.Semaphore | None = None
_REQUEST_SEM_LOOP: asyncio.AbstractEventLoop | None = None


def get_request_semaphore(max_concurrent: int = 5) -> asyncio.Semaphore:
    """Lazily create request semaphore in the current event loop."""
    global _REQUEST_SEM, _REQUEST_SEM_LOOP

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _REQUEST_SEM is None or _REQUEST_SEM_LOOP is not current_loop:
        _REQUEST_SEM = asyncio.Semaphore(max_concurrent) if current_loop else None
        _REQUEST_SEM_LOOP = current_loop

    return _REQUEST_SEM


__all__ = [
    "VK_API_BASE_URL",
    "VK_TIMEOUT_SECONDS",
    "VK_MAX_RETRIES",
    "VK_BACKOFF_BASE",
    "VK_DEFAULT_LOOKBACK_DAYS",
    "VK_REQUIRED_COLUMNS",
    "VK_INT_COLUMNS",
    "VK_FLOAT_COLUMNS",
    "VK_STRING_COLUMNS",
    "get_request_semaphore",
]
