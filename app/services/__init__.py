"""Business services for Untatiz."""

from app.services.data_loader import (
    load_data,
)
from app.services.war_calculator import (
    get_war,
    calculate_fa_war,
)
from app.services.notification import (
    generate_news,
    send_discord_webhook,
    notify_update_complete,
    check_and_notify,
)

__all__ = [
    # Data Loader
    "load_data",
    # WAR Calculator
    "get_war",
    "calculate_fa_war",
    # Notification
    "generate_news",
    "send_discord_webhook",
    "notify_update_complete",
    "check_and_notify",
]
