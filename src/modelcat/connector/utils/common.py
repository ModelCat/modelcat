from datetime import datetime, timezone

from enum import Enum


class UserChoice(Enum):
    YES = "y"
    NO = "n"
    PROMPT = "p"


def format_local_datetime(dt: datetime, fmt: str = "%B %d, %Y %H:%M:%S") -> str:
    """
    Convert a UTC datetime to the user's local time and format it nicely.

    Args:
        dt (datetime): A datetime object (UTC or naive).
        fmt (str): Format string for strftime. Default: "January 2, 2025 12:04:10".

    Returns:
        str: Formatted datetime string in the local timezone.
    """
    if dt.tzinfo is None:
        # Assume naive datetimes are in UTC
        dt = dt.replace(tzinfo=timezone.utc)
    local_dt = dt.astimezone()  # converts to system local timezone
    return local_dt.strftime(fmt)
