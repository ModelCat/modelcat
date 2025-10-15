from datetime import datetime, timezone
from enum import Enum
from importlib import metadata as ilmd


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


def resolve_version(name: str) -> str:
    try:
        # Prefer installed distribution metadata
        return ilmd.version(name)
    except ilmd.PackageNotFoundError:
        # Fallback: try module __version__ (useful in editable/dev mode)
        try:
            mod = __import__(name)
            return getattr(mod, "__version__", "unknown")
        except Exception:
            return "unknown"
