from typing import Union
from datetime import datetime, timezone


def normalise_to_unix_ts(value: Union[int, float, datetime] | None) -> int:
    """
    Converts user input (datetime or Unix timestamp) into a UTC Unix timestamp (int).

    - If `value` is int/float: assumed to be a Unix timestamp in seconds.
    - If `value` is datetime: converted to UTC, made timezone-aware, then converted to Unix ts.
    """
    if not value:
        raise ValueError("")
    
    if isinstance(value, (int, float)):
        return int(value)
    
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)

        return int(value.timestamp())


    raise TypeError("Expected int, float, or datetime.")
