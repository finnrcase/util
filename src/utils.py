from datetime import datetime


def parse_datetime(dt_string: str):
    """
    Convert string to datetime object.
    """
    return datetime.fromisoformat(dt_string)