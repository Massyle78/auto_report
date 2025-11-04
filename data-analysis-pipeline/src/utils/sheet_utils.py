import re


def safe_sheet_name(name: str) -> str:
    """Return an Excel-safe sheet name (<=31 chars, no []:*?/\\)."""
    sanitized = re.sub(r"[\[\]:\\/*?]", "_", str(name))
    return sanitized[:31] if len(sanitized) > 31 else sanitized


