"""Shared helpers for stats modules.

Leading underscore keeps stats/__init__.py's auto-discovery from treating
this as a metric module itself.
"""


def hhmm_to_minutes(value: str | None) -> int | None:
    """Parses a Darwin/CIF time string ('HHMM', possibly 'HH:MM') into
    minutes since midnight. Returns None for anything that doesn't parse -
    callers should skip the row rather than guess at a bad value.
    """
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) < 4:
        return None
    hours, minutes = int(digits[:2]), int(digits[2:4])
    if not (0 <= hours < 24 and 0 <= minutes < 60):
        return None
    return hours * 60 + minutes


def delay_minutes(planned: str | None, observed: str | None) -> float | None:
    """observed - planned, in minutes, wrapped to the shorter side of
    midnight (a train scheduled 23:58 and observed at 00:02 is 4 minutes
    late, not -1436).
    """
    planned_m = hhmm_to_minutes(planned)
    observed_m = hhmm_to_minutes(observed)
    if planned_m is None or observed_m is None:
        return None

    diff = observed_m - planned_m
    if diff > 12 * 60:
        diff -= 24 * 60
    elif diff < -12 * 60:
        diff += 24 * 60
    return diff
