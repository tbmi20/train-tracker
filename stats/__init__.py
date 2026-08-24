"""Modular reliability stats.

Each sibling module (except ones starting with '_') is a metric, and
exposes:
    METRIC_NAME: str
    compute(db, stat_date: str) -> list[{"scope_type", "scope_value", "value"}]

run_all() discovers every such module automatically and persists its
results via Database.upsert_daily_stat - adding a new metric is "add a
file here", no registration step and no DAG/schema changes needed.
"""

import importlib
import logging
import pkgutil

logger = logging.getLogger(__name__)


def _discover_metric_modules():
    modules = []
    for _, name, _ in pkgutil.iter_modules(__path__):
        if name.startswith("_"):
            continue
        module = importlib.import_module(f"{__name__}.{name}")
        if hasattr(module, "METRIC_NAME") and hasattr(module, "compute"):
            modules.append(module)
        else:
            logger.warning(
                "stats.%s doesn't look like a metric module (missing "
                "METRIC_NAME/compute) - skipping",
                name,
            )
    return modules


def run_all(db, stat_date: str) -> dict[str, int]:
    """Computes and persists every registered metric for stat_date.

    Returns {metric_name: rows_written}, for DAG task logging.
    """
    counts = {}
    for module in _discover_metric_modules():
        rows = module.compute(db, stat_date)
        for row in rows:
            db.upsert_daily_stat(
                module.METRIC_NAME,
                row["scope_type"],
                row["scope_value"],
                stat_date,
                row["value"],
            )
        counts[module.METRIC_NAME] = len(rows)
        logger.info(
            "stats.%s: wrote %d rows for %s", module.METRIC_NAME, len(rows), stat_date
        )
    return counts
