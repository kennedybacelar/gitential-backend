import logging
from pympler import muppy, summary
from structlog import get_logger

logger = get_logger(__name__)


def log_memory_usage(msg: str, limit=20):
    if logger.isEnabledFor(logging.DEBUG):
        all_objects = muppy.get_objects()
        sum1 = summary.summarize(all_objects)
        logger.debug(f"MEM USAGE|{msg}", formatted=list(summary.format_(sum1, limit=limit)))
