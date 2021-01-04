from enum import Enum
from pydantic import BaseSettings


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warn = "warn"
    error = "error"
    critical = "critical"


class Executor(str, Enum):
    process_pool = "process_pool"
    single_tread = "single_thread"


class GitentialSettings(BaseSettings):
    log_level: LogLevel = LogLevel.info
    executor: Executor = Executor.process_pool
    process_pool_size: int = 8
    show_progress: bool = False
