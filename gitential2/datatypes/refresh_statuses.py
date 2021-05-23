from enum import Enum
from datetime import datetime
from typing import Optional, List, Union
from .common import CoreModel


class RefreshCommitsPhase(str, Enum):
    pending = "pending"
    cloning = "clone"
    extract = "extract"
    persist = "persist"
    done = "done"


class LegacyRepositoryStatusStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    finished = "finished"


class LegacyRepositoryRefreshStatus(CoreModel):
    id: int
    name: str
    done: bool = False
    status: LegacyRepositoryStatusStatus = LegacyRepositoryStatusStatus.pending
    error: Optional[List[Union[bool, str]]] = None
    phase: RefreshCommitsPhase = RefreshCommitsPhase.pending
    clone: float = 0.0
    extract: float = 0.0
    persist: float = 0.0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RepositoryRefreshStatus(CoreModel):
    workspace_id: int
    repository_id: int
    repository_name: str
    commits_refresh_scheduled: bool = False

    commits_last_successful_run: Optional[datetime] = None
    commits_started: Optional[datetime] = None
    commits_last_run: Optional[datetime] = None
    commits_error: bool = False
    commits_error_msg: str = ""
    commits_in_progress: bool = False
    commits_phase: RefreshCommitsPhase = RefreshCommitsPhase.pending

    prs_refresh_scheduled: bool = False

    prs_last_successful_run: Optional[datetime] = None
    prs_last_run: Optional[datetime] = None
    prs_in_progress: bool = False
    prs_error: bool = False
    prs_error_msg: str = ""

    def to_legacy(self) -> LegacyRepositoryRefreshStatus:
        legacy_dict = {
            "id": self.repository_id,
            "name": self.repository_name,
            "started_at": self.commits_started,
            "finished_at": self.commits_last_successful_run,
        }
        return LegacyRepositoryRefreshStatus(**legacy_dict)
