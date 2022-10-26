from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from structlog import get_logger

from ..dependencies import current_user, gitential_context
from ...core import GitentialContext
from ...core.refresh_v2 import refresh_repository
from ...datatypes.refresh import RefreshStrategy, RefreshType

logger = get_logger(__name__)

router = APIRouter(tags=["testing"])


class TestingRefreshRepo(BaseModel):
    wid: int
    rid: int
    force: Optional[bool] = False
    strategy: Optional[RefreshStrategy] = RefreshStrategy.one_by_one
    refresh_type: Optional[RefreshType] = RefreshType.commits_only


@router.post("/testing/refresh-repo")
def workspaces(
    testing_refresh_repo: TestingRefreshRepo,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    logger.info("Testing refresh repo endpoint called.", testing_refresh_repo=testing_refresh_repo)
    return refresh_repository(
        g=g,
        workspace_id=testing_refresh_repo.wid,
        repository_id=testing_refresh_repo.rid,
        strategy=testing_refresh_repo.strategy,
        refresh_type=testing_refresh_repo.refresh_type,
        force=testing_refresh_repo.force,
    )
