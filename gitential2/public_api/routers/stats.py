# pylint: skip-file
from typing import List, Dict, Any, Optional
import datetime as dt

from fastapi import APIRouter, Depends

from gitential2.datatypes.stats import StatsRequest
from gitential2.datatypes.permissions import Entity, Action
from gitential2.core import collect_stats, GitentialContext, check_permission
from ..dependencies import gitential_context, current_user

router = APIRouter(tags=["metrics"])


@router.post("/workspaces/{workspace_id}/stats")
async def workspace_stats(
    val: StatsRequest,
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)

    return collect_stats(g, workspace_id, val)


@router.post("/workspaces/{workspace_id}/multi-stats")
async def workspace_multi_stats(
    stats_request: Dict[str, StatsRequest],
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)

    ret: dict = {}
    for name, val in stats_request.items():
        result = collect_stats(g, workspace_id, val)
        ret[name] = result
    return ret


@router.get("/workspaces/{workspace_id}/projects/{project_id}/developers")
async def developers(orient: str, limit: int):
    return [{"name": "Béla", "email": "test@test.hu"}, {"name": "Józsi", "email": "test2@test.hu"}]


# @router.get("/workspaces/{workspace_id}/projects/{project_id}/outlier")
# async def outlier(
#     orient: str,
#     limit: int,
#     _from: str = Query(None, alias="from"),
#     to: str = Query(None, alias="to"),
# ):
#     return {}
# https://gitlab.ops.gitential.com/oauth/authorize?response_type=code&client_id=9b7faac02a309e905637f2491d2151dec834398c9190d62478c13277283121a4&redirect_uri=https%3A%2F%2F2e9af89e042f.ngrok.io%2Fv2%2Fauth%2Fgitlab-second&scope=api+read_repository+email+read_user&state=V8OSsqBeWXTMB9gBorAMym9XI1zez5


# @router.get("/workspaces/{workspace_id}/project-summary")
# async def project_summary(
#     workspace_id: int,
#     _from: str = Query(None, alias="from"),
#     to: str = Query(None, alias="to"),
#     orient: str = "records",
# ):
#     return []

#     data = {
#         "prevSum": [
#             {
#                 "project_name": "Project2",
#                 "project_id": 2507,
#                 "hours": 17.383553351,
#                 "loc": 4411.5999884754,
#                 "churn_ratio": 0.9887360683,
#             },
#             {"project_name": "Project3", "project_id": 2512, "hours": 1.58, "loc": 50.7999992371, "churn_ratio": 1.0},
#             {
#                 "project_name": "Project1",
#                 "project_id": 2496,
#                 "hours": 5.2260923638,
#                 "loc": 152.4000041485,
#                 "churn_ratio": 0.8248175182,
#             },
#         ],
#         "actualSum": [
#             {
#                 "project_name": "Project2",
#                 "project_id": 2507,
#                 "hours": 16.3339988157,
#                 "loc": 1552.2000007629,
#                 "churn_ratio": 0.9666439755,
#             },
#             {
#                 "project_name": "Project3",
#                 "project_id": 2512,
#                 "hours": 0.7141666667,
#                 "loc": 7.1999998093,
#                 "churn_ratio": 1.0,
#             },
#             {
#                 "project_name": "Project1",
#                 "project_id": 2496,
#                 "hours": 17.2619693802,
#                 "loc": 1219.20002909,
#                 "churn_ratio": 0.9646946565,
#             },
#         ],
#     }
#     if to == str(dt.date.today()):
#         return data["actualSum"]
#     else:
#         return data["prevSum"]
