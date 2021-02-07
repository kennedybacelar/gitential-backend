# pylint: skip-file
from typing import List, Dict, Any, Optional
import datetime as dt

from pydantic import BaseModel, Field
from fastapi import APIRouter, Query

router = APIRouter(tags=["metrics"])


class StatsRequest(BaseModel):
    metrics: List[str]
    dimensions: List[str]
    filters: Dict[str, Any]
    sort_by: Optional[List[str]] = None
    type: str = "aggregate"  # or "select"


@router.post("/stats")
async def stats(stats_request: StatsRequest):
    print("!!!", stats_request)
    keys = stats_request.metrics + stats_request.dimensions
    if stats_request.sort_by:
        keys = keys + stats_request.sort_by
    return {key: [] for key in keys}


@router.post("/workspaces/{workspace_id}/stats")
async def workspace_stats(stats_request: StatsRequest):
    print("!!!", stats_request)
    keys = stats_request.metrics + stats_request.dimensions
    if stats_request.sort_by:
        keys = keys + stats_request.sort_by
    return {key: [] for key in keys}


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


@router.get("/workspaces/{workspace_id}/project-summary")
async def project_summary(
    workspace_id: int,
    _from: str = Query(None, alias="from"),
    to: str = Query(None, alias="to"),
    orient: str = "records",
):
    data = {
        "prevSum": [
            {
                "project_name": "Project2",
                "project_id": 2507,
                "hours": 17.383553351,
                "loc": 4411.5999884754,
                "churn_ratio": 0.9887360683,
            },
            {"project_name": "Project3", "project_id": 2512, "hours": 1.58, "loc": 50.7999992371, "churn_ratio": 1.0},
            {
                "project_name": "Project1",
                "project_id": 2496,
                "hours": 5.2260923638,
                "loc": 152.4000041485,
                "churn_ratio": 0.8248175182,
            },
        ],
        "actualSum": [
            {
                "project_name": "Project2",
                "project_id": 2507,
                "hours": 16.3339988157,
                "loc": 1552.2000007629,
                "churn_ratio": 0.9666439755,
            },
            {
                "project_name": "Project3",
                "project_id": 2512,
                "hours": 0.7141666667,
                "loc": 7.1999998093,
                "churn_ratio": 1.0,
            },
            {
                "project_name": "Project1",
                "project_id": 2496,
                "hours": 17.2619693802,
                "loc": 1219.20002909,
                "churn_ratio": 0.9646946565,
            },
        ],
    }
    if to == str(dt.date.today()):
        return data["actualSum"]
    else:
        return data["prevSum"]
