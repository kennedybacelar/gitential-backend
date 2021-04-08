from collections import defaultdict
from gitential2.datatypes.stats import FilterName, Query, DimensionName, MetricName, QueryType
from .context import GitentialContext
from .stats_v2 import IbisQuery


def get_repos_projects(g: GitentialContext, workspace_id: int) -> dict:
    projects = {p.id: p for p in g.backend.projects.all(workspace_id)}
    project_repos = g.backend.project_repositories.all(workspace_id)
    ret = defaultdict(list)
    for pr in project_repos:
        p = projects.get(pr.project_id)
        if p:
            ret[pr.repo_id].append({"id": pr.repo_id, "project_id": p.id, "project_name": p.name})
    return ret


def get_repo_top_devs(g: GitentialContext, workspace_id: int) -> dict:
    query = Query(
        dimensions=[
            DimensionName.name,
            DimensionName.email,
            DimensionName.repo_id,
        ],
        filters={FilterName.is_merge: False},
        metrics=[MetricName.count_commits],
        type=QueryType.aggregate,
    )
    result = IbisQuery(g, workspace_id, query).execute()
    print(result.values.groupby(result.values["repo_id"]))
    # print(result.values["repo_id"])
    return {
        "categories": [],
        "series": {
            "emails": [],
            "names": [],
        },
    }


def get_dev_top_repos(g: GitentialContext, workspace_id: int) -> dict:
    query = Query(
        dimensions=[
            DimensionName.name,
            DimensionName.email,
            DimensionName.repo_id,
        ],
        filters={FilterName.is_merge: False},
        metrics=[MetricName.count_commits],
        type=QueryType.aggregate,
    )
    result = IbisQuery(g, workspace_id, query).execute()
    print(result.values.groupby(result.values["repo_id"]))
    return {
        "categories": [],
        "series": {
            "repo_ids": [],
            "repo_names": [],
        },
    }


def get_developers(g: GitentialContext, workspace_id: int) -> list:
    return [{"name": dev.name, "email": dev.email} for dev in g.backend.authors.all(workspace_id) if dev.active]


def get_dev_related_projects(g: GitentialContext, workspace_id: int) -> dict:
    query = Query(
        dimensions=[
            DimensionName.name,
            DimensionName.email,
            DimensionName.repo_id,
        ],
        filters={FilterName.is_merge: False},
        metrics=[MetricName.count_commits],
        type=QueryType.aggregate,
    )
    result = IbisQuery(g, workspace_id, query).execute()
    print(result.values.groupby(result.values["repo_id"]))
    return {
        "categories": [],
        "series": {
            "project_ids": [],
            "project_names": [],
        },
    }
