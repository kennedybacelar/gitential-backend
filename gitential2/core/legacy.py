from typing import List, Optional
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


def _get_commit_counts_by_dev_and_repo(
    g: GitentialContext,
    workspace_id: int,
    project_id: Optional[int] = None,
    repo_id: Optional[int] = None,
    from_: Optional[str] = None,
    to_: Optional[str] = None,
):
    query = Query(
        dimensions=[
            DimensionName.aid,
            DimensionName.repo_id,
        ],
        filters={FilterName.is_merge: False, FilterName.active: False},
        metrics=[MetricName.count_commits],
        type=QueryType.aggregate,
        sort_by=[["aid", True], ["count_commits", False]],
    )
    if project_id:
        query.filters[FilterName.project_id] = project_id
    if repo_id:
        query.filters[FilterName.repo_ids] = [repo_id]
    if from_ and to_:
        query.filters[FilterName.day] = [from_, to_]
    result = IbisQuery(g, workspace_id, query).execute()
    return result.values.to_dict("records")


def get_repo_top_devs(g: GitentialContext, workspace_id: int) -> dict:
    dev_repo_commit_counts = _get_commit_counts_by_dev_and_repo(g, workspace_id)
    author_emails_names = {
        author.id: (author.email, author.name) for author in g.backend.authors.all(workspace_id) if author.active
    }
    results: dict = defaultdict(lambda: {"emails": [], "names": []})

    for row in dev_repo_commit_counts:
        if row["aid"] in author_emails_names:
            email, name = author_emails_names[row["aid"]]

            results[row["repo_id"]]["emails"].append(email)
            results[row["repo_id"]]["names"].append(name)

    return _to_categories_series_response(results, series_names=["emails", "names"])


def get_dev_top_repos(g: GitentialContext, workspace_id: int) -> dict:
    dev_repo_commit_counts = _get_commit_counts_by_dev_and_repo(g, workspace_id)
    author_emails = {author.id: author.email for author in g.backend.authors.all(workspace_id) if author.active}
    repository_names = {repo.id: repo.name for repo in g.backend.repositories.all(workspace_id)}

    results: dict = defaultdict(lambda: {"repo_ids": [], "repo_names": []})

    for row in dev_repo_commit_counts:
        if row["aid"] in author_emails and row["repo_id"] in repository_names:
            results[author_emails[row["aid"]]]["repo_ids"].append(row["repo_id"])
            results[author_emails[row["aid"]]]["repo_names"].append(repository_names[row["repo_id"]])

    return _to_categories_series_response(results, series_names=["repo_ids", "repo_names"])


def get_dev_related_projects(g: GitentialContext, workspace_id: int) -> dict:
    dev_repo_commit_counts = _get_commit_counts_by_dev_and_repo(g, workspace_id)
    author_emails = {author.id: author.email for author in g.backend.authors.all(workspace_id) if author.active}
    project_names = {project.id: project.name for project in g.backend.projects.all(workspace_id)}
    repos_to_projects = defaultdict(list)

    for project_repo in g.backend.project_repositories.all(workspace_id):
        repos_to_projects[project_repo.repo_id].append(project_repo.project_id)

    results: dict = defaultdict(lambda: {"project_ids": [], "project_names": []})

    for row in dev_repo_commit_counts:
        if row["aid"] in author_emails and row["repo_id"] in repos_to_projects:
            project_ids = repos_to_projects[row["repo_id"]]
            for project_id in project_ids:
                if project_id not in results[author_emails[row["aid"]]]["project_ids"] and project_id in project_names:
                    results[author_emails[row["aid"]]]["project_ids"].append(project_id)
                    results[author_emails[row["aid"]]]["project_names"].append(project_names[project_id])

    return _to_categories_series_response(results, series_names=["project_ids", "project_names"])


def _to_categories_series_response(result_dict: dict, series_names: List[str]):
    ret: dict = {"categories": [], "series": {}}

    for series_name in series_names:
        ret["series"][series_name] = []

    for key, value in result_dict.items():
        ret["categories"].append(key)
        for series_name in series_names:
            ret["series"][series_name].append(",".join(str(v) for v in value[series_name]))

    return ret


def get_developers(
    g: GitentialContext,
    workspace_id: int,
    project_id: Optional[int] = None,
    repo_id: Optional[int] = None,
    from_: Optional[str] = None,
    to_: Optional[str] = None,
) -> list:
    all_active_developers = {
        dev.id: {"name": dev.name, "email": dev.email} for dev in g.backend.authors.all(workspace_id) if dev.active
    }

    if project_id or repo_id:
        dev_repo_commit_counts = _get_commit_counts_by_dev_and_repo(
            g, workspace_id, project_id=project_id, repo_id=repo_id, from_=from_, to_=to_
        )
        print(dev_repo_commit_counts)
        ret = []
        aids = set(row["aid"] for row in dev_repo_commit_counts)
        for aid in aids:
            if aid in all_active_developers:
                ret.append(all_active_developers[aid])
        return ret
    else:
        return list(all_active_developers.values())
