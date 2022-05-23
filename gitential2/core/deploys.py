from typing import List, Iterable
from pydantic.datetime_parse import parse_datetime


from gitential2.datatypes.deploys import Deploy, DeployedPullRequest, DeployedCommit, DeployedIssue

from .context import GitentialContext


def get_all_deploys(g: GitentialContext, workspace_id: int) -> Iterable[Deploy]:
    return g.backend.deploys.all(workspace_id)


def register_deploy(g: GitentialContext, workspace_id: int, deploy_json: dict) -> Deploy:

    pull_requests = _transform_to_deployed_pull_request(deploy_json)
    commits = _transform_to_deployed_commit(deploy_json)
    issues = _transform_to_deployed_issue(deploy_json)

    deploy_obj = _transform_to_deploy(
        deploy_json=deploy_json,
        pull_requests=pull_requests,
        commits=commits,
        issues=issues,
    )

    deploy = g.backend.deploys.create(workspace_id=workspace_id, obj=deploy_obj)
    return deploy


def _transform_to_deployed_pull_request(deploy_json: dict) -> List[DeployedPullRequest]:
    pull_requests = deploy_json.get("pull_requests")
    ret = []
    if pull_requests:
        for single_pull_request in pull_requests:
            ret.append(
                DeployedPullRequest(
                    id=single_pull_request["pr_id"],
                    repo_id=single_pull_request["repo_id"],
                    number=single_pull_request.get("number"),
                    repo_name=single_pull_request.get("repo_name"),
                    title=single_pull_request.get("title"),
                    created_at=parse_datetime(single_pull_request["created_at"]),
                    merged_at=parse_datetime(single_pull_request["merged_at"])
                    if single_pull_request.get("merged_at")
                    else None,
                )
            )
        return ret
    return ret


def _transform_to_deployed_commit(deploy_json: dict) -> List[DeployedCommit]:
    commits = deploy_json.get("commits")
    ret = []
    if commits:
        for single_commit in commits:
            ret.append(DeployedCommit(**single_commit))
        return ret
    return ret


def _transform_to_deployed_issue(deploy_json: dict) -> List[DeployedIssue]:
    issues = deploy_json.get("issues")
    ret = []
    if issues:
        for single_issue in issues:
            ret.append(DeployedIssue(**single_issue))
        return ret
    return ret


def _transform_to_deploy(
    deploy_json: dict,
    pull_requests: List[DeployedPullRequest],
    commits: List[DeployedCommit],
    issues: List[DeployedIssue],
) -> Deploy:

    _deployed_at = deploy_json["deployed_at"]

    return Deploy(
        id=deploy_json["id"],
        pull_requests=pull_requests,
        commits=commits,
        issues=issues,
        environment=deploy_json["environment"],
        deployed_at=parse_datetime(_deployed_at),
    )
