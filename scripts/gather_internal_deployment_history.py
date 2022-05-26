import os
import sys
from pathlib import Path
from datetime import timezone, timedelta, datetime
from typing import Tuple, List, Optional
import pygit2
from pygit2 import Repository

from gitential2.cli_v2.common import print_results, OutputFormat
from gitential2.datatypes.deploys import Deploy, DeployedCommit


def _parse_repo_name(repo_label: str) -> str:

    repo_assignment = {
        "backend_version": "gitential2",
        "frontend_version": "catwalk2",
        "frontend_v2_version": "gitential-front-end",
        "chart_version": "helm-chart",
    }
    repo_name = repo_assignment.get(repo_label)

    return repo_name if repo_name else "untracked"


def _parse_patch(patch: str, commit) -> Optional[Deploy]:

    if not patch:
        return None

    addition = "+"
    # deletion = "-"
    environments = []
    repositories = []
    environment_addition = "+++ b"
    repostory_labels = ["+frontend_version", "+backend_version", "+frontend_v2_version", "+chart_version"]

    splitted_patch = patch.splitlines()
    for line in splitted_patch:
        # if line[0] in [deletion, addition]:
        if line[0] in [addition]:
            if line.startswith(environment_addition):
                splitted_line = line.split("/")
                if splitted_line[-1] == "environment.yaml":
                    environments.append(splitted_line[1])

            for repo_label in repostory_labels:
                if line.startswith(repo_label):
                    splitted_line = line.split(":")
                    repositories.append((_parse_repo_name(splitted_line[0][1:]), splitted_line[1]))

    return _transform_to_deploy(environments, repositories, commit)


def _transform_to_deployed_commits(_commit_id: str, repositories: Tuple[str, str]) -> List[DeployedCommit]:
    ret = []
    for repository in repositories:
        deployed_commit = DeployedCommit(
            commit_id=_commit_id,
            repository_name=repository[0],
            git_ref=repository[1],
        )
        ret.append(deployed_commit)
    return ret


def _transform_to_deploy(environments, repositories, commit) -> Deploy:
    tzinfo = timezone(timedelta(minutes=commit.author.offset))
    _deployed_at = datetime.fromtimestamp(float(commit.author.time), tzinfo)
    _commit_id = str(commit.id)

    deployed_pull_requests = []
    deployed_issues = []
    deployed_commits = _transform_to_deployed_commits(_commit_id, repositories)

    list_of_repo_names = []

    for repo in repositories:
        list_of_repo_names.append(repo[0])

    list_of_repo_names = list(set(list_of_repo_names))

    deploy = Deploy(
        id=f"{_commit_id}X{'Y'.join(environments).split()[0]}" if environments else f"{_commit_id}-untracked_env",
        repositories=list_of_repo_names,
        environments=environments,
        pull_requests=deployed_pull_requests,
        commits=deployed_commits,
        issues=deployed_issues,
        deployed_at=_deployed_at,
    )

    return deploy


def gathering_commits_in_master_branch(path: Path) -> List[Deploy]:
    file_path = os.path.join(path, ".git")
    repo = Repository(file_path)

    for _master in ["master", "main"]:
        master_branch = repo.lookup_branch(_master)
        if master_branch:
            break

    if not master_branch:
        raise Exception("master/main branch not found")

    repo.checkout(master_branch)
    last = repo[repo.head.target]

    commits = list(repo.walk(last.id, pygit2.GIT_SORT_TIME))

    ret = []
    for i, commit in enumerate(commits):
        if i < len(commits) - 1:
            commit_diff = repo.diff(commits[i + 1], commit)
            deploy = _parse_patch(commit_diff.patch, commit)
            if deploy:
                ret.append(deploy)
    return ret


if __name__ == "__main__":
    path = Path(sys.argv[1])  # path to the git repo
    internal_deployment_history = gathering_commits_in_master_branch(path)
    print_results(internal_deployment_history, format_=OutputFormat.json)
