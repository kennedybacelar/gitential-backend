import os
from pathlib import Path
from datetime import timezone, timedelta, datetime
from typing import Tuple, List, Set
import pygit2
import shutil
import subprocess
import tempfile

from pygit2 import Repository
from contextlib import contextmanager

from gitential2.cli_v2.common import validate_directory_exists
from gitential2.export.exporters import JSONExporter
from gitential2.datatypes.deploys import Deploy, DeployedCommit


REPO_CLONE_URLS = {
    "gitential2": "git@gitlab.com:gitential-com/gitential2.git",
    "catwalk2": "git@gitlab.com:gitential-com/catwalk2.git",
    "gitential-front-end": "git@gitlab.com:gitential-com/gitential-front-end.git",
    "helm-chart": "git@gitlab.com:gitential-com/helm-chart.git",
}

ALL_COMMITS_PATH = "tempdir_all_commits/"


def _parse_repo_name(repo_label: str) -> str:

    repo_assignment = {
        "backend_version": "gitential2",
        "frontend_version": "catwalk2",
        "frontend_v2_version": "gitential-front-end",
        "chart_version": "helm-chart",
    }
    repo_name = repo_assignment.get(repo_label)

    return repo_name if repo_name else "untracked"


@contextmanager
def set_directory(path: Path):
    origin = Path().absolute()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)


def clone_and_save():
    if not os.path.exists(ALL_COMMITS_PATH):
        os.mkdir(ALL_COMMITS_PATH)
    for repo, clone_repo_url in REPO_CLONE_URLS.items():
        dirpath = tempfile.mkdtemp()
        subprocess.run(f"git clone {clone_repo_url} {dirpath}", shell=True, check=True)
        with set_directory(dirpath):
            git_all_commit_hashes_by_repo = (
                subprocess.run("git rev-list --all", shell=True, stdout=subprocess.PIPE, check=True)
                .stdout.decode()
                .strip()
            )
        shutil.rmtree(dirpath)
        with open(os.path.join(ALL_COMMITS_PATH, f"{repo}.txt"), "w+", encoding="utf-8") as f:
            f.write(git_all_commit_hashes_by_repo)


def get_list_of_commits_vs_commit_patches(path: Path) -> List[tuple]:
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

    list_of_commits_and_commit_patches = []
    for i, commit in enumerate(commits):
        if i < len(commits) - 1:
            commit_diff = repo.diff(commits[i + 1], commit)
            list_of_commits_and_commit_patches.append((commit, commit_diff.patch))
    return list_of_commits_and_commit_patches


def get_environments_and_repo_info_by_commit_patch(patch: str):

    if not patch:
        return None

    addition = "+"
    environments: List[str] = []
    repo_names_and_short_commit_ids = []
    environment_addition = "+++ b"
    repository_labels = [
        "+frontend_version",
        "+backend_version",
        "+frontend_v2_version",
        "+chart_version",
    ]

    splitted_patch = patch.splitlines()
    for line in splitted_patch:
        if line[0] in [addition]:
            line = line.strip()
            if line.startswith(environment_addition):
                splitted_line = line.split("/")
                if splitted_line[-1] == "environment.yaml":
                    environments.append(splitted_line[1])

            for repo_label in repository_labels:
                if line.startswith(repo_label):
                    splitted_line = line.split(":")
                    repo_names_and_short_commit_ids.append((_parse_repo_name(splitted_line[0][1:]), splitted_line[1]))
    if not repo_names_and_short_commit_ids:
        repo_names_and_short_commit_ids = []
    return environments, list(set(repo_names_and_short_commit_ids))


def _getting_full_commit_id(repo_name: str, short_commit_id):
    repo_file_name = f"{ALL_COMMITS_PATH}{repo_name}.txt"
    with open(repo_file_name, encoding="utf-8") as file:
        for line in file.readlines():
            if line.strip().startswith(short_commit_id.strip()):
                return line.strip()


def _filling_commit_info(repo_names_and_short_commit_ids: Set[Tuple[str, str]]) -> List[dict]:
    repos_in_commit = []
    for repo_info in repo_names_and_short_commit_ids:

        short_commit_id = repo_info[1].strip()

        deployed_commit = DeployedCommit(
            repository_name=repo_info[0],
            repository_namespace=f"gitential-com/{repo_info[0]}",
            repository_url=f"https://gitlab.com/gitential-com/{repo_info[0]}",
            commit_id=_getting_full_commit_id(repo_info[0], short_commit_id),
            git_ref=short_commit_id,
        )
        repos_in_commit.append(deployed_commit)
    return repos_in_commit


def _transform_to_deploy_object(commit, environments, repos_in_commit) -> Deploy:

    tzinfo = timezone(timedelta(minutes=commit.author.offset))
    _deployed_at = datetime.fromtimestamp(float(commit.author.time), tzinfo)
    commit_id_of_environments_repo = str(commit.id)

    deploy = Deploy(
        id=f"deploy_{commit_id_of_environments_repo}"
        if environments
        else f"{commit_id_of_environments_repo}-untracked_env",
        environments=environments,
        pull_requests=[],
        commits=repos_in_commit,
        issues=[],
        deployed_at=_deployed_at,
    )

    return deploy


def gathering_internal_deployment_history(path: Path):

    validate_directory_exists(Path(path))
    clone_and_save()
    list_of_commits_and_commit_patches = get_list_of_commits_vs_commit_patches(path)
    all_deploys = []
    for commit, commit_patch in list_of_commits_and_commit_patches:
        if not commit_patch:
            continue
        environments, repo_names_and_short_commit_ids = get_environments_and_repo_info_by_commit_patch(commit_patch)
        if not repo_names_and_short_commit_ids:
            continue
        repos_in_commit = _filling_commit_info(repo_names_and_short_commit_ids)
        deploy_json_body = _transform_to_deploy_object(commit, environments, repos_in_commit)
        all_deploys.append(deploy_json_body)
    shutil.rmtree(ALL_COMMITS_PATH)
    return all_deploys


def exporting_internal_deployment_history_into_json_file(repo_source_path: Path, destination_path: Path = ""):

    validate_directory_exists(Path(repo_source_path))
    validate_directory_exists(Path(destination_path))

    prefix = "dep_"
    deployment_data = gathering_internal_deployment_history(path=repo_source_path)

    exporter = JSONExporter(destination_path, prefix)

    for single in deployment_data:
        exporter.export_object(single)
