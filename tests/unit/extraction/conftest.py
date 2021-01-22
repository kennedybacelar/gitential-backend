import os
import pathlib
import pytest

from gitential2.datatypes.extraction import LocalGitRepository
from gitential2.datatypes.repositories import GitRepository
from gitential2.extraction.repository import clone_repository


@pytest.fixture(scope="session")
def test_repositories():
    repositories = [
        ("flask", 1, "https://github.com/pallets/flask.git"),
        ("hostname", 2, "https://github.com/laco/hostname.git"),
    ]
    ret = {}
    for name, repo_id, clone_url in repositories:
        local_path = os.path.join("/tmp", name)
        if os.path.isdir(local_path):
            ret[name] = LocalGitRepository(id=repo_id, directory=pathlib.PosixPath(local_path))
        else:
            repo = GitRepository(id=repo_id, clone_url=clone_url)
            ret[name] = clone_repository(repository=repo, destination_path=local_path)

    return ret
