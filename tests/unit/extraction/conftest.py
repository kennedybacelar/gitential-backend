import os
import pathlib
import pytest

from gitential2.datatypes.extraction import LocalGitRepository
from gitential2.datatypes.repositories import RepositoryInDB, GitProtocol
from gitential2.extraction.repository import clone_repository


@pytest.fixture(scope="session")
def test_repositories():
    repositories = [
        ("flask", 1, "https://github.com/pallets/flask.git", GitProtocol.https),
        ("hostname", 2, "https://github.com/laco/hostname.git", GitProtocol.https),
    ]
    ret = {}
    for name, repo_id, clone_url, protocol in repositories:
        local_path = os.path.join("/tmp", name)
        if os.path.isdir(local_path):
            ret[name] = LocalGitRepository(repo_id=repo_id, directory=pathlib.PosixPath(local_path))
        else:
            repo = RepositoryInDB(id=repo_id, clone_url=clone_url, protocol=protocol)
            ret[name] = clone_repository(repository=repo, destination_path=local_path)

    return ret
