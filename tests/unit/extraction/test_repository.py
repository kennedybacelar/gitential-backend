import os
import pathlib
from datetime import datetime, timezone

import pytest

from gitential2.extraction.repository import (
    clone_repository,
    get_repository_state,
    get_commits,
    extract_commit,
    extract_commit_patches,
    blame_porcelain,
    extract_incremental,
)
from gitential2.extraction.output import DataCollector
from gitential2.datatypes import (
    GitRepository,
    LocalGitRepository,
    GitRepositoryState,
    GitRepositoryStateChange,
    ECommit,
    UserPassCredential,
    KeypairCredentials,
)

TEST_PUBLIC_REPOSITORY = "https://github.com/laco/hostname.git"
TEST_SSH_PRIVATE_REPOSITORY = "git@gitlab.com:gitential-com/test-repository.git"
TEST_HTTPS_PRIVATE_REPOSITORY = "https://gitlab.com/gitential-com/test-repository.git"


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
            ret[name] = LocalGitRepository(repo_id, pathlib.PosixPath(local_path))
        else:
            repo = GitRepository(repo_id, clone_url)
            ret[name] = clone_repository(repository=repo, destination_path=local_path)

    return ret


@pytest.mark.slow
def test_clone_repository_without_credential(tmp_path):
    ret = clone_repository(GitRepository(repo_id=1, clone_url=TEST_PUBLIC_REPOSITORY), destination_path=tmp_path)
    assert isinstance(ret, LocalGitRepository)


@pytest.mark.slow
def test_clone_repository_with_userpass(tmp_path, secrets):
    ret = clone_repository(
        repository=GitRepository(repo_id=1, clone_url=TEST_HTTPS_PRIVATE_REPOSITORY),
        destination_path=tmp_path,
        credentials=UserPassCredential(
            secrets["TEST_HTTPS_PRIVATE_REPOSITORY_USERNAME"], secrets["TEST_HTTPS_PRIVATE_REPOSITORY_PASSWORD"]
        ),
    )
    assert isinstance(ret, LocalGitRepository)


@pytest.mark.skip
@pytest.mark.slow
def test_clone_repository_ssh_with_keypair(tmp_path, secrets):
    ret = clone_repository(
        repository=GitRepository(repo_id=1, clone_url=TEST_SSH_PRIVATE_REPOSITORY),
        destination_path=tmp_path,
        credentials=KeypairCredentials(
            username="git",
            pubkey=secrets["TEST_SSH_PRIVATE_REPOSITORY_PUBLIC_KEY"],
            privkey=secrets["TEST_SSH_PRIVATE_REPOSITORY_PRIVATE_KEY"],
        ),
    )
    assert isinstance(ret, LocalGitRepository)
    state = get_repository_state(ret)
    assert "master" in state.branches


def is_commit_hash(commit_id):
    return isinstance(commit_id, str) and len(commit_id) == 40


def test_get_repository_state(test_repositories):
    state = get_repository_state(test_repositories["flask"])
    assert "master" in state.branches and is_commit_hash(state.branches["master"])
    assert "1.0" in state.tags and state.tags["1.0"] == "291f3c338c4d302dbde01ab9153a7817e5a780f5"


def test_get_commits_fist_time_returns_all_commits(test_repositories):
    commit_ids = list(get_commits(test_repositories["hostname"]))
    assert "26e47101fa3d283ce44dc565c04ecc8e95de3e9d" in commit_ids


def test_get_commits_empty_if_no_changes_have_made(test_repositories):
    repo = test_repositories["flask"]
    state = get_repository_state(repo)
    commit_ids = list(get_commits(repo, previous_state=state, current_state=state))
    assert len(commit_ids) == 0


def test_get_commits_return_only_new_commits(test_repositories):
    repo = test_repositories["hostname"]
    state = get_repository_state(repo)

    commit_ids = list(
        get_commits(
            repo,
            previous_state=GitRepositoryState(branches={"master": "db0f1427e9201b274fc9015455901c0800eb4d3f"}, tags={}),
            current_state=state,
        )
    )
    assert "db0f1427e9201b274fc9015455901c0800eb4d3f" not in commit_ids
    assert "26e47101fa3d283ce44dc565c04ecc8e95de3e9d" in commit_ids


def test_extract_commit(test_repositories):
    repo = test_repositories["flask"]
    # https://github.com/pallets/flask/commit/dc11cdb4a4627b9f8c79e47e39aa7e1357151896
    commit_id = "dc11cdb4a4627b9f8c79e47e39aa7e1357151896"
    output = DataCollector()

    extract_commit(repo, commit_id, output)

    assert "e_commit" in output.values
    res = output.values["e_commit"][0]

    assert res.commit_id == "dc11cdb4a4627b9f8c79e47e39aa7e1357151896"
    assert res.atime == datetime(2020, 11, 5, 9, 0, 57, tzinfo=timezone.utc)
    assert res.aemail == "davidism@gmail.com"
    assert res.aname == "David Lord"

    assert res.ctime == datetime(2020, 11, 5, 9, 27, 52, tzinfo=timezone.utc)
    assert res.cemail == "davidism@gmail.com"
    assert res.cname == "David Lord"

    assert "move send_file and send_from_directory to Werkzeug" in res.message
    assert res.nparents == 1
    assert res.tree_id == "373f9a98e6eb1d1833101396966a7a4440fd5a47"


def _get_first(iterable, condition):
    return next(i for i in iterable if condition(i))


def test_extract_commit_patches_and_rewrites(test_repositories):
    repo = test_repositories["flask"]
    # https://github.com/pallets/flask/commit/dc11cdb4a4627b9f8c79e47e39aa7e1357151896
    commit_id = "253570784cdcc85d82142128ce33e3b9d8f8db14"
    output = DataCollector()

    extract_commit_patches(repo, commit_id, output)

    assert "e_patch" in output.values
    assert "e_patch_rewrite" in output.values

    patches = output.values["e_patch"]
    patch_rewrites = output.values["e_patch_rewrite"]

    cli_p = _get_first(patches, lambda p: p.newpath == "src/flask/cli.py")
    assert cli_p.oldpath == cli_p.newpath == "src/flask/cli.py"
    assert not cli_p.is_binary
    assert cli_p.status == "M"
    assert (cli_p.loc_i, cli_p.loc_d) == (22, 24)
    assert (cli_p.comp_i, cli_p.comp_d) == (46, 58)
    assert (cli_p.loc_i_std, cli_p.loc_d_std) == (0, 0)
    assert (cli_p.comp_i_std, cli_p.comp_d_std) == (0, 0)
    assert (cli_p.oldsize, cli_p.newsize) == (31013, 30906)
    assert (cli_p.nrewrites, cli_p.rewrites_loc) == (6, 24)

    cli_rewrites = [p_r for p_r in patch_rewrites if p_r.newpath == "src/flask/cli.py"]
    assert len(cli_rewrites) == 6
    assert all(r.commit_id == commit_id for r in cli_rewrites)
    assert all(r.newpath == "src/flask/cli.py" for r in cli_rewrites)
    assert {r.rewritten_commit_id: r.loc_d for r in cli_rewrites} == {
        "3bdb90f06b9d3167320180d4a5055dcd949bf72f": 7,
        "5b7fd9ad889e54d4d694d310b559c921d7df75cf": 3,
        "81576c236a4dde33aeed13c84abed0ad2e796c2f": 1,
        "9594876c1f6377a3f8b911d31e8c941295baa281": 1,
        "9641f07d9159cd4289a3b89fbf5cf0bfc0102050": 1,
        "fa6eded6f572dd4bc23b030f025156cdd1e63305": 11,
    }


def test_blame_porcelain(test_repositories):
    repo = test_repositories["flask"]
    result = blame_porcelain(repo.directory, "tests/test_json.py", "373f0dd82e68ab7cac7a77344e715dbe68faebd3")
    assert result[1] == "a0e2aca770c756d9f7de53339e2cf9067a52df11"
    assert result[412] == "6def8a4a489ce1321a44bc1947c5c86620afdb3f"


@pytest.mark.slow
def test_extract_incremental(settings, test_repositories):
    output = DataCollector()
    repository = GitRepository(repo_id=1, clone_url=str(test_repositories["hostname"].directory))
    result = extract_incremental(
        repository=repository,
        output=output,
        settings=settings,
    )
    assert isinstance(result, GitRepositoryState)
    assert "master" in result.branches
    assert "e_commit" in output.values and len(output.values["e_commit"]) >= 2
