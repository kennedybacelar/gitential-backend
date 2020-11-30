from typing import Optional, Dict, Generator
from collections import defaultdict
from pathlib import Path
import subprocess
import math
import re
import numpy as np
import pygit2
from structlog import get_logger

from gitential2.datatypes import (
    GitRepository,
    RepositoryCredentials,
    LocalGitRepository,
    GitRepositoryState,
    ECommit,
    EPatch,
    EPatchRewrite,
    Kind,
    UserPassCredential,
    KeypairCredentials,
)
from gitential2.extraction.output import OutputHandler
from gitential2.utils.tempdir import TemporaryDirectory
from gitential2.utils.timer import Timer
from gitential2.utils.executors import ProcessPoolExecutor


logger = get_logger(__name__)


# https://stackoverflow.com/questions/40883798/how-to-get-git-diff-of-the-first-commit
EMPTY_TREE_HASH = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
COMMIT_ID_RE = re.compile(r"^\w{40}")


def extract_incremental(
    clone_url: str,
    output: OutputHandler,
    credentials: Optional[RepositoryCredentials] = None,
    previous_state: Optional[GitRepositoryState] = None,
):
    with TemporaryDirectory() as workdir:
        local_repo = clone_repository(clone_url, destination_path=workdir.path, credentials=credentials)
        current_state = get_repository_state(local_repo)
        commits = get_commits(local_repo, previous_state=previous_state, current_state=current_state)

        executor = ProcessPoolExecutor(local_repo=local_repo, output=output, description="Extracting commits")
        executor.map(fn=_extract_single_commit, items=commits)

        return current_state


def _extract_single_commit(commit_id, local_repo: LocalGitRepository, output: OutputHandler):
    extract_commit(local_repo, commit_id, output)
    extract_commit_patches(local_repo, commit_id, output)


def clone_repository(
    repository: GitRepository, destination_path: Path, credentials: Optional[RepositoryCredentials] = None
) -> LocalGitRepository:
    with TemporaryDirectory() as workdir:

        def _construct_callbacks(credentials):
            if isinstance(credentials, UserPassCredential):
                userpass = pygit2.UserPass(username=credentials.username, password=credentials.password)
                return pygit2.RemoteCallbacks(credentials=userpass)
            elif isinstance(credentials, KeypairCredentials):
                keypair = pygit2.Keypair(
                    username=credentials.username,
                    pubkey=workdir.new_file(credentials.pubkey),
                    privkey=workdir.new_file(credentials.privkey),
                    passphrase=credentials.passphrase,
                )
                return pygit2.RemoteCallbacks(credentials=keypair)
            return None

        pygit2.clone_repository(
            url=repository.clone_url, path=str(destination_path), callbacks=_construct_callbacks(credentials)
        )
        return LocalGitRepository(repository.repo_id, destination_path)


def _git2_repo(repository: LocalGitRepository) -> pygit2.Repository:
    return pygit2.Repository(str(repository.directory))


def get_repository_state(repository: LocalGitRepository) -> GitRepositoryState:
    g2_repo = _git2_repo(repository)

    def _get_references(from_, prefix, skip=None) -> Dict[str, str]:
        ret = {}
        skip = skip or []
        for ref in from_:
            if ref.startswith(prefix) and (ref not in skip):
                commit, _ = g2_repo.resolve_refish(ref)
                name = ref.replace(prefix, "")
                ret[name] = str(commit.id)
        return ret

    return GitRepositoryState(
        branches=_get_references(g2_repo.branches.remote, prefix="origin/", skip=["origin/HEAD"]),
        tags=_get_references(g2_repo.references, prefix="refs/tags/"),
    )


def get_commits(
    repository: LocalGitRepository,
    previous_state: Optional[GitRepositoryState] = None,
    current_state: Optional[GitRepositoryState] = None,
) -> Generator[str, None, None]:

    current_state = current_state or get_repository_state(repository)
    previous_state = previous_state or GitRepositoryState(branches={}, tags={})
    heads = current_state.commit_ids
    tails = previous_state.commit_ids
    g2_repo = _git2_repo(repository)
    commits_already_yielded = set()

    for head in heads:
        walker = g2_repo.walk(head, pygit2.GIT_SORT_TOPOLOGICAL)  #  | pygit2.GIT_SORT_REVERSE)

        # Ignore the tails and ancestors
        for tail in tails:
            walker.hide(tail)

        # Collect all commits
        for commit in walker:

            # If we saw the commit before, we already know it's ancestors
            if str(commit.id) in commits_already_yielded:
                walker.hide(commit.id)
            else:
                commits_already_yielded.add(str(commit.id))
                yield str(commit.id)


def extract_commit(repository: LocalGitRepository, commit_id: str, output: OutputHandler):
    g2_repo = _git2_repo(repository)
    commit = g2_repo.get(commit_id)
    atime = _utc_timestamp_for(commit.author)
    ctime = _utc_timestamp_for(commit.committer)

    output.write(
        Kind.E_COMMIT.value,
        ECommit(
            commit_id=commit.id.hex,
            atime=atime,
            aemail=commit.author.email,
            aname=commit.author.name,
            ctime=ctime,
            cemail=commit.committer.email,
            cname=commit.committer.name,
            message=commit.message,
            nparents=len(commit.parent_ids),
            tree_id=str(commit.tree_id),
        ),
    )


def extract_commit_patches(repository: LocalGitRepository, commit_id: str, output: OutputHandler, **kwargs):
    g2_repo = kwargs.get("g2_repo") or _git2_repo(repository)
    commit = kwargs.get("commit") or g2_repo.get(commit_id)

    def _get_parents_and_diffs():
        parents = [g2_repo.get(parent_id) for parent_id in commit.parent_ids]
        diffs = [g2_repo.diff(parent, commit) for parent in parents]
        if parents:
            return parents, diffs
        else:
            # Initial commit, use the empty tree
            return [g2_repo.get(EMPTY_TREE_HASH)], [g2_repo.diff(EMPTY_TREE_HASH, commit)]

    parents, diffs = _get_parents_and_diffs()

    patch_count = 0
    for parent, diff in zip(parents, diffs):
        for patch in diff:
            _extract_patch(commit, parent, patch, g2_repo, output)
            patch_count += 1
    logger.debug("patch count", patch_count=patch_count, repository=repository, commit_id=commit_id)
    return patch_count


def _extract_patch(commit, parent, patch, g2_repo, output):
    patch_stats = _get_patch_stats(patch)
    nrewrites, rewrites_loc = _extract_patch_rewrites(commit, parent, patch, g2_repo, output)
    output.write(
        Kind.E_PATCH.value,
        EPatch(
            commit_id=commit.id.hex,
            parent_commit_id=parent.id.hex,
            status=patch.delta.status_char(),
            newpath=patch.delta.new_file.path,
            oldpath=patch.delta.old_file.path,
            newsize=patch.delta.new_file.size,
            oldsize=patch.delta.old_file.size,
            is_binary=patch.delta.is_binary,
            lang="todo",
            langtype="todo",
            loc_d=patch_stats[0],
            loc_i=patch_stats[1],
            comp_d=patch_stats[2],
            comp_i=patch_stats[3],
            loc_d_std=patch_stats[4],
            loc_i_std=patch_stats[5],
            comp_d_std=patch_stats[6],
            comp_i_std=patch_stats[7],
            nhunks=len(patch.hunks),
            nrewrites=nrewrites,
            rewrites_loc=rewrites_loc,
        ),
    )


def _get_patch_stats(patch):
    with Timer("_get_patch_stats", threshold_ms=100):
        if len(patch.hunks) == 0:  # pylint: disable=compare-to-zero
            return np.zeros(8)

        addition = "+"
        deletion = "-"
        stats = np.zeros((len(patch.hunks), 4), dtype=np.int32)
        view = stats[:, :]
        for i, hunk in enumerate(patch.hunks):
            for line in hunk.lines:
                if line.origin == deletion:
                    view[i, 0] = view[i, 0] + 1
                    view[i, 2] = view[i, 2] + _indentation(line.raw_content)
                elif line.origin == addition:
                    view[i, 1] = view[i, 1] + 1
                    view[i, 3] = view[i, 3] + _indentation(line.raw_content)
        return np.hstack((stats.sum(axis=0), stats.std(axis=0)))


def _indentation(s, tabsize=4):
    nspaces = 0
    ntabs = 0
    for i in range(len(s)):  # pylint: disable=consider-using-enumerate
        c = s[i]
        if c == 32:
            nspaces += 1
        elif c == 9:
            if nspaces > 0:
                ntabs += math.ceil(nspaces / tabsize)
                nspaces = 0
            ntabs += 1
        else:
            if nspaces > 0:
                ntabs += math.ceil(nspaces / tabsize)
            return ntabs
    return 0


def _extract_patch_rewrites(commit, parent, patch, g2_repo, output):  # pylint: disable=too-complex
    def _is_addition():
        return patch.delta.status_char() == "A"

    def _is_initial_commit():
        return not isinstance(parent, pygit2.Commit)

    def _is_empty_patch():
        return len(patch.hunks) == 0  # pylint: disable=compare-to-zero

    def _is_binary():
        return patch.delta.is_binary

    if _is_addition() or _is_initial_commit() or _is_empty_patch() or _is_binary():
        return 0, 0

    deletion = "-"
    filepath = patch.delta.old_file.path
    rewrites = defaultdict(int)
    with Timer("blame_porcelain", threshold_ms=1000):
        line_blame_dict = blame_porcelain(g2_repo.path, filepath, str(parent.id))

    if not line_blame_dict:
        return 0, 0

    with Timer("calc rewrites", threshold_ms=1000):
        for hunk in patch.hunks:
            for line in hunk.lines:
                if line.origin == deletion:
                    blame_co_id = line_blame_dict[line.old_lineno]
                    rewrites[blame_co_id] += 1

    with Timer("construct output", threshold_ms=1000):
        for rewritten_commit_id, line_count in rewrites.items():
            rewritten = g2_repo.get(rewritten_commit_id)
            output.write(
                Kind.E_PATCH_REWRITE.value,
                EPatchRewrite(
                    commit_id=commit.id.hex,
                    atime=_utc_timestamp_for(commit.author),
                    aemail=commit.author.email,
                    newpath=patch.delta.new_file.path,
                    rewritten_commit_id=str(rewritten_commit_id),
                    rewritten_atime=_utc_timestamp_for(rewritten.author),
                    rewritten_aemail=rewritten.author.email,
                    loc_d=line_count,
                ),
            )

    return len(rewrites), sum(rewrites.values())


def _utc_timestamp_for(signature):
    return signature.time + signature.offset * 60


def blame_porcelain(git_path, filepath, newest_commit) -> Dict[int, str]:
    """Returns with line-> commit_id mapping"""

    blame_porcelain_command = ["git", "blame", newest_commit, "--porcelain", "--", filepath]
    try:
        completed_process = subprocess.run(blame_porcelain_command, capture_output=True, cwd=git_path, check=True)
    except subprocess.CalledProcessError:
        logger.exception("Failed to run git blame.", newest_commit=newest_commit, git_path=git_path, filepath=filepath)
        return {}
    porcelain_output = completed_process.stdout.decode(errors="ignore")

    def is_not_header(line):
        return (
            line.startswith("\t")
            or line.startswith("author")
            or line.startswith("committer")
            or line.startswith("summary")
            or line.startswith("filename")
            or line.startswith("previous")
        )

    headers = {}
    for line in porcelain_output.splitlines():
        if is_not_header(line):
            continue
        words = line.split(" ")
        # THE PORCELAIN FORMAT
        # In this format, each line is output after a header;
        # the header at the minimum has the first line which has:
        # 40-byte SHA-1 of the commit the line is attributed to;
        # the line number of the line in the original file;
        # the line number of the line in the final file;
        if words and COMMIT_ID_RE.match(words[0]):
            headers[int(words[2])] = words[0]

    return headers