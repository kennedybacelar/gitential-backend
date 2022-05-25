import os
from pathlib import Path
from datetime import timezone, timedelta, datetime

import pygit2
from pygit2 import Repository


def gathering_commits_in_master_branch(path: Path):
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

    for commit in repo.walk(last.id, pygit2.GIT_SORT_TIME):
        current = repo.revparse_single(f"{master_branch.name}")
        previous = repo.revparse_single(f"{master_branch.name}^")
        commit_diff_obj = repo.diff(current, previous)
        commit_diff = commit_diff_obj.patch
        date = commit.author.time
        tzinfo = timezone(timedelta(minutes=commit.author.offset))
        commit_date_formatted = datetime.fromtimestamp(float(date), tzinfo)
