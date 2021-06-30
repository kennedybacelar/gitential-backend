from typing import List
from itertools import combinations

from gitential2.datatypes.refresh import RefreshType
from gitential2.datatypes.authors import AuthorInDB
from .refresh_v2 import refresh_workspace
from .authors import authors_matching, merge_authors
from .context import GitentialContext


def deduplicate_authors(g: GitentialContext, workspace_id: int, dry_run: bool = False) -> List[List[AuthorInDB]]:
    clusters: List[List[AuthorInDB]] = []

    def _add_to_the_cluster(first, second):
        for cluster in clusters:
            if first in cluster and second in cluster:
                break
            if first in cluster and second not in cluster:
                cluster.append(second)
                break
            if second in cluster and first not in cluster:
                cluster.append(first)
                break
        else:
            # Create a new cluster
            clusters.append([first, second])

    all_authors = g.backend.authors.all(workspace_id)
    for first, second in combinations(all_authors, 2):
        if first.id != second.id and authors_matching(first, second):
            _add_to_the_cluster(first, second)
    if not dry_run and clusters:
        for cluster in clusters:
            merge_authors(g, workspace_id, cluster)
        refresh_workspace(g, workspace_id, refresh_type=RefreshType.commit_calculations_only)
    return clusters
