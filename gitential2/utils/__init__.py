from typing import Optional
from datetime import datetime
from urllib.parse import urlparse


def levenshtein(s1: str, s2: str):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)  # pylint: disable=arguments-out-of-order
    # len(s1) >= len(s2)
    if not s2:
        return len(s1)
    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = (
                previous_row[j + 1] + 1
            )  # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1  # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]


def levenshtein_ratio(s1: str, s2: str) -> float:
    distance = levenshtein(s1, s2)
    return 1.0 - (distance / max(len(s1), len(s2)))


def find_first(predicate, iterable):
    for i in iterable:
        if predicate(i):
            return i
    return None


def remove_none(iterable):
    return [e for e in iterable if e is not None]


def rchop(s, sub):
    return s[: -len(sub)] if s.endswith(sub) else s


def lchop(s, sub):
    return s[len(sub) :] if s.startswith(sub) else s


def calc_repo_namespace(clone_url: str) -> str:
    def _remove_last_part(path):
        _ignored_parts = ["_git"]
        return "/".join([e for e in path.split("/")[:-1] if e not in _ignored_parts]).strip("/")

    if "://" in clone_url:
        if clone_url.startswith("ssh://") and len(clone_url.split(":")) > 2:
            # bad, messed up uri + ssh clone_url ssh://git@xxxx:yyyyy.git ...
            return calc_repo_namespace(clone_url[6:])
        parsed_url = urlparse(clone_url)

        return _remove_last_part(parsed_url.path)
    else:
        _, path = clone_url.split(":")
        return _remove_last_part(path)


def split_timerange(from_: datetime, to_: datetime, parts: int = 2):
    time_delta = to_ - from_
    step = time_delta / parts
    start_dt = from_
    end_dt = from_ + step
    yield start_dt, end_dt
    while to_ - end_dt > step:
        start_dt, end_dt = end_dt, end_dt + step
        yield start_dt, end_dt
    yield end_dt, to_


def common_elements_if_not_none(l1: Optional[list], l2: Optional[list]) -> Optional[list]:
    if l1 is None:
        return l2
    elif l2 is None:
        return l1
    else:
        ret = []
        for e in l1 + l2:
            if (e in l1) and (e in l2):
                ret.append(e)
        return ret
