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
