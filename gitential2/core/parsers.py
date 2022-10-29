from typing import List


def parse_repo_ids_from_url_param(param: str) -> List[int]:
    """
    @description: This function helps to parse url params in the form ?foo=1&foo=2
    Where there are multiple url param with the same key as in the case
    from our Frontend App
    """
    splitted_param = param.split("&")
    repo_items = [i for i in splitted_param if "repo_id" in i]
    sentence = "".join(repo_items)
    repo_ids = [int(i) for i in sentence if i.isdigit()]
    return repo_ids
