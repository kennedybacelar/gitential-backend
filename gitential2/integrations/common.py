from typing import Optional
from requests.utils import parse_header_links


def walk_next_link(client, starting_url, acc=None):
    def _get_next_link(link_header) -> Optional[str]:
        if link_header:
            header_links = parse_header_links(link_header)
            for link in header_links:
                if link["rel"] == "next":
                    return link["url"]
        return None

    print(f"WALKING: {starting_url}")
    acc = acc or []
    response = client.request("GET", starting_url)
    items, headers = response.json(), response.headers
    acc = acc + items
    next_url = _get_next_link(headers.get("Link"))
    if next_url:
        return walk_next_link(client, next_url, acc)
    else:
        return acc
