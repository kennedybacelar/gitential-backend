from typing import Optional
from requests.utils import parse_header_links
from requests import Response
from structlog import get_logger

logger = get_logger(__name__)


def walk_next_link(client, starting_url, acc=None, max_pages=50):
    def _get_next_link(link_header) -> Optional[str]:
        if link_header:
            header_links = parse_header_links(link_header)
            for link in header_links:
                if link["rel"] == "next":
                    return link["url"]
        return None

    logger.debug("Walking next link", url=starting_url)

    acc = acc or []
    response = client.request("GET", starting_url)
    if response.status_code == 200:
        items, headers = response.json(), response.headers
        acc = acc + items
        next_url = _get_next_link(headers.get("Link"))
        if next_url and max_pages > 0:
            return walk_next_link(client, next_url, acc, max_pages=max_pages - 1)
        else:
            return acc
    else:
        log_api_error(response)
        return acc


def log_api_error(response: Response):
    logger.error(
        "Failed to get API resource",
        url=response.request.url,
        status_code=response.status_code,
        response_text=response.text,
        response_headers=response.headers,
    )
