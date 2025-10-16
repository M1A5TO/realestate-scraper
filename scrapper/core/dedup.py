# core/dedup.py
from __future__ import annotations

from collections.abc import Iterable


def normalize_url(u: str) -> str:
    # uproszczona normalizacja: bez fragmentu i trailing slash
    from urllib.parse import urlsplit, urlunsplit
    parts = list(urlsplit(u))
    parts[2] = parts[2].rstrip("/")  # path
    parts[3] = "&".join(sorted(filter(None, parts[3].split("&"))))  # query: sort keys
    parts[4] = ""  # fragment
    return urlunsplit(parts)

class DedupeSet:
    """Prosty dedupe w pamięci po URL lub offer_id."""
    def __init__(self):
        self._seen_urls: set[str] = set()
        self._seen_ids: set[str] = set()

    def seen_url(self, url: str) -> bool:
        nu = normalize_url(url)
        if nu in self._seen_urls:
            return True
        self._seen_urls.add(nu)
        return False

    def seen_id(self, offer_id: str) -> bool:
        if offer_id in self._seen_ids:
            return True
        self._seen_ids.add(offer_id)
        return False

    def bulk_mark_urls(self, urls: Iterable[str]) -> None:
        for u in urls:
            self._seen_urls.add(normalize_url(u))
