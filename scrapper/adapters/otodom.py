# scrapper/adapters/otodom.py
from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scrapper.adapters.base import BaseAdapter, OfferIndex
from scrapper.core.dedup import DedupeSet, normalize_url
from scrapper.core.http import HttpClient, join_url
from scrapper.core.parse import soup
from scrapper.core.storage import append_rows_csv, urls_csv_path

BASE = "https://www.otodom.pl"

# heurystyka: linki do ofert mają segment /pl/oferta/
OFFER_HREF_RE = re.compile(r"/pl/oferta/[^\"'#?]+")
# czasem ID pojawia się jako sufiks -ID<digits> lub ?unique_id=<digits>
OFFER_ID_RE = re.compile(r"(?:-ID|unique_id=)(\d{6,})")


def _build_listing_url(city: str, deal: str, kind: str, page: int) -> str:
    """/pl/oferty/{deal}/{kind}/{city_slug}?page=N."""
    city_slug = (
        city.strip()
        .lower()
        .replace(" ", "-")
        .replace("ą", "a").replace("ć", "c").replace("ę", "e").replace("ł", "l")
        .replace("ń", "n").replace("ó", "o").replace("ś", "s").replace("ź", "z").replace("ż", "z")
    )
    base = f"{BASE}/pl/oferty/{deal}/{kind}/{city_slug}"
    return f"{base}?page={page}"


def _extract_offer_links(html: str) -> list[str]:
    """Ekstrakcja URL-i ofert z listingu."""
    s = soup(html)
    hrefs: list[str] = []
    for a in s.select("a[href]"):
        h = a.get("href", "")
        if OFFER_HREF_RE.search(h):
            hrefs.append(h)
    hrefs += OFFER_HREF_RE.findall(html)  # fallback
    out, seen = [], set()
    for h in hrefs:
        full = normalize_url(join_url(BASE, h))
        if full not in seen:
            seen.add(full)
            out.append(full)
    return out


def _maybe_offer_id(url: str) -> str | None:
    m = OFFER_ID_RE.search(url)
    return m.group(1) if m else None


@dataclass
class OtodomAdapter(BaseAdapter):
    source: str = "otodom"
    http: HttpClient | None = None
    out_dir: Path | None = None

    def with_deps(self, http: HttpClient, out_dir: Path) -> "OtodomAdapter":
        self.http = http
        self.out_dir = out_dir
        return self

    # P8 — listing
    def discover(self, *, city: str, deal: str, kind: str, max_pages: int = 1) -> Iterable[OfferIndex]:
        assert self.http is not None, "HttpClient not set. Call with_deps()."
        ded = DedupeSet()
        page = 1
        while page <= max_pages:
            url = _build_listing_url(city, deal, kind, page)
            resp = self.http.get(url, accept="text/html")
            links = _extract_offer_links(resp.text)
            if not links and page > 1:
                break
            for ln in links:
                if ded.seen_url(ln):
                    continue
                idx: OfferIndex = {"offer_url": ln, "page_idx": page}
                oid = _maybe_offer_id(ln)
                if oid:
                    idx["offer_id"] = oid
                yield idx
            page += 1

    def write_urls_csv(self, rows: Iterable[OfferIndex]) -> Path:
        assert self.out_dir is not None, "out_dir not set. Call with_deps()."
        header = ["offer_url", "offer_id", "page_idx"]
        path = urls_csv_path(self.out_dir)
        append_rows_csv(path, rows, header)
        return path