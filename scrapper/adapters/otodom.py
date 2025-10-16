# scrapper/adapters/otodom.py
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from scrapper.adapters.base import BaseAdapter, OfferIndex
from scrapper.core.dedup import DedupeSet, normalize_url
from scrapper.core.http import HttpClient, join_url
from scrapper.core.parse import find_ld_json_all, select_text, soup
from scrapper.core.storage import append_rows_csv, offers_csv_path, urls_csv_path

BASE = "https://www.otodom.pl"
OFFER_HREF_RE = re.compile(r"/pl/oferta/[^\"'#?]+")
OFFER_ID_RE = re.compile(r"(?:-ID|unique_id=)(\d{6,})")


def _build_listing_url(city: str, deal: str, kind: str, page: int) -> str:
    city_slug = (
        city.strip().lower().replace(" ", "-")
        .replace("ą", "a").replace("ć", "c").replace("ę", "e").replace("ł", "l")
        .replace("ń", "n").replace("ó", "o").replace("ś", "s").replace("ź", "z").replace("ż", "z")
    )
    return f"{BASE}/pl/oferty/{deal}/{kind}/{city_slug}?page={page}"


def _extract_offer_links(html: str) -> list[str]:
    s = soup(html)
    hrefs: list[str] = []
    for a in s.select("a[href]"):
        h = a.get("href", "")
        if OFFER_HREF_RE.search(h):
            hrefs.append(h)
    hrefs += OFFER_HREF_RE.findall(html)
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


def _json_loads_safe(txt: str) -> Any | None:
    try:
        return json.loads(txt)
    except Exception:
        return None


def _coerce_float(x) -> float | None:
    try:
        return float(str(x).replace(" ", "").replace(",", "."))
    except Exception:
        return None


def _coerce_int(x) -> int | None:
    try:
        return int(float(str(x).replace(" ", "").replace(",", ".")))
    except Exception:
        return None


def _iso_or_none(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _parse_ld_json_offer(html: str) -> dict[str, Any]:
    blocks = find_ld_json_all(html)
    out: dict[str, Any] = {}
    photos: list[str] = []
    for raw in blocks:
        data = _json_loads_safe(raw)
        if not data:
            continue
        candidates = data if isinstance(data, list) else [data]
        for d in candidates:
            if not isinstance(d, dict):
                continue
            price = None
            currency = None
            if "offers" in d and isinstance(d["offers"], dict):
                ospec = d["offers"].get("price") or d["offers"].get("priceSpecification", {}).get("price")
                price = ospec if ospec is not None else price
                currency = d["offers"].get("priceCurrency") or d["offers"].get("priceSpecification", {}).get("priceCurrency")
            if "price" in d and price is None:
                price = d.get("price")
            if "priceCurrency" in d and currency is None:
                currency = d.get("priceCurrency")
            if price is not None:
                out["price_amount"] = _coerce_float(price)
            if currency:
                out["price_currency"] = str(currency).upper()

            if "name" in d and not out.get("title"):
                out["title"] = str(d["name"]).strip()
            if "description" in d and not out.get("description"):
                out["description"] = str(d["description"]).strip()

            addr = d.get("address") or {}
            if isinstance(addr, dict):
                out.setdefault("city", addr.get("addressLocality") or addr.get("addressRegion"))
                out.setdefault("street", addr.get("streetAddress"))
            geo = d.get("geo") or {}
            if isinstance(geo, dict):
                out["lat"] = _coerce_float(geo.get("latitude"))
                out["lon"] = _coerce_float(geo.get("longitude"))

            out.setdefault("posted_at", _iso_or_none(d.get("datePosted") or d.get("datePublished")))
            out.setdefault("updated_at", _iso_or_none(d.get("dateModified")))

            imgs = d.get("image") or d.get("photos") or []
            if isinstance(imgs, list):
                for im in imgs:
                    if isinstance(im, str):
                        photos.append(im)
                    elif isinstance(im, dict) and im.get("url"):
                        photos.append(im["url"])

            area = d.get("floorSize") or d.get("area") or {}
            if isinstance(area, dict) and "value" in area:
                out["area_m2"] = _coerce_float(area["value"])
            rooms = d.get("numberOfRooms")
            if rooms is not None:
                out["rooms"] = _coerce_int(rooms)

    if photos:
        out["photos_from_json"] = photos
    return out


def _parse_fallback_css(html: str) -> dict[str, Any]:
    s = soup(html)
    out: dict[str, Any] = {}
    t = select_text(s, "h1, h1[data-cy='adpage-header-title']")
    if t:
        out["title"] = t
    ptxt = select_text(s, "[data-cy='adPageHeader-price'], .price-box, .css-1w6f3ze")
    if ptxt:
        m = re.search(r"([\d\s.,]+)", ptxt)
        if m:
            out["price_amount"] = _coerce_float(m.group(1))
        cur = "PLN" if "zł" in ptxt or "PLN" in ptxt.upper() else None
        if cur:
            out["price_currency"] = cur
    city = select_text(s, "[data-cy='adPageHeader-locality']")
    if city:
        out["city"] = city
    area = select_text(s, "ul, .css-1ci0qpi, .parameters li")
    if area:
        m = re.search(r"([\d.,]+)\s*m", area)
        if m:
            out["area_m2"] = _coerce_float(m.group(1))
    desc = select_text(s, "[data-cy='adPage-description'], .description, [itemprop='description']")
    if desc:
        out["description"] = desc
    return out


def _offer_id_from_url(url: str) -> str | None:
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

    # P10 — szczegół
    def parse_offer(self, url: str) -> dict[str, Any]:
        assert self.http is not None, "HttpClient not set. Call with_deps()."
        url = normalize_url(url)
        html = self.http.get(url, accept="text/html").text

        data = {
            "offer_id": _offer_id_from_url(url) or "",
            "source": self.source,
            "url": url,
        }

        ld = _parse_ld_json_offer(html)
        data.update({k: v for k, v in ld.items() if k != "photos_from_json"})

        fb = _parse_fallback_css(html)
        for k, v in fb.items():
            data.setdefault(k, v)

        if not data.get("price_currency") and data.get("price_amount"):
            data["price_currency"] = "PLN"
        data.setdefault("title", "")
        data.setdefault("description", "")
        return data

    def write_offers_csv(self, rows: Iterable[dict[str, Any]]) -> Path:
        assert self.out_dir is not None, "out_dir not set. Call with_deps()."
        header = [
            "offer_id","source","url","title","price_amount","price_currency",
            "property_type","market_type","city","district","street","lat","lon",
            "area_m2","rooms","floor","max_floor","year_built","building_type",
            "ownership","agent","agency","phone","description","features","json_raw",
            "posted_at","updated_at","first_seen","last_seen"
        ]
        path = offers_csv_path(self.out_dir)
        append_rows_csv(path, rows, header)
        return path
