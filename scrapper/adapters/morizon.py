from __future__ import annotations

import json, re, urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from bs4 import BeautifulSoup  # (nieużywane bezpośrednio, ale ok jeśli chcesz podmienić soup)

from scrapper.adapters.base import BaseAdapter, OfferIndex, PhotoMeta
from scrapper.core.dedup import DedupeSet, normalize_url
from scrapper.core.http import HttpClient, join_url
from scrapper.core.parse import soup
from scrapper.core.storage import append_rows_csv, offers_csv_path, photos_csv_path, urls_csv_path, append_offer_row
from scrapper.core.log import get_logger
log = get_logger("scrapper.morizon")
import hashlib
import time
from math import radians, sin, cos, asin, sqrt
from datetime import datetime 
# ---------- Pomocnicze ----------

_PL_BBOX = (49.0, 54.9, 14.0, 24.5)  # min_lat, max_lat, min_lon, max_lon (z lekkim marginesem)

# --- Morizon "hydrated" bounds: {"latitude":ID,"longitude":ID}, <lat>, <lon>, {"latitude":ID,"longitude":ID}, <lat>, <lon>
_MORIZON_HYDRATED_BOUNDS_RE = re.compile(
    r'\{"latitude":\d+,"longitude":\d+\}\s*,\s*(?P<lat1>\d{2}\.\d+)\s*,\s*(?P<lon1>\d{2}\.\d+)'
    r'\s*,\s*\{"latitude":\d+,"longitude":\d+\}\s*,\s*(?P<lat2>\d{2}\.\d+)\s*,\s*(?P<lon2>\d{2}\.\d+)',
    re.I
)
_OFFER_HREF_RE = re.compile(
    r'href=["\'](?P<href>(?:https?:)?//www\.morizon\.pl/oferta/[^"\']*mzn\d+)[\'"]|'
    r'href=["\'](?P<rel>/oferta/[^"\']*mzn\d+)[\'"]',
    re.I
)
# --- ceny (obsługa spacji zwykłej i niełamliwej) ---
PRICE_TOTAL_RE  = re.compile(r'(\d[\d\s\u00A0,\.]{3,})\s*zł(?!\s*/\s*m(?:2|²))', re.I)
PRICE_PERM2_RE  = re.compile(r'(\d[\d\s\u00A0,\.]{3,})\s*zł\s*/\s*m(?:2|²)', re.I)


def _category_from_kind(kind: Optional[str]) -> str:
    # najprostsze i najstabilniejsze kategorie w Morizon
    k = (kind or "").lower()
    if k.startswith("miesz"):  # mieszkanie/mieszkania
        return "mieszkania"
    if k.startswith("dom"):
        return "domy"
    if k.startswith("dzial"):  # dzialka/działka
        return "dzialki"
    if k.startswith("lokal"):
        return "lokale"
    return "mieszkania"

def _is_plausible_pl(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    min_lat, max_lat, min_lon, max_lon = _PL_BBOX
    return (min_lat <= float(lat) <= max_lat) and (min_lon <= float(lon) <= max_lon)

def _coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    txt = str(x).strip().replace("\xa0", " ")
    txt = txt.replace(" ", "")
    txt = txt.replace(",", ".")
    m = re.match(r"^-?\d+(?:\.\d+)?$", txt)
    return float(txt) if m else None

def _offer_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"(mzn\d{6,})", url)
    return m.group(1) if m else None

def _parse_ld_json_blocks(html: str) -> list[dict]:
    out: list[dict] = []
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S|re.I):
        raw = m.group(1).strip()
        if not raw:
            continue
        # Morizon czasem pakuje tam tablicę obiektów
        try:
            obj = json.loads(raw)
            if isinstance(obj, list):
                out.extend([x for x in obj if isinstance(x, dict)])
            elif isinstance(obj, dict):
                out.append(obj)
        except Exception:
            # bywa trailing koma / komentarze – odpuść, niech inne ścieżki zadziałają
            continue
    return out



def _extract_offer_links(html: str) -> list[str]:
    if not html:
        return []
    out: list[str] = []
    for m in _OFFER_HREF_RE.finditer(html):
        if m.group("href"):
            out.append(m.group("href"))
        elif m.group("rel"):
            out.append("https://www.morizon.pl" + m.group("rel"))
    # lekkie odszumianie parametrów śledzących i powtórek
    norm = []
    seen = set()
    for u in out:
        u = normalize_url(u)
        oid = _offer_id_from_url(u)
        if oid and oid not in seen:
            seen.add(oid)
            norm.append(u)
    return norm

def _extract_latlon_from_hydrated_bounds(html: str) -> tuple[Optional[float], Optional[float]]:
    """
    Morizon embeduje granice mapy jako: {latitude:ID, longitude:ID}, <lat>, <lon>, {latitude:ID, longitude:ID}, <lat>, <lon>
    Traktujemy to jako NE i SW i zwracamy środek.
    """
    if not html:
        return None, None
    m = _MORIZON_HYDRATED_BOUNDS_RE.search(html)
    if not m:
        return None, None
    try:
        lat1 = _coerce_float(m.group("lat1")); lon1 = _coerce_float(m.group("lon1"))
        lat2 = _coerce_float(m.group("lat2")); lon2 = _coerce_float(m.group("lon2"))
        if None in (lat1, lon1, lat2, lon2):
            return None, None
        la = (lat1 + lat2) / 2.0
        lo = (lon1 + lon2) / 2.0
        if _is_plausible_pl(la, lo):
            return la, lo
        if _is_plausible_pl(lo, la):  # defensywnie
            return lo, la
    except Exception:
        pass
    return None, None

def _extract_from_ld(block: dict) -> dict:
    data: dict[str, Any] = {}
    try:
        oa = block.get("offers") or block.get("offer")
        if isinstance(oa, dict):
            pa = _coerce_float(oa.get("price"))
            if pa is not None:
                data["price_amount"] = pa
            if oa.get("priceCurrency"):
                data["price_currency"] = str(oa.get("priceCurrency")).upper()

        if block.get("name"):
            data["title"] = str(block["name"]).strip()

        if block.get("floorSize"):
            val = block["floorSize"].get("value")
            if val is not None and _coerce_float(val) is not None:
                data["area_m2"] = _coerce_float(val)
        if block.get("numberOfRooms"):
            nr = _coerce_float(block["numberOfRooms"])
            if nr is not None:
                data["rooms"] = nr

        addr = block.get("address")
        if isinstance(addr, dict):
            if addr.get("addressLocality"):
                data["city"] = str(addr["addressLocality"]).strip()
            if addr.get("streetAddress"):
                data["street"] = str(addr["streetAddress"]).strip()

        if block.get("datePosted"):
            data["posted_at"] = str(block["datePosted"]).strip()
        if block.get("dateModified"):
            data["updated_at"] = str(block["dateModified"]).strip()

        geo = block.get("geo")
        if isinstance(geo, dict):
            la = _coerce_float(geo.get("latitude"))
            lo = _coerce_float(geo.get("longitude"))
            if _is_plausible_pl(la, lo):
                data["lat"], data["lon"] = la, lo

        images = []
        if block.get("image"):
            if isinstance(block["image"], list):
                images = [str(x) for x in block["image"] if x]
            elif isinstance(block["image"], str):
                images = [block["image"]]
        if images:
            data["photos_from_json"] = images
    except Exception:
        pass
    return data

def _extract_geo_from_dom(html: str) -> tuple[Optional[float], Optional[float]]:
    patterns = [
        r'itemprop=["\']latitude["\'][^>]*content=["\']([0-9\.\-]+)["\'].*?itemprop=["\']longitude["\'][^>]*content=["\']([0-9\.\-]+)["\']',
        r'property=["\']place:location:latitude["\'][^>]*content=["\']([0-9\.\-]+)["\'].*?property=["\']place:location:longitude["\'][^>]*content=["\']([0-9\.\-]+)["\']',
        r'data-lat=["\']([0-9\.\-]+)["\']\s+data-lng=["\']([0-9\.\-]+)["\']',
        r'data-latitude=["\']([0-9\.\-]+)["\']\s+data-longitude=["\']([0-9\.\-]+)["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.I|re.S)
        if m:
            la, lo = _coerce_float(m.group(1)), _coerce_float(m.group(2))
            if _is_plausible_pl(la, lo):
                return la, lo
    return None, None

def _extract_latlon_from_any_json(html: str) -> tuple[Optional[float], Optional[float]]:
    try:
        # "latitude": ..., "longitude": ...
        for m in re.finditer(r'"latitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1.*?"longitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\3', html, re.I|re.S):
            la = _coerce_float(m.group(2)); lo = _coerce_float(m.group(4))
            if _is_plausible_pl(la, lo):
                return la, lo

        # "lat": ..., "lng"/"lon"/"long": ...
        for m in re.finditer(r'"lat"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1.*?"(?:lng|lon|long)"\s*:\s*("?)(-?\d+(?:\.\d+)?)\3', html, re.I|re.S):
            la = _coerce_float(m.group(2)); lo = _coerce_float(m.group(4))
            if _is_plausible_pl(la, lo):
                return la, lo

        # Leaflet
        m = re.search(r'L\.marker\(\s*\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]', html, re.I)
        if m:
            la = _coerce_float(m.group(1)); lo = _coerce_float(m.group(2))
            if _is_plausible_pl(la, lo):
                return la, lo

        # Mapbox (uwaga: kolejność [lon, lat])
        m = re.search(r'setLngLat\(\s*\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]\)', html, re.I)
        if m:
            lo = _coerce_float(m.group(1)); la = _coerce_float(m.group(2))
            if _is_plausible_pl(la, lo):
                return la, lo

        # Hydrated bounds (środek bboxa)
        la_h, lo_h = _extract_latlon_from_hydrated_bounds(html)
        if _is_plausible_pl(la_h, lo_h):
            return la_h, lo_h
    except Exception:
        pass
    return None, None

def _city_district_street_from_url(url: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parsuje {city, district, street} z URL Morizonu.
    Obsługuje numerację ulic (np. -9-), wielowyrazowe nazwy oraz różne kolejności tokenów.
    Przykład: /oferta/sprzedaz-mieszkanie-gdansk-stogi-skiby-9-61m2-mzn2046408664
              -> ("Gdansk","Stogi","Skiby")
    """
    u = normalize_url(url)
    m = re.search(r'/oferta/([a-z0-9\-\u0105\u0107\u0119\u0142\u0144\u00F3\u015B\u017A\u017C\u017A]+)', u, re.I)
    if not m:
        return None, None, None
    tail = m.group(1)

    m2 = re.search(r'-(\d+)\s*m2-', tail, re.I)
    if not m2:
        return None, None, None
    head = tail[:m2.start()]  # np. sprzedaz-mieszkanie-gdansk-stogi-skiby-9
    toks = [t for t in head.split('-') if t]

    pref = {"sprzedaz","wynajem","mieszkanie","mieszkania","dom","domy","dzialka","dzialki","lokal","lokale","nieruchomosci"}
    i = 0
    while i < len(toks) and toks[i] in pref:
        i += 1
    if i+1 >= len(toks):
        return None, None, None

    city_tok = toks[i]
    district_tok = toks[i+1]
    rest = toks[i+2:]

    # Z końca usuń numery domów (np. 9, 9a)
    while rest and re.fullmatch(r'\d+[a-zA-Z]?', rest[-1]):
        rest.pop()
    street_tok = rest[0] if rest else None

    def fix(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        s = s.strip("-").replace("-", " ")
        return s[:1].upper() + s[1:]

    return fix(city_tok), fix(district_tok), fix(street_tok)


def _extract_area_rooms_from_text(page_text: str) -> tuple[Optional[float], Optional[float]]:
    # PEWNY guard: jeśli brak tekstu – unikamy TypeError
    if not page_text:
        return None, None
    area = None; rooms = None
    try:
        m_rooms = re.search(r'\b(\d+)\s+pokoje?\b', page_text, re.I)
        if m_rooms:
            rooms = _coerce_float(m_rooms.group(1))

        clean = re.sub(r'\b\d+\s*[–-]\s*\d+\s*m[²2]\b', ' ', page_text)
        best = None
        for m in re.finditer(r'([\d\.,]+)\s*m[²2]\b', clean, re.I):
            ctx = clean[max(0, m.start()-16):m.start()]
            if "zł" in ctx or "/m" in ctx:
                continue
            v = _coerce_float(m.group(1))
            if v and 10 <= v <= 1000:
                best = v if best is None else max(best, v)
        if best is not None:
            area = best
    except Exception:
        pass
    return area, rooms

def _has_inquiry_price(bs) -> bool:
    txt = " ".join([
        (bs.select_one(".price-row_wrapper") or bs).get_text(" ", strip=True),
        (bs.select_one("button") or bs).get_text(" ", strip=True),
    ]).lower()
    return "zapytaj o cen" in txt or "zapytaj o cenę" in txt

def _extract_prices_from_text(txt: str) -> tuple[Optional[float], Optional[float]]:
    """Zwraca (price_amount, price_per_m2) jeśli znajdzie w tekście."""
    if not txt:
        return None, None
    pa = ppm2 = None

    m_total = PRICE_TOTAL_RE.search(txt)
    if m_total:
        pa = _coerce_float(m_total.group(1))

    m_ppm2 = PRICE_PERM2_RE.search(txt)
    if m_ppm2:
        ppm2 = _coerce_float(m_ppm2.group(1))

    return pa, ppm2

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Odległość wielkiego koła w metrach."""
    if None in (lat1, lon1, lat2, lon2):
        return float("inf")
    R = 6371000.0
    φ1, λ1, φ2, λ2 = map(radians, (lat1, lon1, lat2, lon2))
    dφ = φ2 - φ1
    dλ = λ2 - λ1
    a = sin(dφ/2)**2 + cos(φ1)*cos(φ2)*sin(dλ/2)**2
    return 2*R*asin(a**0.5)


def _coerce_int(v):
    try:
        return int(v)
    except Exception:
        return None




# ---------- Adapter ----------

@dataclass
class MorizonAdapter(BaseAdapter):
    source: str = "morizon"
    http: Optional[HttpClient] = None
    out_dir: Optional[Path] = None
    use_osm_geocode: bool = False

    def with_deps(self, http: HttpClient, out_dir: Path, use_osm_geocode: bool = False):
        self.http = http
        self.out_dir = out_dir
        self.use_osm_geocode = use_osm_geocode
        return self

    def write_urls_csv(self, rows: Iterable[OfferIndex]) -> Path:
        assert self.out_dir is not None, "out_dir not set. Call with_deps()."
        header = ["offer_url", "offer_id", "page_idx", "source"]
        materialized = []
        for r in rows or []:
            d = dict(r)
            d["source"] = self.source
            materialized.append(d)
        path = urls_csv_path(self.out_dir)
        append_rows_csv(path, materialized, header)
        return path


    def write_offers_csv(self, rows: list[dict]) -> Path:
        assert self.out_dir is not None
        path = offers_csv_path(self.out_dir)
        # NIE ustawiamy lokalnego headera, NIE wołamy append_rows_csv
        for r in rows or []:
            append_offer_row(path, r)   # wymusza stałą kolejność kolumn i nagłówek
        return path

    # def write_photos_csv(self, rows: list[PhotoMeta]) -> Path:
    #     assert self.out_dir is not None
    #     header = ["source","offer_id","seq","url","width","height","local_path","bytes","hash","status","downloaded_at"]
    #     path = photos_csv_path(self.out_dir)
    #     append_rows_csv(path, rows, header)
    #     return path

    def _geo_cache_path(self) -> Path:
        assert self.out_dir is not None
        return self.out_dir / "geocache_osm.json"

    def _geo_cache_load(self) -> dict[str, tuple[float, float]]:
        p = self._geo_cache_path()
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _geo_cache_save(self, cache: dict[str, tuple[float, float]]) -> None:
        p = self._geo_cache_path()
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _geocode_osm(self, *, city: Optional[str], street: Optional[str]) -> tuple[Optional[float], Optional[float]]:
        """
        Fallback: Nominatim. Z cache na dysku. Limit 1 zapyt./sek (honorujemy RPS klienta).
        """
        q_parts = []
        if street: q_parts.append(str(street))
        if city:   q_parts.append(str(city))
        q_parts.append("Polska")
        q = ", ".join(q_parts).strip().lower()
        if not q or self.http is None:
            return None, None

        cache = self._geo_cache_load()
        if q in cache:
            la, lo = cache[q]
            return (float(la), float(lo)) if _is_plausible_pl(la, lo) else (None, None)

        # Nominatim: GET /search?format=jsonv2&q=...
        base = "https://nominatim.openstreetmap.org/search"
        params = {
            "format": "jsonv2",
            "q": q,
            "addressdetails": 0,
            "limit": 1,
            # opcjonalnie zawęź do PL bbox (minimalizuje pomyłki):
            "viewbox": f"{_PL_BBOX[2]},{_PL_BBOX[1]},{_PL_BBOX[3]},{_PL_BBOX[0]}",
            "bounded": 1,
        }
        url = base + "?" + urllib.parse.urlencode(params)
        try:
            resp = self.http.get(url, accept="application/json")
            data = resp.json()
            if isinstance(data, list) and data:
                la = _coerce_float(data[0].get("lat"))
                lo = _coerce_float(data[0].get("lon"))
                if _is_plausible_pl(la, lo):
                    cache[q] = (la, lo)
                    self._geo_cache_save(cache)
                    return la, lo
        except Exception:
            pass
        return None, None



    def _snap_geo_if_far(self, data: dict, *, max_dist_m: float = 800.0) -> None:
        """
        Jeśli mamy city+street i geo z OSM różni się > max_dist_m, to podmień na OSM.
        Nie robi nic, gdy OSM nie zwróciło punktu.
        """
        if not self.use_osm_geocode:
            return
        city = data.get("city")
        street = data.get("street")
        if not city or not street:
            return
        la, lo = data.get("lat"), data.get("lon")
        la_osm, lo_osm = self._geocode_osm(city=city, street=street)
        if _is_plausible_pl(la_osm, lo_osm):
            if (not _is_plausible_pl(la, lo)) or (_haversine_m(la, lo, la_osm, lo_osm) > max_dist_m):
                data["lat"], data["lon"] = la_osm, lo_osm

    # --- DETAIL ---
    def parse_offer(self, url: str) -> dict:
        assert self.http is not None
        url = normalize_url(url)
        html = self.http.get(url, accept="text/html").text
        bs = soup(html)
        inquiry = _has_inquiry_price(bs)

        data: dict[str, Any] = {
            "source": self.source,
            "url": url,
            "offer_id": _offer_id_from_url(url) or "",
            "title": (bs.select_one("h1").get_text(strip=True) if bs.select_one("h1") else "")
        }

        # 1) LD-JSON → najwyższy priorytet
        for block in _parse_ld_json_blocks(html):
            ext = _extract_from_ld(block)
            for k, v in ext.items():
                if v not in (None, ""):
                    data.setdefault(k, v)

        # 2) City / district / street z URL (nie nadpisuj jeśli już są)
        c, d, s = _city_district_street_from_url(url)
        if c and not data.get("city"):     data["city"] = c
        if d and not data.get("district"): data["district"] = d
        if s and not data.get("street"):   data["street"] = s

        # 3) Tekst strony
        page_text = bs.get_text(" ", strip=True)

        # 3a) Metraż/pokoje z pełnego tekstu (fallback)
        if data.get("area_m2") is None or data.get("rooms") is None:
            a2, r2 = _extract_area_rooms_from_text(page_text)
            if data.get("area_m2") is None and a2 is not None:
                data["area_m2"] = a2
            if data.get("rooms") is None and r2 is not None:
                data["rooms"] = r2

        # 3b) Ceny (fallback z tekstu)
        if not inquiry:
            pa_fallback, ppm2_from_text = _extract_prices_from_text(page_text)
            if data.get("price_amount") is None and pa_fallback is not None:
                data["price_amount"] = pa_fallback
            if data.get("price_amount") is not None and not data.get("price_currency"):
                data["price_currency"] = "PLN"
            if data.get("price_per_m2") is None and ppm2_from_text is not None:
                data["price_per_m2"] = ppm2_from_text
        else:
            # twarde wyzerowanie ceny całkowitej na stronach "Zapytaj o cenę"
            data["price_amount"] = None
            data["price_currency"] = None
            data["price_per_m2"] = None

        # 4) GEO: DOM → JSON/JS → HYDRATED → OSM
        la, lo = data.get("lat"), data.get("lon")

        if not _is_plausible_pl(la, lo):
            la_dom, lo_dom = _extract_geo_from_dom(html)
            if _is_plausible_pl(la_dom, lo_dom):
                la, lo = la_dom, lo_dom

        if not _is_plausible_pl(la, lo):
            la_js, lo_js = _extract_latlon_from_any_json(html)
            if _is_plausible_pl(la_js, lo_js):
                la, lo = la_js, lo_js

        if not _is_plausible_pl(la, lo):
            la_h, lo_h = _extract_latlon_from_hydrated_bounds(html)
            if _is_plausible_pl(la_h, lo_h):
                la, lo = la_h, lo_h

        if not _is_plausible_pl(la, lo) and self.use_osm_geocode:
            la_osm, lo_osm = self._geocode_osm(city=data.get("city"), street=data.get("street"))
            if _is_plausible_pl(la_osm, lo_osm):
                la, lo = la_osm, lo_osm

        if _is_plausible_pl(la, lo):
            data["lat"], data["lon"] = la, lo
        else:
            data.pop("lat", None); data.pop("lon", None)

        self._snap_geo_if_far(data, max_dist_m=800.0)
        # 5) cena/m² i korekta metrażu – tylko jeśli NIE inquiry
        if not inquiry and data.get("price_amount") and data.get("area_m2") and not data.get("price_per_m2"):
            try:
                pa = float(data["price_amount"]); ar = float(data["area_m2"])
                if ar > 0:
                    data["price_per_m2"] = round(pa / ar, 2)
            except Exception:
                pass

        # i sekcja korekty area z pa/ppm2:
        if not inquiry:
            try:
                pa = float(data["price_amount"]) if data.get("price_amount") is not None else None
                ppm2 = float(data["price_per_m2"]) if data.get("price_per_m2") is not None else None
                ar = float(data["area_m2"]) if data.get("area_m2") is not None else None
            except Exception:
                pa = ppm2 = ar = None

            if pa and ppm2 and ppm2 > 0:
                ar_calc = pa / ppm2
                if (ar is None) or (abs(ar - ar_calc) / ar_calc > 0.08):
                    data["area_m2"] = round(ar_calc, 2)

        return data

    def discover(self, city: str, deal: Optional[str], kind: Optional[str], max_pages: int = 1) -> list[OfferIndex]:
        """
        Prosty, stabilny listing:
        https://www.morizon.pl/{kategoria}/{miasto}/?page=N
        gdzie kategoria ∈ {mieszkania, domy, dzialki, lokale}.
        Parametr 'deal' (sprzedaz/wynajem) zostawiamy na później – na wielu listingach
        sprzedaz jest domyślna, a oferty i tak mają w URL 'sprzedaz-...'.
        """
        assert self.http is not None
        rows: list[OfferIndex] = []
        dedup_ids: set[str] = set()

        category = _category_from_kind(kind)
        city_slug = urllib.parse.quote((city or "").strip().lower().replace(" ", "-"))

        for page in range(1, int(max_pages) + 1):
            url = f"https://www.morizon.pl/{category}/{city_slug}/?page={page}"
            try:
                html = self.http.get(url, accept="text/html").text
            except Exception as e:
                log.warning("discover_fetch_fail", extra={"extra": {"url": url, "err": type(e).__name__}})
                continue

            links = _extract_offer_links(html)
            if not links:
                # Spróbuj alternatywnego wejścia (np. /nieruchomosci/{kategoria}/{miasto}/?page=N)
                alt = f"https://www.morizon.pl/nieruchomosci/{category}/{city_slug}/?page={page}"
                try:
                    html2 = self.http.get(alt, accept="text/html").text
                    links = _extract_offer_links(html2)
                except Exception:
                    links = []

            for href in links:
                oid = _offer_id_from_url(href)
                if not oid or oid in dedup_ids:
                    continue
                dedup_ids.add(oid)
                rows.append({
                    "offer_url": normalize_url(href),
                    "offer_id": oid,
                    "page_idx": page,
                })

            log.info("discover_page", extra={"extra": {"page": page, "found": len(links), "kept": len(rows)}})

        return rows
    
    def with_deps(self, http: HttpClient, out_dir: Path, use_osm_geocode: bool = False):
        self.http = http
        self.out_dir = out_dir
        self.use_osm_geocode = use_osm_geocode
        return self
    
    def parse_photos(self, html_or_url: str) -> list[PhotoMeta]:
        """
        Zwraca wszystkie zdjęcia z galerii Morizon (strona .../photo).
        Buduje absolutne URL-e, deduplikuje, numeruje seq od 0.
        Nie zapisuje plików, tylko linki.
        """
        assert self.http is not None, "HttpClient not set. Call with_deps()."

        def _best_from_srcset(srcset: str) -> str | None:
            best, best_w = None, -1
            for part in (srcset or "").split(","):
                p = part.strip().split()
                if not p:
                    continue
                url = p[0]
                w = 0
                if len(p) > 1 and p[1].lower().endswith("w"):
                    try:
                        w = int(p[1][:-1])
                    except:
                        w = 0
                if w > best_w:
                    best, best_w = url, w
            return best

        # 1) Ustal właściwy URL galerii
        if html_or_url.startswith("http"):
            base = html_or_url.split("?")[0].rstrip("/")
            photo_url = base + ("/photo" if not base.endswith("/photo") else "")
            html = self.http.get(photo_url, accept="text/html").text
        else:
            # rzadki przypadek: ktoś podał już HTML
            html = html_or_url

        # 2) Parsuj tylko zdjęcia z kontenera galerii
        s = soup(html)
        seen = set()
        urls: list[str] = []

        # tylko elementy galerii, nie ikonki UI ani reklamy
        sel = (
            "#gallery__photos img[src], "
            "#gallery__photos img[data-src], "
            ".gallery__photos img[src], "
            ".gallery__photos img[data-src], "
            ".gallery__photos picture source[srcset]"
        )
        for el in s.select(sel):
            # preferuj source/srcset (największy wariant), inaczej img src/data-src
            if el.name == "source":
                cand = _best_from_srcset(el.get("srcset") or "")
            else:
                cand = el.get("data-src") or el.get("src")

            if not cand:
                continue

            # odfiltruj logotypy, ikony, placeholdery, 1x1
            low = cand.lower()
            if ("nuxt-assets" in low or "svg" in low or "icon" in low or
                "placeholder" in low or "1x1" in low or "adsystem" in low or
                "onet-ad" in low):
                continue

            u = normalize_url(join_url("https://www.morizon.pl", cand))
            if u in seen:
                continue
            seen.add(u)
            urls.append(u)

        # 3) Zbuduj strukturę wyjściową
        return [{"seq": i, "url": u} for i, u in enumerate(urls)]



    
    def write_photo_links_csv(
        self, *, offer_id: str, offer_url: str,
        photo_list: list[PhotoMeta], limit: int | None = None,
    ) -> Path:
        assert self.out_dir is not None
        rows: list[dict] = []
        cap = len(photo_list) if limit is None else min(limit, len(photo_list))
        seq_auto = 0
        for i in range(cap):
            ph = photo_list[i]
            if isinstance(ph, dict):
                url = ph.get("url") or ""; seq = ph.get("seq")
            elif isinstance(ph, str):
                url = ph; seq = None
            else:
                try:
                    url = ph[1]; seq = ph[0] if isinstance(ph[0], int) else None
                except Exception:
                    continue
            if not url:
                continue
            if seq is None:
                seq = seq_auto
            rows.append({"offer_id": offer_id, "seq": int(seq), "url": url})
            seq_auto = int(seq) + 1
        from scrapper.core.storage import photos_csv_path, append_rows_csv
        path = photos_csv_path(self.out_dir)
        append_rows_csv(path, rows, header=["offer_id", "seq", "url"])
        return path