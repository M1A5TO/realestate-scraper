# adapters/otodom.py
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from bs4 import BeautifulSoup

from scrapper.adapters.base import BaseAdapter, OfferIndex, PhotoMeta
from scrapper.core.dedup import DedupeSet, normalize_url
from scrapper.core.http import HttpClient, join_url
from scrapper.core.images import download_photo
from scrapper.core.parse import find_ld_json_all, select_text, soup
from scrapper.core.storage import append_rows_csv, offers_csv_path, photos_csv_path, urls_csv_path

BASE = "https://www.otodom.pl"

# heurystyka: linki do ofert mają segment /pl/oferta/
OFFER_HREF_RE = re.compile(r"/pl/oferta/[^\"'#?]+")

# czasem ID pojawia się jako sufiks -ID<digits> lub ?unique_id=<digits>
OFFER_ID_RE = re.compile(r"(?:-ID|[?&]unique_id=)([A-Za-z0-9]{4,})")

NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', re.S)

PRICE_TOTAL_RE   = re.compile(r"(\d[\d\s.,]{3,})\s*zł(?!\s*/\s*m(?:2|²))", re.I)
PRICE_PER_M2_RE  = re.compile(r"(\d[\d\s.,]{3,})\s*zł\s*/\s*m(?:2|²)", re.I)


def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[ąćęłńóśżź]", lambda m: {
        "ą":"a","ć":"c","ę":"e","ł":"l","ń":"n","ó":"o","ś":"s","ż":"z","ź":"z"
    }[m.group(0)], s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")
def _json_loads_safe(txt: str) -> Any | None:
    try:
        return json.loads(txt)
    except Exception:
        return None

def _first(v):
    return v[0] if isinstance(v, list) and v else v

def _deepget(d, path, default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur: cur = cur[k]
        else: return default
    return cur

def _parse_next_data(html: str) -> dict[str, Any]:
    print("--- DEBUG: Uruchomiono _parse_next_data ---")
    bs = BeautifulSoup(html, "lxml")
    script_tag = bs.find("script", {"id": "__NEXT_DATA__"})
    if not script_tag or not script_tag.string:
        print("--- DEBUG: Nie znaleziono __NEXT_DATA__ ---")
        return {}
    json_text = script_tag.string

    try:
        jd = json.loads(json_text)
        print("--- DEBUG: Poprawnie sparsowano JSON z __NEXT_DATA__ ---")  # du
    except Exception as e:
        print(f"--- DEBUG: BŁĄD parsowania JSON: {e} ---")  # du
        return {}

    # Otodom zwykle: props.pageProps.ad  (bywają też inne nazwy, dlatego kilka ścieżek)
    ad = (
        _deepget(jd, ["props", "pageProps", "ad"])
        or _deepget(jd, ["props", "pageProps", "classified"])
        or {}
    )
    out: dict[str, Any] = {}
    if not isinstance(ad, dict):
        print("--- DEBUG: Nie znaleziono obiektu 'ad' w oczekiwanej ścieżce ---")  # du
        return out
    print("--- DEBUG: Znaleziono obiekt 'ad' ---")  # du

    # Czy to strona inwestycji/wielolokalowa?
    pp = _deepget(jd, ["props", "pageProps"]) or {}
    multi_units = bool(pp.get("paginatedUnits")) or bool(pp.get("developmentData"))

    # Tytuł, opis
    out["title"] = ad.get("title") or ad.get("name") or ""
    out["description"] = ad.get("description") or ""

    # --- CENY: z NEXT_DATA.topInformation ---
    tmp_price_amount: float | None = None
    tmp_price_per_m2: float | None = None

    ti_list = ad.get("topInformation") or []
    for ti in ti_list:
        lab = (ti.get("label") or "").lower()
        if lab == "price":
            v = (ti.get("values") or [None])[0]
            amt = _coerce_int(v)
            if amt is not None:
                tmp_price_amount = float(amt)
        elif lab in ("price_per_m2", "price_per_sqm", "price_per_square_meter"):
            raw = (ti.get("values") or [None])[0] or ti.get("localizedValue")
            per = _coerce_float(raw)
            if per is not None:
                tmp_price_per_m2 = per

    # Cena: z nagłówka HTML, ale NIE ustawiaj price_amount z banera na stronach multi_units
    price_node_txt = (
        select_text(bs, "[data-cy='adPageHeader-price']") or
        select_text(bs, "[data-testid='ad-price']") or
        select_text(bs, ".price, .price-box, [class*='price']")
    )
    if price_node_txt:
        m_total = PRICE_TOTAL_RE.search(price_node_txt)
        if m_total and not multi_units and tmp_price_amount is None:
            tmp_price_amount = _coerce_float(m_total.group(1))
        else:
            m_pm2 = PRICE_PER_M2_RE.search(price_node_txt)
            if m_pm2 and tmp_price_per_m2 is None:
                tmp_price_per_m2 = _coerce_float(m_pm2.group(1))

    # Lokalizacja (jak było)
    location_data = _deepget(ad, ["location"]) or {}
    print(f"--- DEBUG: location_data: {location_data} ---")  # du
    addr = location_data.get("address") or {}
    print(f"--- DEBUG: addr: {addr} ---")  # du
    city = _deepget(addr, ["city", "name"])
    dist = _deepget(addr, ["district", "name"])
    street = _deepget(addr, ["street", "name"])
    out["city"] = city
    out["district"] = dist
    out["street"] = street

    # Geo
    coords = location_data.get("coordinates") or {}
    print(f"--- DEBUG: coords: {coords} ---")  # du
    out["lat"] = _coerce_float(coords.get("latitude"))
    out["lon"] = _coerce_float(coords.get("longitude"))
    print(f"--- DEBUG: Wyekstrahowano lat={out.get('lat')}, lon={out.get('lon')} ---")  # du

    # Metryki
    area_val = _coerce_float(ad.get("area") or ad.get("usableArea") or ad.get("totalArea"))
    if area_val is not None:
        out["area_m2"] = area_val

    rooms_val = _coerce_int(ad.get("rooms") or ad.get("roomsNumber") or ad.get("numberOfRooms"))
    if rooms_val is not None:
        out["rooms"] = rooms_val
    out["floor"] = _coerce_int(ad.get("floor") or _deepget(ad, ["level", "value"]))
    out["max_floor"] = _coerce_int(ad.get("totalFloors") or ad.get("buildingFloors"))
    out["year_built"] = _coerce_int(ad.get("buildYear") or ad.get("yearBuilt"))

    # FINALIZACJA CEN po znaniu area_m2
    if tmp_price_per_m2 is not None:
        out["price_per_m2"] = tmp_price_per_m2
        if out.get("area_m2") and tmp_price_amount is None:
            tmp_price_amount = float(int(round(tmp_price_per_m2 * float(out["area_m2"]))))

    if tmp_price_amount is not None:
        out["price_amount"] = tmp_price_amount
        out["price_currency"] = "PLN"

    # Typy rynku i nieruchomości
    out["market_type"] = (ad.get("marketType") or ad.get("market") or "").lower() or None
    out["property_type"] = (ad.get("estateType") or ad.get("propertyType") or "").lower() or None
    out["building_type"] = (ad.get("buildingType") or _deepget(ad, ["building", "type"]) or None)

    # Własność
    out["ownership"] = ad.get("ownership") or _deepget(ad, ["legal", "ownership"])

    # Agent / agencja / tel
    contact = ad.get("contact") or {}
    out["agent"] = contact.get("name") or contact.get("agentName")
    out["agency"] = contact.get("agencyName") or _deepget(contact, ["agency", "name"])
    out["phone"] = (contact.get("phone") or contact.get("phoneNumber") or "").strip() or None

    # Daty
    out["posted_at"] = _iso_or_none(ad.get("createdAt") or ad.get("publicationDate"))
    out["updated_at"] = _iso_or_none(ad.get("updatedAt") or ad.get("modificationDate"))

    # Cechy
    feats = ad.get("features") or ad.get("amenities")
    print(f"--- DEBUG: Zwracany słownik (fragment): lat={out.get('lat')}, lon={out.get('lon')}, street={out.get('street')}")  # du
    if isinstance(feats, list):
        out["features"] = sorted([str(x).strip() for x in feats if x and str(x).strip()])
    return out



def _coerce_float(x) -> float | None:
    try:
        s = str(x).strip().replace("\u00A0", " ").replace(" ", "").replace(",", ".")
        # odfiltruj ewentualne sufiksy jak 'zł'
        m = re.match(r"^[+-]?\d+(?:\.\d+)?", s)
        return float(m.group(0)) if m else None
    except Exception:
        return None

def _coerce_int(x) -> int | None:
    try:
        f = _coerce_float(x)
        return int(f) if f is not None else None
    except Exception:
        return None

def _iso_or_none(s: str | None) -> str | None:
    if not s: return None
    for fmt in ("%Y-%m-%d","%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).isoformat()
        except Exception:
            continue
    return None


def _parse_ld_json_offer(html: str) -> dict[str, Any]:
    """
    Szuka bloków LD+JSON. Wyciąga: tytuł, cenę, walutę, adres, geolokację, cechy, zdjęcia, daty.
    Schematy na portalach bywają różne: @type: Offer, Product, RealEstateListing itp.
    """
    blocks = find_ld_json_all(html)
    out: dict[str, Any] = {}
    photos: list[str] = []
    for raw in blocks:
        data = _json_loads_safe(raw)
        if not data: 
            continue
        # Uporządkuj listę możliwych kontenerów
        candidates = data if isinstance(data, list) else [data]
        for d in candidates:
            if not isinstance(d, dict):
                continue
            # 1) Listing/Offer/Product
            # Cena i waluta bywają w Offer.priceSpecification albo Product.offers
            price = None
            currency = None
            if "offers" in d and isinstance(d["offers"], dict):
                ospec = (
                    d["offers"].get("price")
                    or d["offers"].get("priceSpecification", {}).get("price")
                )
                price = ospec if ospec is not None else price
                currency = (
                    d["offers"].get("priceCurrency")
                    or d["offers"].get("priceSpecification", {}).get("priceCurrency")
                )
            if "price" in d and price is None:
                price = d.get("price")
            if "priceCurrency" in d and currency is None:
                currency = d.get("priceCurrency")
            if price is not None:
                out["price_amount"] = _coerce_float(price)
            if currency:
                out["price_currency"] = str(currency).upper()
            if "@graph" in d and isinstance(d["@graph"], list):
                for g in d["@graph"]:
                    if isinstance(g, dict):
                        candidates.append(g)
            # już istniejące odczyty zostaw; dodaj:
            if "numberOfRooms" in d and d["numberOfRooms"] is not None:
                out["rooms"] = _coerce_int(_first(d["numberOfRooms"]))
            if "floorLevel" in d:
                out["floor"] = _coerce_int(_first(d["floorLevel"]))
            if "numberOfFloors" in d:
                out["max_floor"] = _coerce_int(_first(d["numberOfFloors"]))
            if "yearBuilt" in d:
                out["year_built"] = _coerce_int(_first(d["yearBuilt"]))
            if "category" in d:
                out["property_type"] = str(d["category"]).lower()
            # Tytuł / opis
            if "name" in d and not out.get("title"):
                out["title"] = str(d["name"]).strip()
            if "description" in d and not out.get("description"):
                out["description"] = str(d["description"]).strip()

            # Adres i geo
            addr = d.get("address") or {}
            if isinstance(addr, dict):
                out.setdefault("city", addr.get("addressLocality") or addr.get("addressRegion"))
                out.setdefault("street", addr.get("streetAddress"))
            geo = d.get("geo") or {}
            if isinstance(geo, dict):
                out["lat"] = _coerce_float(geo.get("latitude"))
                out["lon"] = _coerce_float(geo.get("longitude"))

            # Daty
            out.setdefault("posted_at", _iso_or_none(d.get("datePosted") or d.get("datePublished")))
            out.setdefault("updated_at", _iso_or_none(d.get("dateModified")))

            # Zdjęcia z LD JSON (lista URL lub obiekty ImageObject)
            imgs = d.get("image") or d.get("photos") or []
            if isinstance(imgs, list):
                for im in imgs:
                    if isinstance(im, str):
                        photos.append(im)
                    elif isinstance(im, dict) and im.get("url"):
                        photos.append(im["url"])

            # Cechy i metryki (opcjonalnie)
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

    # Tytuł
    t = select_text(s, "h1, [data-cy='adpage-header-title'], [data-testid='ad-title']")
    if t: out["title"] = t

    # Cena: tylko węzły cenowe. Bez skanowania całego dokumentu.
    price_node_txt = (
        select_text(s, "[data-cy='adPageHeader-price']") or
        select_text(s, "[data-testid='ad-price']") or
        select_text(s, ".price, .price-box, [class*='price']")
    )
    if price_node_txt:
        m = PRICE_TOTAL_RE.search(price_node_txt)
        if m:
            out["price_amount"] = _coerce_float(m.group(1))
            out["price_currency"] = "PLN"
        else:
            m2 = PRICE_PER_M2_RE.search(price_node_txt)
            if m2:
                out["price_per_m2"] = _coerce_float(m2.group(1))
    txt_all = s.get_text(" ", strip=True)
    cand_price = price_node_txt or txt_all
    m = re.search(r"([\d\s.,]+)\s*(?:zł|PLN)", cand_price or "", re.I)
    if m:
        out["price_amount"] = _coerce_float(m.group(1))
        out["price_currency"] = "PLN"

    # Lokalizacja: breadcrumb / nagłówek
    loc = (
        select_text(s, "[data-cy='adPageHeader-locality']") or
        select_text(s, "[data-testid='ad-locality']") or
        select_text(s, "nav a[href*='/pl/oferty/']")  # okruszki
    )
    if loc: out["city"] = loc

    # Metraż
    # Szukaj „m²” w listach parametrów, a w razie czego w całym tekście:
    params_txt = select_text(s, "[data-testid='ad-params'], .parameters, ul") + " " + txt_all
    m = re.search(r"([\d.,]+)\s*m²|\bm2\b", params_txt.replace(" ", ""), re.I)
    if m:
        out["area_m2"] = _coerce_float(m.group(1))

    # Pokoje
    m = re.search(r"(\d{1,2})\s*(?:pokoje|pokój|pokoi)\b", params_txt, re.I)
    if m:
        out["rooms"] = _coerce_int(m.group(1))

    # Ulica (opcjonalnie)
    street = select_text(s, "[itemprop='streetAddress']") or select_text(s, "[data-testid='address-line']")
    if street: out["street"] = street

    return out


def _offer_id_from_url(url: str) -> str | None:
    m = OFFER_ID_RE.search(url)
    return m.group(1) if m else None

def _kind_path(kind: str) -> str:
    # Otodom używa „mieszkanie”, „dom”
    k = kind.strip().lower()
    return "mieszkanie" if "mieszk" in k else "dom"

def _deal_path(deal: str) -> str:
    # „sprzedaz” lub „wynajem” w ścieżce
    d = deal.strip().lower()
    return "wynajem" if "naj" in d else "sprzedaz"

def _build_listing_url(city: str, deal: str, kind: str, page: int) -> str:
    """Nowy wzorzec listingu: /pl/oferty/{deal}/{kind}/{city_slug}?page=N."""
    city_slug = (
        city.strip().lower()
        .replace(" ", "-")
        .replace("ą", "a").replace("ć", "c").replace("ę", "e").replace("ł", "l")
        .replace("ń", "n").replace("ó", "o").replace("ś", "s").replace("ź", "z").replace("ż", "z")
    )
    base = f"https://www.otodom.pl/pl/oferty/{deal}/{kind}/{city_slug}"
    return f"{base}?page={page}"
def _extract_offer_links(html: str) -> list[str]:
    """Ostrożna ekstrakcja URL-i ofert z listingu. Fallback: każdy <a> dopasowany regexem."""
    s = soup(html)
    hrefs: list[str] = []
    # 1) szybkie przejście po wszystkich <a>
    for a in s.select("a[href]"):
        h = a.get("href", "")
        if OFFER_HREF_RE.search(h):
            hrefs.append(h)
    # 2) także z surowego HTML dla pewności (shadow DOM / data-href itp.)
    hrefs += OFFER_HREF_RE.findall(html)
    # normalizacja i dedupe lokalne
    out = []
    seen = set()
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

    def with_deps(self, http: HttpClient, out_dir: Path) -> OtodomAdapter:
        self.http = http
        self.out_dir = out_dir
        return self

    def discover(self, *, city: str, deal: str, kind: str, max_pages: int = 1) -> Iterable[OfferIndex]:
        assert self.http is not None, "HttpClient not set. Call with_deps()."
        ded = DedupeSet()
        page = 1
        found_total = 0
        while page <= max_pages:
            url = _build_listing_url(city, deal, kind, page)
            resp = self.http.get(url, accept="text/html")
            links = _extract_offer_links(resp.text)
            if not links and page > 1:
                break  # brak wyników na kolejnej stronie → koniec
            for ln in links:
                if ded.seen_url(ln):
                    continue
                idx: OfferIndex = {"offer_url": ln, "page_idx": page}
                oid = _maybe_offer_id(ln)
                if oid:
                    idx["offer_id"] = oid
                found_total += 1
                yield idx
            page += 1

    # Zapis urls.csv jako osobna funkcja narzędziowa (wywoływana z pipelines/discover.py)
    def write_urls_csv(self, rows: Iterable[OfferIndex]) -> Path:
        assert self.out_dir is not None, "out_dir not set. Call with_deps()."
        header = ["offer_url", "offer_id", "page_idx"]
        path = urls_csv_path(self.out_dir)
        append_rows_csv(path, rows, header)
        return path
    
        # —————— SZCZEGÓŁ OFERTY ——————
    def parse_offer(self, url: str) -> dict[str, Any]:
        assert self.http is not None, "HttpClient not set. Call with_deps()."
        url = normalize_url(url)
        r = self.http.get(url, accept="text/html")
        html = r.text

        data = {
            "offer_id": _offer_id_from_url(url) or "",
            "source": self.source,
            "url": url,
        }

        # 1) LD+JSON
        ld = _parse_ld_json_offer(html)
        data.update({k: v for k, v in ld.items() if k != "photos_from_json"})

        nd = _parse_next_data(html)
        for k, v in nd.items():
            if k not in data or data[k] in (None, "", 0):
                data[k] = v

        # 2) Fallback CSS
        fb = _parse_fallback_css(html)
        for k, v in fb.items():
            if k not in data or data[k] in (None, "", 0):
                data[k] = v

        # 3) Normalizacja typów i podstawowe domyślne wartości
        if not data.get("price_currency") and data.get("price_amount"):
            data["price_currency"] = "PLN"
        data.setdefault("title", "")
        data.setdefault("description", "")

        # 4) Pierwsze/ostatnie widzenie — tu nie ustawiamy; pipeline może dodać
        # 5) Kopia surowego LD JSON (zabezpieczenie pod dalsze analizy)
        if "photos_from_json" in ld:
            data["json_raw"] = ""  # unikamy ogromnych rekordów; zostawiamy puste lub skrót
        return data

    def write_offers_csv(self, rows: Iterable[dict[str, Any]]) -> Path:
        """Zapisuje rekordy ofert do offers.csv."""
        assert self.out_dir is not None, "out_dir not set. Call with_deps()."
        header = [
        "offer_id","source","url","title",
        "price_amount","price_currency","price_per_m2",
        "property_type","market_type","city","district","street","lat","lon",
        "area_m2","rooms","floor","max_floor","year_built","building_type",
        "ownership","agent","agency","phone","description","features","json_raw",
        "posted_at","updated_at","first_seen","last_seen"
        ]
        path = offers_csv_path(self.out_dir)
        append_rows_csv(path, rows, header)
        return path

    # —————— ZDJĘCIA ——————
    def parse_photos(self, html_or_url: str) -> list[PhotoMeta]:
        """
        Zwraca listę PhotoMeta: seq,url,(opcjonalnie width,height).
        Preferuje listę z LD+JSON; fallback: atrybuty <img>, data-*.
        """
        assert self.http is not None, "HttpClient not set. Call with_deps()."
        if html_or_url.startswith("http"):
            html = self.http.get(html_or_url, accept="text/html").text
        else:
            html = html_or_url

        out: list[PhotoMeta] = []
        # 1) LD+JSON zdjęcia
        ld = _parse_ld_json_offer(html)
        photos = ld.get("photos_from_json", []) if isinstance(ld, dict) else []
        for i, u in enumerate(photos):
            if isinstance(u, str):
                out.append({"seq": i, "url": normalize_url(join_url(BASE, u))})

        # 2) Fallback: IMG w treści
        if not out:
            s = soup(html)
            seq = 0
            for im in s.select(
                "img[src], img[data-src], img[data-lazy], img[data-cy='gallery-image']"
            ):                
                u = im.get("src") or im.get("data-src") or im.get("data-lazy")
                if not u: 
                    continue
                u = normalize_url(join_url(BASE, u))
                # prosta filtracja miniaturek
                if "thumb" in u or "mini" in u:
                    continue
                out.append({"seq": seq, "url": u})
                seq += 1

        # 3) Dedupe po URL
        seen=set()
        uniq: list[PhotoMeta] = []
        for ph in out:
            u = ph["url"]
            if u in seen: 
                continue
            seen.add(u)
            uniq.append(ph)
        return uniq

    def download_and_write_photos(
        self,
        *,
        offer_id: str,
        offer_url: str,
        photo_list: list[PhotoMeta],
        img_root: Path,
        limit: int | None = None,
    ) -> Path:
        """
        Pobiera zdjęcia wg listy, zapisuje do katalogu {source}/{offer_id}/, 
        tworzy wpisy w photos.csv.
        """
        assert self.http is not None and self.out_dir is not None, "Deps not set."
        rows = []
        count = 0
        for ph in photo_list:
            if limit is not None and count >= limit:
                break
            seq = int(ph.get("seq", count))
            url = ph["url"]
            try:
                res = download_photo(self.http, url, img_root, self.source, offer_id, seq)
                rows.append({
                    "offer_id": offer_id,
                    "source": self.source,
                    "seq": seq,
                    "url": url,
                    "local_path": str(res.path),
                    "width": ph.get("width"),
                    "height": ph.get("height"),
                    "bytes": res.bytes,
                    "hash": res.sha256,
                    "status": res.status,
                    "downloaded_at": datetime.utcnow().isoformat(timespec="seconds")+"Z",
                })
            except Exception:
                rows.append({
                    "offer_id": offer_id,
                    "source": self.source,
                    "seq": seq,
                    "url": url,
                    "local_path": "",
                    "width": ph.get("width"),
                    "height": ph.get("height"),
                    "bytes": 0,
                    "hash": "",
                    "status": "failed",
                    "downloaded_at": datetime.utcnow().isoformat(timespec="seconds")+"Z",
                })
            count += 1
        header = [
            "offer_id",
            "source",
            "seq",
            "url",
            "local_path",
            "width",
            "height",
            "bytes",
            "hash",
            "status",
            "downloaded_at",
        ]
        path = photos_csv_path(self.out_dir)
        append_rows_csv(path, rows, header)
        return path
