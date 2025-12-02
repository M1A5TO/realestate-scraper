# scrapper/adapters/gratka.py
from __future__ import annotations

import json
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable, Any
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit
from scrapper.adapters.base import BaseAdapter, OfferIndex, PhotoMeta
from scrapper.core.http import HttpClient, join_url
from scrapper.core.parse import soup, select_text
from scrapper.core.storage import photos_csv_path, append_rows_csv, urls_csv_path
from scrapper.core.log import get_logger

log = get_logger("scrapper")

PRICE_TOTAL_RE = re.compile(r"([\d\s.,]+)\s*(?:zł|PLN)", re.I)
PRICE_PERM2_RE = re.compile(r"([\d\s.,]+)\s*zł\s*/\s*m", re.I)
_STREET_PREFIXES = ("ul.", "ulica", "al.", "aleja", "aleje", "pl.", "plac", "os.", "osiedle")
_PL_DATE_RE = re.compile(
    r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?\s*$"
)

def _addr_has(addr: dict, keys: list[str], needle_norm: str | None) -> bool:
    """
    Sprawdza, czy w którymś z podanych pól adresu występuje (po normalizacji) zadany fragment.
    Używa tego samej normalizacji co _norm().
    """
    if not needle_norm:
        return True  # nic do sprawdzania
    for k in keys:
        v = addr.get(k)
        if not v:
            continue
        if needle_norm in _norm(v):
            return True
    return False


def _to_iso_datetime(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    # 1) już ISO?
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return s
    except Exception:
        pass
    # 2) dd.mm.yyyy [HH:MM[:SS]]
    m = _PL_DATE_RE.match(s)
    if m:
        dd, mm, yyyy, hh, mi, ss = m.groups()
        dd = int(dd); mm = int(mm); yyyy = int(yyyy)
        hh = int(hh) if hh else 0
        mi = int(mi) if mi else 0
        ss = int(ss) if ss else 0
        try:
            dt = datetime(yyyy, mm, dd, hh, mi, ss)
            # zwróć ISO bez strefy (jak w innych adapterach)
            return dt.strftime("%Y-%m-%dT%H:%M:%S") if (hh or mi or ss) else dt.strftime("%Y-%m-%d")
        except Exception:
            return None
    # 3) inne lokalne formaty? spróbuj kilku znanych masek
    for fmt in ("%d.%m.%Y", "%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S") if " %H" in fmt else dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def _slug(s: str) -> str:
    import re, unicodedata
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

def _norm(s: str) -> str:
    import unicodedata, re
    s = (s or "").lower().strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s).strip()

def _city_in_text(text: str, city: str) -> bool:
    t = _norm(text)
    c = _norm(city)
    # dopasuj pełne słowo lub prefix ("gdansk", "gdansk wrzeszcz")
    return f" {c} " in f" {t} " or t.startswith(c) or c in t.split(" ")


def _coerce_float(v):
    try:
        if v is None:
            return None
        return float(str(v).replace(" ", "").replace(",", "."))
    except Exception:
        return None

def _coerce_int(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None

def _is_plausible_pl(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    return 48.5 <= lat <= 55.5 and 14.0 <= lon <= 24.5

# --- lokalne helpery (brak w core.* w Twojej wersji) ---

def normalize_url(u: str) -> str:
    """Prosta normalizacja do deduplikacji: usuń spacje, fragment i trailing slash."""
    from urllib.parse import urlsplit, urlunsplit
    if not u:
        return ""
    u = u.strip()
    parts = list(urlsplit(u))
    parts[2] = parts[2].rstrip("/")              # path bez końcowego /
    parts[3] = "&".join(sorted(filter(None, parts[3].split("&"))))  # posortuj query
    parts[4] = ""                                # bez fragmentu
    return urlunsplit(parts)

def _best_from_srcset(srcset: str) -> str | None:
    """Wybierz największy wariant z listy srcset."""
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
            except Exception:
                w = 0
        if w > best_w:
            best, best_w = url, w
    return best

def clean_spaces(s: str | None) -> str | None:
    if s is None:
        return None
    import re
    out = re.sub(r"\s+", " ", s).strip()
    return out if out else None

def only_digits_float(s: str | None) -> float | None:
    """Wydobądź pierwszą liczbę (z , lub .) z tekstu i zwróć jako float."""
    if not s:
        return None
    import re
    m = re.search(r"[\d\s.,]+", s)
    if not m:
        return None
    txt = m.group(0).replace(" ", "").replace(",", ".")
    try:
        return float(txt)
    except Exception:
        return None



def _extract_ld_json_blocks(html: str) -> list[dict]:
    out: list[dict] = []
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.I|re.S):
        try:
            txt = m.group(1).strip()
            data = json.loads(txt)
            if isinstance(data, dict):
                out.append(data)
            elif isinstance(data, list):
                out.extend([d for d in data if isinstance(d, dict)])
        except Exception:
            pass
    return out

def _from_ld(block: dict) -> dict:
    out: dict[str, Any] = {}
    try:
        # Tytuł
        if block.get("name"):
            out["title"] = clean_spaces(str(block["name"]))
        # Cena z offers
        offers = block.get("offers") or block.get("offer")
        if isinstance(offers, dict):
            pa = _coerce_float(offers.get("price"))
            if pa is not None:
                out["price_amount"] = pa
            cur = offers.get("priceCurrency")
            if cur:
                out["price_currency"] = str(cur).upper()

        # Metryki
        fs = block.get("floorSize") or {}
        if isinstance(fs, dict) and "value" in fs:
            out["area_m2"] = _coerce_float(fs.get("value"))
        nr = block.get("numberOfRooms")
        if nr is not None:
            out["rooms"] = _coerce_float(nr)

        # Adres
        addr = block.get("address") or {}
        if isinstance(addr, dict):
            out.setdefault("city", clean_spaces(str(addr.get("addressLocality") or "")) or None)
            if addr.get("streetAddress"):
                out.setdefault("street", clean_spaces(str(addr["streetAddress"])))

        # Daty
        if block.get("datePosted"):
            out["posted_at"] = str(block["datePosted"]).strip()
        if block.get("dateModified"):
            out["updated_at"] = str(block["dateModified"]).strip()

        # GEO
        geo = block.get("geo") or {}
        if isinstance(geo, dict):
            la = _coerce_float(geo.get("latitude"))
            lo = _coerce_float(geo.get("longitude"))
            if _is_plausible_pl(la, lo):
                out["lat"], out["lon"] = la, lo

        # Zdjęcia z LD
        imgs = block.get("image") or block.get("photos") or []
        if isinstance(imgs, list):
            photos = []
            for im in imgs:
                if isinstance(im, str):
                    photos.append(im)
                elif isinstance(im, dict) and im.get("url"):
                    photos.append(im["url"])
            if photos:
                out["photos_from_json"] = photos
        elif isinstance(imgs, str):
            out["photos_from_json"] = [imgs]
    except Exception:
        pass
    return out

def _extract_geo_any(html: str) -> tuple[Optional[float], Optional[float]]:
    # 1) LD/DOM klasyczne
    la, lo = _extract_geo_from_dom(html)
    if _is_plausible_pl(la, lo):
        return la, lo

    # 2) __NUXT__ lub inny JSON: pary lat/lon
    for pat in [
        r'"latitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1[^{}]{0,200}?"longitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\3',
        r'"lat"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1[^{}]{0,200}?"(?:lng|lon|long)"\s*:\s*("?)(-?\d+(?:\.\d+)?)\3',
    ]:
        m = re.search(pat, html, re.I | re.S)
        if m:
            la = _coerce_float(m.group(2)); lo = _coerce_float(m.group(4))
            if _is_plausible_pl(la, lo):
                return la, lo

    # 3) coordinates/center jako lista; spróbuj [lon, lat] i [lat, lon]
    for key in ["coordinates", "center", "position", "lngLat", "latLng"]:
        m = re.search(rf'"{key}"\s*:\s*\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]', html, re.I)
        if m:
            a = _coerce_float(m.group(1)); b = _coerce_float(m.group(2))
            # sprawdź obie kolejności
            if _is_plausible_pl(b, a):
                return b, a
            if _is_plausible_pl(a, b):
                return a, b

    # 4) URL-e map w atrybutach (iframe/src) z lat/lng albo ll=lat,lon
    m = re.search(r'(?:[?&](?:lat|latitude)=(-?\d+(?:\.\d+)?).{0,40}(?:lng|lon|longitude)=(-?\d+(?:\.\d+)?))', html, re.I)
    if m:
        la = _coerce_float(m.group(1)); lo = _coerce_float(m.group(2))
        if _is_plausible_pl(la, lo):
            return la, lo
    m = re.search(r'[?&]ll=(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)', html, re.I)
    if m:
        la = _coerce_float(m.group(1)); lo = _coerce_float(m.group(2))
        if _is_plausible_pl(la, lo):
            return la, lo

    return None, None

def _extract_geo_from_dom(html: str) -> tuple[Optional[float], Optional[float]]:
    # typowe wzorce jak w morizon
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
    # Mapbox/Leaflet
    m = re.search(r'L\.marker\(\s*\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]', html, re.I)
    if m:
        la, lo = _coerce_float(m.group(1)), _coerce_float(m.group(2))
        if _is_plausible_pl(la, lo):
            return la, lo
    m = re.search(r'setLngLat\(\s*\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]\)', html, re.I)
    if m:
        lo, la = _coerce_float(m.group(1)), _coerce_float(m.group(2))
        if _is_plausible_pl(la, lo):
            return la, lo
    # Spróbuj znaleźć pary "latitude"/"longitude" lub "lat"/"lng" w JSON
    try:
        for m in re.finditer(r'"latitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1.*?"longitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\3', html, re.I|re.S):
            la = _coerce_float(m.group(2)); lo = _coerce_float(m.group(4))
            if _is_plausible_pl(la, lo):
                return la, lo
        for m in re.finditer(r'"lat"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1.*?"(?:lng|lon|long)"\s*:\s*("?)(-?\d+(?:\.\d+)?)\3', html, re.I|re.S):
            la = _coerce_float(m.group(2)); lo = _coerce_float(m.group(4))
            if _is_plausible_pl(la, lo):
                return la, lo
    except Exception:
        pass
    return None, None
def _osm_geocode_pl(http: HttpClient, *, street: str | None, district: str | None, city: str | None) -> tuple[Optional[float], Optional[float]]:
    """
    Geokodowanie z Nominatim dla adresów w Polsce:
    - najpierw próbujemy najbardziej szczegółowych zapytań (ulica + dzielnica + miasto),
    - potem ulica + miasto,
    - potem dzielnica + miasto,
    - na końcu (city-only) TYLKO jeśli naprawdę nie mamy ulicy ani dzielnicy.
    Wynik musi pasować do miasta, a przy bardziej szczegółowych zapytaniach także do ulicy / dzielnicy.
    """
    street_n   = _norm(street)   if street   else None
    district_n = _norm(district) if district else None
    city_n     = _norm(city)     if city     else None

    q_specs: list[tuple[str, dict]] = []

    # 1) ulica + dzielnica + miasto
    if street and district and city:
        q_specs.append((
            ", ".join([street, district, city, "Polska"]),
            {"need_city": True, "need_street": True, "need_district": False},
        ))

    # 2) ulica + miasto
    if street and city:
        q_specs.append((
            ", ".join([street, city, "Polska"]),
            {"need_city": True, "need_street": True, "need_district": False},
        ))

    # 3) dzielnica + miasto
    if district and city:
        q_specs.append((
            ", ".join([district, city, "Polska"]),
            {"need_city": True, "need_street": False, "need_district": True},
        ))

    # 4) tylko miasto – TYLKO jeśli nie mamy ani ulicy, ani dzielnicy
    if city and not (street or district):
        q_specs.append((
            ", ".join([city, "Polska"]),
            {"need_city": True, "need_street": False, "need_district": False},
        ))

    from urllib.parse import urlencode
    base_url = "https://nominatim.openstreetmap.org/search"

    for q, spec in q_specs:
        try:
            url = base_url + "?" + urlencode({
                "q": q,
                "format": "jsonv2",
                "limit": "3",
                "countrycodes": "pl",
                "addressdetails": "1",
            })
            data = http.get(url, accept="application/json").json()
        except Exception:
            continue

        if not isinstance(data, list):
            continue

        for rec in data:
            if not isinstance(rec, dict):
                continue

            la = _coerce_float(rec.get("lat"))
            lo = _coerce_float(rec.get("lon"))
            if not _is_plausible_pl(la, lo):
                continue

            addr = rec.get("address") or {}
            if not isinstance(addr, dict):
                addr = {}

            # 1) Miasto musi pasować (city/town/village/municipality lub display_name)
            if spec.get("need_city") and city_n:
                if not _addr_has(addr, ["city", "town", "village", "municipality", "county"], city_n):
                    dn = rec.get("display_name") or ""
                    if city_n not in _norm(dn):
                        continue

            # 2) Ulica – wymagamy przy zapytaniach ze street
            if spec.get("need_street") and street_n:
                if not _addr_has(addr, ["road", "pedestrian", "footway", "residential"], street_n):
                    dn = rec.get("display_name") or ""
                    if street_n not in _norm(dn):
                        continue

            # 3) Dzielnica – wymagamy tylko, gdy spec mówi need_district=True
            if spec.get("need_district") and district_n:
                if not _addr_has(addr, ["suburb", "neighbourhood", "city_district", "borough"], district_n):
                    dn = rec.get("display_name") or ""
                    if district_n not in _norm(dn):
                        continue

            # Jeśli dotarliśmy tutaj, adres wygląda sensownie
            return la, lo

    # nic sensownego nie znaleziono
    return None, None


def _price_from_nodes(soup_obj) -> tuple[Optional[float], Optional[float], Optional[str]]:
    total_txt = select_text(soup_obj, ".price-row__wrapper, [data-cy='priceRowPrice']")
    ppm2_txt = select_text(soup_obj, ".price-row__price-m2, [data-cy='detailsRowTextPriceM2Formatted']")
    pa = None; ppm2 = None; cur = None

    # blokuj „zarezerwowano”, „zarezerwowane”, „zapytaj o cenę”
    txt_all = " ".join([t for t in [total_txt, ppm2_txt] if t]).lower()
    if any(k in txt_all for k in ("zarezerw", "zapytaj o cen", "brak ceny", "cena do uzgodnienia")):
        return None, None, None

    if total_txt:
        m = PRICE_TOTAL_RE.search(total_txt)
        if m:
            pa = _coerce_float(m.group(1)); cur = "PLN"
    if ppm2_txt:
        m2 = PRICE_PERM2_RE.search(ppm2_txt)
        if m2:
            ppm2 = _coerce_float(m2.group(1))
    return pa, ppm2, cur


def _area_rooms_from_nodes(soup_obj) -> tuple[Optional[float], Optional[float]]:
    """
    Spróbuj wyciągnąć powierzchnię i liczbę pokoi:
    1) ze starych pól [data-cy='detailsRowTextNumberOfRooms'] / [data-cy='detailsRowTextArea']
    2) z nowego bloku .details-highlighted-parameters__item (Pokoje / Powierzchnia)
    """
    rooms: Optional[float] = None
    area: Optional[float] = None

    # --- 1) Stare selektory data-cy (dla kompatybilności) ---
    rooms_txt = select_text(soup_obj, "[data-cy='detailsRowTextNumberOfRooms']")
    area_txt  = select_text(soup_obj, "[data-cy='detailsRowTextArea']")

    if rooms_txt:
        m = re.search(r"(\d+)", rooms_txt)
        if m:
            rooms = _coerce_float(m.group(1))

    if area_txt:
        # użyj helpera, który radzi sobie z przecinkami/spacjami
        a = only_digits_float(area_txt)
        if a is not None:
            area = a

    # --- 2) Nowy layout: "highlighted parameters" ---
    # <div class="details-highlighted-parameters__item-label">Pokoje</div>
    # <div class="details-highlighted-parameters__item-value"><strong>4</strong></div>
    if rooms is None or area is None:
        for item in soup_obj.select(".details-highlighted-parameters__item"):
            label_el = item.select_one(".details-highlighted-parameters__item-label")
            value_el = item.select_one(".details-highlighted-parameters__item-value")
            if not (label_el and value_el):
                continue

            label = (label_el.get_text(" ", strip=True) or "").lower()
            value = value_el.get_text(" ", strip=True) or ""

            # Liczba pokoi
            if rooms is None and "pokoj" in label:
                m = re.search(r"(\d+)", value)
                if m:
                    rooms = _coerce_float(m.group(1))

            # Powierzchnia (np. "71,31 m² + balkon 3,62 m²")
            if area is None and ("powierzchnia" in label or "m²" in value.lower()):
                a = only_digits_float(value)
                if a is not None:
                    area = a

    return area, rooms

def _clean_street(s: str | None) -> str | None:
    if not s:
        return None
    t = s.strip()
    # usuń przecinki i wielokrotne spacje
    t = re.sub(r"\s*,\s*", " ", t)
    t = re.sub(r"\s+", " ", t)
    # usuń prefiksy typu „ul.”, „al.”, „plac”
    low = t.lower()
    for p in _STREET_PREFIXES:
        if low.startswith(p + " "):
            t = t[len(p)+1:].lstrip()
            break
    # jeśli po czyszczeniu jest puste albo to same cyfry → None
    if not re.search(r"[A-Za-zĄąĆćĘęŁłŃńÓóŚśŻżŹź]", t):
        return None
    return t

def _address_from_nodes(soup_obj) -> tuple[Optional[str], Optional[str], Optional[str]]:
    # 1) JSON-LD (najpewniejsze)
    street = None
    city = None
    district = None
    for ld in soup_obj.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(ld.get_text() or "{}")
        except Exception:
            continue
        blocks = data if isinstance(data, list) else [data]
        for b in blocks:
            if not isinstance(b, dict):
                continue
            addr = b.get("address") or {}
            if isinstance(addr, dict):
                city = city or clean_spaces(addr.get("addressLocality"))
                street = street or clean_spaces(addr.get("streetAddress"))
        if street and city:
            break

    # 2) Header lokalizacji na stronie:
    # <h2 class="location-row__header--with-map location-row__header">
    #   <span>Chmielna</span>
    #   <div class="location-row__main-location">
    #     <span>Gdańsk,</span><span>Stare Miasto</span>
    #   </div>
    # </h2>
    if not (city and (district or street)):
        hdr = (
            soup_obj.select_one(".location-row__header--with-map.location-row__header")
            or soup_obj.select_one(".location-row__header")
            or soup_obj.select_one(".location-row__left h2")
            or soup_obj.select_one(".location-row__second_column h2")
        )
        if hdr:
            spans = [
                el.get_text(" ", strip=True).rstrip(",")
                for el in hdr.select("span")
                if el.get_text(strip=True)
            ]
            # Jeśli mamy 3 spany: [ulica, miasto, dzielnica]
            if len(spans) >= 3:
                street = street or clean_spaces(spans[0])
                city = city or clean_spaces(spans[1])
                district = district or clean_spaces(spans[2])
            elif len(spans) == 2:
                # Bez ulicy: [miasto, dzielnica]
                city = city or clean_spaces(spans[0])
                district = district or clean_spaces(spans[1])
            elif len(spans) == 1:
                city = city or clean_spaces(spans[0])

            # Dodatkowo spróbuj wyciągnąć miasto/dzielnicę z bloku location-row__main-location
            ml = hdr.select_one(".location-row__main-location, .main-location")
            if ml:
                sub_spans = [
                    el.get_text(" ", strip=True).rstrip(",")
                    for el in ml.select("span")
                    if el.get_text(strip=True)
                ]
                if sub_spans:
                    city = city or clean_spaces(sub_spans[0])
                    if len(sub_spans) >= 2:
                        district = district or clean_spaces(sub_spans[1])

    # 3) Breadcrumb/alternatywy
    if not (city and (district or street)):
        bc = soup_obj.select("[data-cy='breadcrumb'] li, nav.breadcrumbs li, .breadcrumbs li")
        if bc:
            crumbs = [li.get_text(" ", strip=True).rstrip(",") for li in bc if li.get_text(strip=True)]
            # heurystyka: ostatni element bywa ulicą, przedostatni dzielnicą, pierwszy miastem
            if crumbs:
                city = city or clean_spaces(crumbs[0])
                if len(crumbs) >= 2:
                    district = district or clean_spaces(crumbs[-2])
                if len(crumbs) >= 1:
                    street = street or clean_spaces(crumbs[-1])

    # 4) Inne pola
    if not street:
        cand = soup_obj.select_one("[data-cy='address'], .property-address, .details-address")
        if cand:
            street = clean_spaces(cand.get_text(" ", strip=True))

    # Normalizacja ulicy i sanity-check względem miasta
    street = _clean_street(street)
    if street and city and street.lower() == (city or "").lower():
        street = None
    if street and district and street.lower() == (district or "").lower():
        street = None

    return clean_spaces(city), clean_spaces(district), street

def _offer_id_from_dom(soup_obj) -> Optional[str]:
    # wg prompt: <div data-cy="propertyNumber">gratka-23610653</div>
    oid = select_text(soup_obj, "[data-cy='propertyNumber']")
    return oid or None

def _extract_offer_links_from_listing(html: str, city_name: str) -> list[str]:
    import re
    s = soup(html)
    links: list[str] = []

    # Karty ogłoszeń – różne templatki: preferuj elementy z data-cy, potem dowolne article.
    cards = s.select("article[data-cy='listing-item']") or s.select("article")

    for card in cards:
        a = card.select_one("a[href*='/ob/']")
        if not a:
            continue
        href = a.get("href") or ""
        u = normalize_url(join_url("https://gratka.pl", href))
        if not re.search(r"/ob/\d+(?:/|$)", u):
            continue

        # Lokalizacja w karcie: kilka wariantów
        loc = (
            card.select_one("[data-cy='listing-item-location']") or
            card.select_one(".listing-item__location") or
            card.select_one(".teaser__location") or
            card.select_one(".teaser-location") or
            card
        )
        loc_txt = loc.get_text(" ", strip=True) if hasattr(loc, "get_text") else str(loc)

        if _city_in_text(loc_txt, city_name):
            links.append(u)

    # Fallback: jeśli nic nie przeszło filtrowania po mieście, weź wszystkie /ob/ z całej strony
    if not links:
        for a in s.select("a[href*='/ob/']"):
            href = a.get("href") or ""
            u = normalize_url(join_url("https://gratka.pl", href))
            if re.search(r"/ob/\d+(?:/|$)", u):
                links.append(u)

    # deduplikacja po id
    uniq, seen = [], set()
    for u in links:
        m = re.search(r"/ob/(\d+)", u)
        key = f"gratka-{m.group(1)}" if m else u
        if key in seen:
            continue
        seen.add(key)
        uniq.append(u)
    return uniq

def _to_photo_url(u: str) -> str:
    if not u:
        return u
    if "/photo" in u:
        return u
    sp = urlsplit(u)
    path = sp.path
    if not path.endswith("/"):
        path += "/"
    path += "photo"
    return urlunsplit((sp.scheme, sp.netloc, path, sp.query, ""))

@dataclass
class GratkaAdapter(BaseAdapter):
    source: str = "gratka"
    http: Optional[HttpClient] = None
    out_dir: Optional[Path] = None
    use_osm_geocode: bool = False

    def with_deps(self, *, http: HttpClient, out_dir: Path, use_osm_geocode: bool = False):
        self.http = http
        self.out_dir = out_dir
        self.use_osm_geocode = use_osm_geocode
        return self

    # ---------- DISCOVER ----------
    def discover(self, *, city: str, deal: str, kind: str, max_pages: int = 1) -> Iterable[OfferIndex]:
        assert self.http is not None
        rows: list[OfferIndex] = []
        dedup: set[str] = set()

        # kategorie: "mieszkania", "domy", "dzialki", "lokale" → mapuj po Twojej logice
        kind_slug = {
            "mieszkania": "mieszkania",
            "mieszkanie": "mieszkania",
            "domy": "domy",
            "dom": "domy",
            "dzialki": "dzialki",
            "działki": "dzialki",
            "lokale": "lokale",
            "lokal": "lokale",
        }.get((kind or "").lower(), "mieszkania")

        city_slug = _slug(city)

        for page in range(1, int(max_pages) + 1):
            url = f"https://gratka.pl/nieruchomosci/{kind_slug}/{city_slug}?page={page}"
            try:
                html = self.http.get(url, accept="text/html").text
            except Exception as e:
                log.warning("discover_fetch_fail", extra={"extra": {"url": url, "err": type(e).__name__}})
                continue

            links = _extract_offer_links_from_listing(html, city)
            kept = 0
            for href in links:
                m = re.search(r"/ob/(\d+)", href)
                offer_id = f"{m.group(1)}" if m else ""
                if offer_id and offer_id in dedup:
                    continue
                dedup.add(offer_id or href)
                rows.append({"offer_url": href, "offer_id": offer_id, "page_idx": page})
                kept += 1

            log.info("discover_page", extra={"extra": {"page": page, "found": len(links), "kept": kept}})

        return rows


    def write_urls_csv(self, rows: Iterable[OfferIndex]) -> Path:
        assert self.out_dir is not None
        path = urls_csv_path(self.out_dir)
        # ujednolicone kolumny jak w pozostałych adapterach
        data = []
        for r in rows:
            data.append({
                "offer_url": r.get("offer_url", ""),
                "offer_id":  r.get("offer_id", ""),
                "page_idx":  r.get("page_idx", 0),
                "source":    self.source,
            })
        append_rows_csv(path, data, header=["offer_url", "offer_id", "page_idx", "source"])
        return path

    # ---------- DETAIL ----------
    def parse_offer(self, url: str) -> dict:
        assert self.http is not None
        html = self.http.get(url, accept="text/html").text
        s = soup(html)
        out: dict = {}
        out["source"] = self.source  
        out["url"] = url             
        if not out.get("offer_id"):
            m = re.search(r"/ob/(\d+)", url)
            if m:
                out["offer_id"] = f"gratka-{m.group(1)}"
        # 1) LD+JSON
        for block in _extract_ld_json_blocks(html):
            d = _from_ld(block)
            for k, v in d.items():
                out.setdefault(k, v)

        # 2) Tytuł
        t = select_text(s, "h1.page-details__property-title, [data-cy='pageDetailsPropertyTitle']")
        if t:
            out["title"] = t

        # 3) Ceny
        pa, ppm2, cur = _price_from_nodes(s)
        if pa is not None:
            out["price_amount"] = pa
        if ppm2 is not None:
            out["price_per_m2"] = ppm2
        if cur:
            out["price_currency"] = cur

        # 4) Metryki
        area, rooms = _area_rooms_from_nodes(s)
        if area is not None:
            out["area_m2"] = area
        if rooms is not None:
            out["rooms"] = rooms

        # 5) Adres
        city, district, street = _address_from_nodes(s)
        if city: out["city"] = city
        if district: out["district"] = district
        if street: out["street"] = street

        # 6) Numer ogłoszenia
        oid = _offer_id_from_dom(s)
        if oid:
            out["offer_id"] = oid

        # 7) Daty (sekcja „Ogłoszenie”)
        # Data dodania i aktualizacji są w tabeli .information-table__row > [data-cy='itemValue']
        # Uproszczenie: szukamy labelu, potem value
        for row in s.select(".information-table__row"):
            lbl_el = row.select_one(".information-table__cell--label")
            label_txt = clean_spaces(lbl_el.get_text(" ", strip=True) if lbl_el else row.get_text(" ", strip=True)) or ""
            label_low = label_txt.lower()
            val_el = row.select_one("[data-cy='itemValue']")
            val_txt = clean_spaces(val_el.get_text(" ", strip=True) if val_el else "")
            if "data dodania" in label_low and val_txt:
                iso = _to_iso_datetime(val_txt)
                if iso:
                    out["posted_at"] = iso
            elif "aktualizacja" in label_low and val_txt:
                iso = _to_iso_datetime(val_txt)
                if iso:
                    out["updated_at"] = iso

        # 8) GEO: DOM/JSON → OSM fallback
        la, lo = _extract_geo_any(html)
        if _is_plausible_pl(la, lo):
            out["lat"], out["lon"] = la, lo
        else:
            # Gratka obecnie nie podaje jawnie współrzędnych – próbuj geokodować
            la, lo = _osm_geocode_pl(self.http, street=street, district=district, city=city)
            if _is_plausible_pl(la, lo):
                out["lat"], out["lon"] = la, lo



        # 9) Gdy znamy pa i area, uzupełnij ppm2; gdy znamy pa i ppm2, skoryguj area spójną regułą
        if out.get("price_amount") is not None and out.get("area_m2") and not out.get("price_per_m2"):
            try:
                pa = float(out["price_amount"]); ar = float(out["area_m2"])
                if ar > 0:
                    out["price_per_m2"] = round(pa / ar, 2)
            except Exception:
                pass
        try:
            pa = float(out["price_amount"]) if out.get("price_amount") is not None else None
            ppm2 = float(out["price_per_m2"]) if out.get("price_per_m2") is not None else None
            ar = float(out["area_m2"]) if out.get("area_m2") is not None else None
        except Exception:
            pa = ppm2 = ar = None
        if pa and ppm2 and ppm2 > 0:
            ar_calc = pa / ppm2
            if (ar is None) or (abs(ar - ar_calc) / ar_calc > 0.08):
                out["area_m2"] = round(ar_calc, 2)

        return out

    # ---------- PHOTOS ----------
    


    def parse_photos(self, offer_url: str) -> list[str]:
        """
        Zwraca pełną listę URLi zdjęć z podstrony '.../photo'.
        """
        url = _to_photo_url(offer_url)
        resp = self.http.get(url, accept="text/html")
        html = resp.text
        s = soup(html)

        urls: list[str] = []

        # 1) Galeria: przyciski z miniaturami
        # <button class="gallery__photos-item"><img ... data-cy="thumbnail" src ... srcset ...></button>
        for img in s.select(".gallery__photos-item img, img[data-cy='thumbnail']"):
            srcset = img.get("srcset") or ""
            best = _best_from_srcset(srcset)
            if not best:
                best = img.get("src") or ""
            if not best:
                continue
            u = normalize_url(join_url("https://gratka.pl", best))
            urls.append(u)

        # 2) Fallback: wszystkie <img> w kontenerze galerii
        if not urls:
            gal = s.select_one(".gallery, [data-cy='gallery']")
            if gal:
                for img in gal.select("img"):
                    srcset = img.get("srcset") or ""
                    best = _best_from_srcset(srcset) or img.get("src") or ""
                    if best:
                        urls.append(normalize_url(join_url("https://gratka.pl", best)))

        # deduplikacja z zachowaniem kolejności
        seen = set()
        out: list[str] = []
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
        return out


    def write_photo_links_csv(
        self,
        *,
        offer_id: str,
        offer_url: str,
        photo_list: list[PhotoMeta],
        limit: int | None = None,
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

        path = photos_csv_path(self.out_dir)
        append_rows_csv(path, rows, header=["offer_id", "seq", "url"])
        return path
