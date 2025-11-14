# scrapper/adapters/trojmiasto.py
from __future__ import annotations

import json
import re  # Upewnij się, że 're' jest importowane
import urllib.parse
import reverse_geocoder as rg
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from scrapper.adapters.base import BaseAdapter, OfferIndex, PhotoMeta
from scrapper.core.http import HttpClient, join_url
from scrapper.core.parse import soup, select_text
from scrapper.core.dedup import normalize_url
from scrapper.core.log import get_logger
from scrapper.core.storage import (
    append_rows_csv, offers_csv_path, photos_csv_path, urls_csv_path, append_offer_row
)

log = get_logger("scrapper.trojmiasto")

# --- Funkcje pomocnicze ---

# Skopiowane z morizon.py
_PL_BBOX = (49.0, 54.9, 14.0, 24.5)  # min_lat, max_lat, min_lon, max_lon

# NOWY Regex do parsowania linków mapy "W pobliżu"
# np. .../location/54.57011,18.47521/map/...
LOCATION_HREF_RE = re.compile(r"location/([\d\.]+),([\d\.]+)/map")


def _coerce_float(x: Any) -> Optional[float]:
    """
    Konwertuje wartość na float, obsługując różne formaty tekstowe 
    (np. "499 000 zł", "32,25 m²").
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    
    try:
        s = str(x).strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
        m = re.match(r"^[+-]?\d+(?:\.\d+)?", s) 
        return float(m.group(0)) if m else None
    except Exception:
        return None

# Skopiowane z morizon.py
def _is_plausible_pl(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    min_lat, max_lat, min_lon, max_lon = _PL_BBOX
    return (min_lat <= float(lat) <= max_lat) and (min_lon <= float(lon) <= max_lon)


def _offer_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"(ogl\d{6,})", url, re.I)
    return m.group(1) if m else None

def _parse_next_data(html: str) -> dict:
    try:
        s = soup(html)
        script_tag = s.select_one('script#__NEXT_DATA__[type="application/json"]')
        if script_tag and script_tag.string:
            return json.loads(script_tag.string)
    except Exception as e:
        log.warning("parse_next_data_fail", extra={"extra": {"err_type": type(e).__name__, "err_str": str(e)}})
    return {}


def _parse_classic_html(html: str, data: dict[str, Any]) -> None:
    """
    Funkcja zapasowa (fallback) parsująca "klasyczny" widok strony.
    """
    log.debug("parse_offer_classic_fallback", extra={"extra": {"url": data.get("url")}})
    
    bs = soup(html)

    # 1. Cena
    if not data.get("price_amount"):
        price_str = select_text(bs, 'p.xogField__value--bigPrice span')
        price_val = _coerce_float(price_str)
        if price_val:
            data["price_amount"] = price_val
            data["price_currency"] = "PLN"

    # 2. Miasto
    if not data.get("city"):
        city_node = bs.select_one('span.xogField__value--address')
        try:
            if city_node and city_node.contents:
                # Bierzemy tylko pierwszy fragment tekstu (np. "Sopot")
                # i ignorujemy resztę (np. <br>, "Górny Sopot", ...)
                first_text = city_node.contents[0].strip()
                if first_text:
                    data["city"] = first_text
                else:
                    # Fallback, gdyby pierwszy element był pusty
                    data["city"] = city_node.get_text(strip=True)
            elif city_node:
                 data["city"] = city_node.get_text(strip=True)
        except Exception:
             log.warning("parse_classic_city_fail", extra={"extra": {"url": data.get("url")}})

    # 3. Powierzchnia
    if not data.get("area_m2"):
        try:
            area_node = bs.select_one('span.xogField__label:-soup-contains("Powierzchnia")')
            if area_node:
                area_val_node = area_node.find_next_sibling('span', class_='xogField__value--big')
                if area_val_node:
                    data["area_m2"] = _coerce_float(area_val_node.get_text())
        except Exception:
            log.warning("parse_classic_area_fail", extra={"extra": {"url": data.get("url")}})

    # 4. Pokoje
    if not data.get("rooms"):
        try:
            rooms_node = bs.select_one('span.xogField__label:-soup-contains("Liczba pokoi")')
            if rooms_node:
                rooms_val_node = rooms_node.find_next_sibling('span', class_='xogField__value--big')
                if rooms_val_node:
                    data["rooms"] = _coerce_float(rooms_val_node.get_text())
        except Exception:
            log.warning("parse_classic_rooms_fail", extra={"extra": {"url": data.get("url")}})
            
    # 5. Lat/Lon (OSTATECZNA POPRAWKA: Używamy linków "w pobliżu")
    if not _is_plausible_pl(data.get("lat"), data.get("lon")):
        lat_val, lon_val = None, None
        
        # Znajdź pierwszy link do mapy "w pobliżu"
        #
        nearby_link = bs.select_one('a[data-map-point][href*="location/"]')
        if nearby_link:
            href = nearby_link.get("href", "")
            match = LOCATION_HREF_RE.search(href)
            if match:
                lat_val = _coerce_float(match.group(1))
                lon_val = _coerce_float(match.group(2))

        if _is_plausible_pl(lat_val, lon_val):
            data["lat"] = lat_val
            data["lon"] = lon_val
        else:
             log.warning("parse_classic_latlon_fail", extra={"extra": {"url": data.get("url"), "msg": "Failed to find lat/lon in nearby_links (a[data-map-point])"}})


    # 6. Oblicz price_per_m2 (jeśli to możliwe)
    if not data.get("price_per_m2"):
        if data.get("price_amount") and data.get("area_m2") and data["area_m2"] > 0:
            data["price_per_m2"] = round(data["price_amount"] / data["area_m2"], 2)


@dataclass
class TrojmiastoAdapter(BaseAdapter):
    source: str = "trojmiasto"
    http: Optional[HttpClient] = None
    out_dir: Optional[Path] = None

    def with_deps(self, http: HttpClient, out_dir: Path, **kwargs):
        self.http = http
        self.out_dir = out_dir
        return self

    def discover(self, *, city: str, deal: str, kind: str, max_pages: int = 1) -> Iterable[OfferIndex]:
        assert self.http is not None
        deal_map = {"sprzedaz": "sprzedaż", "wynajem": "wynajem"}
        kind_map = {
            "mieszkanie": "Mieszkanie", "dom": "Dom",
            "dzialka": "Działka", "lokal": "Lokal",
        }
        deal_slug = deal_map.get(str(deal).lower(), "sprzedaż")
        kind_slug = kind_map.get(str(kind).lower(), "Mieszkanie")
        search_slug = f"{kind_slug} na {deal_slug}"
        search_slug_encoded = urllib.parse.quote(search_slug)
        base_url = f"https://ogloszenia.trojmiasto.pl/nieruchomosci/s,{search_slug_encoded}.html"
        rows: list[OfferIndex] = []
        dedup_ids: set[str] = set()
        for page in range(1, int(max_pages) + 1):
            page_url = f"{base_url}?strona={page}" if page > 1 else base_url
            try:
                html = self.http.get(page_url, accept="text/html").text
            except Exception as e:
                log.warning("discover_fetch_fail", extra={"extra": {"url": page_url, "err": type(e).__name__}})
                continue
            bs = soup(html)
            links = bs.select('a[href*="/nieruchomosci-"][href*="ogl"]')
            found_links_count = 0
            for a in links:
                href = a.get("href")
                if not href: continue
                full_url = normalize_url(join_url(base_url, href))
                oid = _offer_id_from_url(full_url)
                if not oid or oid in dedup_ids: continue
                dedup_ids.add(oid)
                rows.append({"offer_url": full_url, "offer_id": oid, "page_idx": page})
                found_links_count += 1
            log.info("discover_page", extra={"extra": {"page": page, "found": found_links_count, "kept": len(rows)}})
            if not links or found_links_count == 0:
                log.info("discover_finish_no_more_links", extra={"extra": {"page": page}})
                break
        return rows

    # --- OSTATECZNA WERSJA parse_offer (hybrydowa) ---
    
    def parse_offer(self, url: str) -> dict:
        """Pobiera i parsuje dane ze strony oferty (obsługuje __NEXT_DATA__ i klasyczny HTML)."""
        assert self.http is not None
        url = normalize_url(url)
        html = self.http.get(url, accept="text/html").text

        data: dict[str, Any] = {
            "source": self.source,
            "url": url,
            "offer_id": _offer_id_from_url(url) or "",
        }

        # --- Scieżka 1: "Nowoczesna" strona (__NEXT_DATA__) ---
        next_data = _parse_next_data(html)
        ad_data = {} # Zainicjuj puste
        
        if next_data:
            try:
                # Sprawdź OBA klucze, "advert" jest preferowany
                ad_data = next_data.get("props", {}).get("pageProps", {}).get("advert", {})
                if not ad_data:
                    ad_data = next_data.get("props", {}).get("pageProps", {}).get("ad", {})

                if ad_data:
                    data["title"] = ad_data.get("title")

                    price_data = ad_data.get("price", {})
                    if isinstance(price_data, dict):
                        data["price_amount"] = _coerce_float(price_data.get("value"))
                        data["price_currency"] = price_data.get("currency")

                    data["posted_at"] = ad_data.get("createdAt")
                    data["updated_at"] = ad_data.get("refreshedAt")

                    # UWAGA: 'coordinates' jest często 'null' w __NEXT_DATA__
                    #
                    coords = ad_data.get("location", {}).get("coordinates", {})
                    if isinstance(coords, dict):
                        data["lat"] = _coerce_float(coords.get("latitude"))
                        data["lon"] = _coerce_float(coords.get("longitude"))

                    loc = ad_data.get("location", {})
                    if isinstance(loc, dict):
                        data["city"] = loc.get("city", {}).get("name")
                        data["district"] = loc.get("district", {}).get("name")

                    characteristics = ad_data.get("characteristics", [])
                    if isinstance(characteristics, list):
                        for item in characteristics:
                            if not isinstance(item, dict): continue
                            key = item.get("key")
                            val = item.get("value")
                            if key == "m": # Powierzchnia
                                data["area_m2"] = _coerce_float(val)
                            elif key == "rooms_num": # Pokoje
                                data["rooms"] = _coerce_float(val)
                    
                    if not data.get("price_per_m2"):
                        if data.get("price_amount") and data.get("area_m2") and data["area_m2"] > 0:
                            data["price_per_m2"] = round(data["price_amount"] / data["area_m2"], 2)

            except Exception as e:
                log.error("parse_offer_next_data_extract_fail", extra={"extra": {"url": url, "err": str(e), "err_type": type(e).__name__}})
        
        if not ad_data or not data.get("price_amount") or not _is_plausible_pl(data.get("lat"), data.get("lon")):
            if not next_data:
                log.debug("parse_offer_classic_html", extra={"extra": {"url": url}})
            else:
                log.warning("parse_offer_next_data_empty_or_incomplete", extra={"extra": {"url": url}})
            
            _parse_classic_html(html, data) # Wypełnij brakujące dane

        if _is_plausible_pl(data.get("lat"), data.get("lon")):
            try:
                coords = (data["lat"], data["lon"])
                result = rg.search(coords, mode=1) 

                if result:
                    city_name = result[0].get('name')
                    if city_name:
                        data["city"] = city_name

            except Exception as e:
                log.warning("reverse_geocode_fail", extra={"extra": {"url": url, "err": str(e)}})

        return data
    
    # --- Pozostałe metody (photos i write_*) ---

    def parse_photos(self, html_or_url: str) -> list[PhotoMeta]:
        """Pobiera linki do zdjęć z __NEXT_DATA__."""
        assert self.http is not None
        
        if html_or_url.startswith("http"):
            url = normalize_url(html_or_url)
            try:
                html = self.http.get(url, accept="text/html").text
            except Exception as e:
                log.warning("parse_photos_fetch_fail", extra={"extra": {"url": html_or_url, "err": str(e)}})
                return []
        else:
            html = html_or_url

        next_data = _parse_next_data(html)
        if not next_data:
            # TODO: Dodać fallback dla zdjęć z klasycznego HTML (np. szukanie <img> w galerii)
            log.error("parse_photos_no_next_data", extra={"extra": {"url": html_or_url[:100]}})
            return []

        image_urls: list[str] = []
        try:
            # Spróbujmy ścieżki "advert" (dla nowoczesnych)
            photos_data = next_data.get("props", {}).get("pageProps", {}).get("advert", {}).get("photos", [])
            
            # Fallback dla starszych (może?)
            if not photos_data:
                 photos_data = next_data.get("props", {}).get("pageProps", {}).get("ad", {}).get("photos", [])

            for photo in photos_data:
                if isinstance(photo, dict) and photo.get("url"):
                    image_urls.append(photo["url"])

        except Exception as e:
            log.error("parse_photos_next_data_extract_fail", extra={"extra": {"url": html_or_url[:100], "err": str(e)}})
            return []

        if not image_urls:
            # TODO: Dodać fallback dla zdjęć z klasycznego HTML
            log.warning("parse_photos_no_images_in_next_data", extra={"extra": {"url_or_snippet": html_or_url[:100]}})
            return []

        # Deduplikacja
        seen = set()
        unique_urls = [u for u in image_urls if u not in seen and (seen.add(u) or True)]

        return [{"seq": i, "url": u} for i, u in enumerate(unique_urls)]

    # --- Metody zapisu (bez zmian) ---

    def write_urls_csv(self, rows: Iterable[OfferIndex]) -> Path:
        assert self.out_dir is not None, "out_dir not set. Call with_deps()."
        header = ["offer_url", "offer_id", "page_idx", "source"]
        materialized = [{"source": self.source, **dict(r)} for r in rows or []]
        path = urls_csv_path(self.out_dir)
        append_rows_csv(path, materialized, header)
        return path

    def write_offers_csv(self, rows: list[dict]) -> Path:
        assert self.out_dir is not None
        path = offers_csv_path(self.out_dir)
        for r in rows or []:
            r.pop("first_seen", None)
            r.pop("last_seen", None)
            append_offer_row(path, r)
        return path

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
                except Exception: continue
            if not url: continue
            if seq is None: seq = seq_auto
            rows.append({"offer_id": offer_id, "seq": int(seq), "url": url})
            seq_auto = int(seq) + 1
        path = photos_csv_path(self.out_dir)
        append_rows_csv(path, rows, header=["offer_id", "seq", "url"])
        return path