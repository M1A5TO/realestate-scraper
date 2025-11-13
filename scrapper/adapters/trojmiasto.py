# scrapper/adapters/trojmiasto.py
from __future__ import annotations

import json
import re
import urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from scrapper.adapters.base import BaseAdapter, OfferIndex, PhotoMeta
from scrapper.core.http import HttpClient, join_url
from scrapper.core.parse import soup
from scrapper.core.dedup import normalize_url
from scrapper.core.log import get_logger
from scrapper.core.storage import (
    append_rows_csv, offers_csv_path, photos_csv_path, urls_csv_path, append_offer_row
)

log = get_logger("scrapper.trojmiasto")

# --- Funkcje pomocnicze (zaczerpnięte z Morizon / dostosowane) ---

def _coerce_float(x: Any) -> Optional[float]:
    """Konwertuje wartość na float, obsługując różne formaty tekstowe."""
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
    """Wyciąga ID oferty (np. 'ogl66186673') z URL-a."""
    m = re.search(r"(ogl\d{6,})", url, re.I)
    return m.group(1) if m else None

def _parse_ld_json_blocks(html: str) -> list[dict]:
    """Znajduje i parsuje wszystkie bloki <script type="application/ld+json"> w HTML-u."""
    out: list[dict] = []
    # Używamy re.finditer, aby złapać wszystkie bloki
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S | re.I):
        raw = m.group(1).strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            if isinstance(obj, list):
                # Czasem w bloku jest lista obiektów
                out.extend([x for x in obj if isinstance(x, dict)])
            elif isinstance(obj, dict):
                out.append(obj)
        except json.JSONDecodeError:
            log.warning("ld_json_parse_fail", extra={"snippet": raw[:100]})
            continue
    return out

# --- Adapter ---

@dataclass
class TrojmiastoAdapter(BaseAdapter):
    source: str = "trojmiasto"
    http: Optional[HttpClient] = None
    out_dir: Optional[Path] = None # Używane przez metody write_* (na razie brak)

    def with_deps(self, http: HttpClient, out_dir: Path, **kwargs):
        """Wstrzykuje zależności (HttpClient, ścieżka wyjściowa)."""
        self.http = http
        self.out_dir = out_dir
        # kwargs (np. use_osm_geocode) jest ignorowane, bo mamy geo z ld+json
        return self

    def discover(self, *, city: str, deal: str, kind: str, max_pages: int = 1) -> Iterable[OfferIndex]:
        """
        Zwraca URL-e ofert z listingu.
        Struktura URL-a: https://ogloszenia.trojmiasto.pl/nieruchomosci/{kategoria-deal}/?strona={N}
        Np.: /nieruchomosci/s,Mieszkanie%20na%20sprzeda%C5%BC.html
        Np.: /nieruchomosci/s,Dom%20na%20sprzeda%C5%BC.html
        Np.: /nieruchomosci/s,Dzia%C5%82ka%20na%20sprzeda%C5%BC.html
        """
        assert self.http is not None, "HttpClient not set. Call with_deps()."
        
        # Tłumaczenie parametrów wejściowych na format Trojmiasto.pl
        deal_map = {"sprzedaz": "sprzedaż", "wynajem": "wynajem"}
        kind_map = {
            "mieszkanie": "Mieszkanie",
            "dom": "Dom",
            "dzialka": "Działka",
            "lokal": "Lokal",
        }
        
        deal_slug = deal_map.get(str(deal).lower(), "sprzedaż")
        kind_slug = kind_map.get(str(kind).lower(), "Mieszkanie")

        # Tworzenie slug-a wyszukiwania, np. "Mieszkanie na sprzedaż"
        search_slug = f"{kind_slug} na {deal_slug}"
        # URL-safe encoding, np. "Mieszkanie%20na%20sprzeda%C5%BC"
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
            # Selektor dla linków do ofert
            links = bs.select('a[href*="/nieruchomosci-"][href*="ogl"]')
            
            found_links_count = 0
            for a in links:
                href = a.get("href")
                if not href:
                    continue
                    
                full_url = normalize_url(join_url(base_url, href))
                oid = _offer_id_from_url(full_url)
                
                if not oid or oid in dedup_ids:
                    continue
                
                dedup_ids.add(oid)
                rows.append({
                    "offer_url": full_url,
                    "offer_id": oid,
                    "page_idx": page,
                })
                found_links_count += 1

            log.info("discover_page", extra={"extra": {"page": page, "found": found_links_count, "kept": len(rows)}})
            
            # Sprawdzenie, czy jest następna strona (jeśli nie ma linków, to prawdopodobnie koniec)
            if not links or found_links_count == 0:
                log.info("discover_finish_no_more_links", extra={"extra": {"page": page}})
                break

        return rows

    def parse_offer(self, url: str) -> dict:
        """Pobiera i parsuje dane ze strony oferty, głównie z ld+json."""
        assert self.http is not None
        url = normalize_url(url)
        html = self.http.get(url, accept="text/html").text

        data: dict[str, Any] = {
            "source": self.source,
            "url": url,
            "offer_id": _offer_id_from_url(url) or "",
        }

        ld_blocks = _parse_ld_json_blocks(html)
        
        # Szukamy głównego bloku (zazwyczaj 'Product' lub 'Offer')
        main_block = None
        for block in ld_blocks:
            type_ = block.get("@type")
            if type_ == "Product" and "offers" in block:
                main_block = block
                break
            if type_ == "Offer": # Fallback
                main_block = block
                break
        
        if not main_block:
            log.error("parse_offer_no_main_ld_block", extra={"extra": {"url": url}})
            return data # Zwróć puste dane (tylko z URL i ID)

        try:
            # Tytuł
            if main_block.get("name"):
                data["title"] = str(main_block["name"]).strip()

            # Cena i waluta (zagnieżdżone w 'offers')
            offers_data = main_block.get("offers")
            if isinstance(offers_data, dict):
                if offers_data.get("price"):
                    data["price_amount"] = _coerce_float(offers_data["price"])
                if offers_data.get("priceCurrency"):
                    data["price_currency"] = str(offers_data["priceCurrency"]).upper()

            # Powierzchnia (zagnieżdżone w 'floorSize')
            floor_size_data = main_block.get("floorSize")
            if isinstance(floor_size_data, dict) and floor_size_data.get("value") is not None:
                data["area_m2"] = _coerce_float(floor_size_data["value"])
            
            # Pokoje
            if main_block.get("numberOfRooms") is not None:
                data["rooms"] = _coerce_float(main_block["numberOfRooms"])

            # Lokalizacja (Geo)
            geo_data = main_block.get("geo")
            if isinstance(geo_data, dict):
                data["lat"] = _coerce_float(geo_data.get("latitude"))
                data["lon"] = _coerce_float(geo_data.get("longitude"))

            # Lokalizacja (Adres)
            address_data = main_block.get("address")
            if isinstance(address_data, dict):
                data["city"] = address_data.get("addressLocality")
                # Trojmiasto.pl nie zawsze podaje ulicę w ld+json
                data["street"] = address_data.get("streetAddress") 

            # Daty
            if main_block.get("datePosted"):
                data["posted_at"] = str(main_block["datePosted"]).strip()
            if main_block.get("dateModified"):
                data["updated_at"] = str(main_block["dateModified"]).strip()

        except Exception as e:
            log.error("parse_offer_ld_extract_fail", extra={"extra": {"url": url, "err": str(e)}})

        return data

    def write_urls_csv(self, rows: Iterable[OfferIndex]) -> Path:
        """Zapisuje zmaterializowane URL-e do urls.csv"""
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
        """Zapisuje oferty do offers.csv używając globalnego nagłówka."""
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
        """Zapisuje zmapowane linki do zdjęć do photos.csv"""
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

    def parse_photos(self, html_or_url: str) -> list[PhotoMeta]:
        """Pobiera linki do zdjęć z bloku ld+json."""
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

        ld_blocks = _parse_ld_json_blocks(html)
        
        image_urls: list[str] = []
        
        for block in ld_blocks:
            images = block.get("image")
            if not images:
                continue
            
            if isinstance(images, list):
                # Oczekiwany format: lista stringów URL
                image_urls.extend([str(img) for img in images if isinstance(img, str) and img.startswith("http")])
            elif isinstance(images, str) and images.startswith("http"):
                # Czasem może być pojedynczy URL
                image_urls.append(images)

        if not image_urls:
            log.warning("parse_photos_no_images_in_ld_json", extra={"extra": {"url_or_snippet": html_or_url[:100]}})
            return []

        # Deduplikacja przy zachowaniu kolejności
        seen = set()
        unique_urls = []
        for u in image_urls:
            if u not in seen:
                seen.add(u)
                unique_urls.append(u)

        return [{"seq": i, "url": u} for i, u in enumerate(unique_urls)]