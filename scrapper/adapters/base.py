# adapters/base.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, TypedDict


class OfferIndex(TypedDict, total=False):
    offer_url: str
    offer_id: str  # opcjonalnie, jeśli da się wydobyć z listingu
    page_idx: int

class PhotoMeta(TypedDict, total=False):
    seq: int
    url: str
    width: int
    height: int

class BaseAdapter(Protocol):
    """
    Kontrakt adaptera serwisu.
    Każdy adapter implementuje trzy kroki, które pipeline wywołuje w stałej kolejności.
    """
    source: str  # np. "otodom"

    def discover(self,*,city: str,deal: str,kind: str,max_pages: int = 1,) -> Iterable[OfferIndex]:
        """Zwraca URL-e ofert (i opcjonalnie offer_id) z listingu; paginacja do max_pages."""
        ...

    def parse_offer(self, url: str) -> dict:
        """
        Pobiera HTML oferty i zwraca dict zgodny z modelem Offer (core/validate.py).
        Walidacja następuje później, w warstwie pipeline.
        """
        ...

    def parse_photos(self, html_or_url: str) -> list[PhotoMeta]:
        """
        Zwraca listę metadanych zdjęć (seq,url,opcjonalnie width/height).
        html_or_url: adapter może przyjąć już pobrany HTML lub URL do dociągnięcia.
        """
        ...
