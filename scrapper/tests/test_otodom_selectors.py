# tests/test_otodom_selectors.py
from __future__ import annotations

import types

from scrapper.adapters.otodom import (
    OtodomAdapter,
    _extract_offer_links,
    _parse_ld_json_offer,
)

HTML_LISTING = """
<html><body>
<a href="/pl/oferta/gdansk-super-mieszkanie-ID123456">Oferta 1</a>
<a href="/pl/oferta/gdansk-fajny-dom-ID654321">Oferta 2</a>
</body></html>
"""

HTML_OFFER = """
<html><head>
<script type="application/ld+json">
{
  "@type": "Product",
  "name": "Mieszkanie 50 m², 2 pokoje",
  "description": "Opis testowy.",
  "image": [
    "https://img.otodom.pl/photos1.jpg",
    "https://img.otodom.pl/photos2.jpg"
  ],
  "offers": {
    "@type": "Offer",
    "price": "499000",
    "priceCurrency": "PLN"
  },
  "address": {
    "@type": "PostalAddress",
    "addressLocality": "Gdańsk",
    "streetAddress": "ul. Testowa 1"
  },
  "geo": {"latitude": 54.35, "longitude": 18.65},
  "numberOfRooms": 2
}
</script>
</head><body></body></html>
"""

class _Resp:
    def __init__(self, text):
        self.text = text


def _mock_get(text):
    def _get(url, accept=None):
        return _Resp(text)

    return _get


def test_listing_links_extraction():
    links = _extract_offer_links(HTML_LISTING)
    assert any("ID123456" in u for u in links)
    assert any("ID654321" in u for u in links)
    assert all(u.startswith("https://") for u in links)


def test_offer_ldjson_parse_basic_fields():
    data = _parse_ld_json_offer(HTML_OFFER)
    assert data["title"].startswith("Mieszkanie")
    assert data["price_amount"] == 499000.0
    assert data["price_currency"] == "PLN"
    assert data["city"] == "Gdańsk"
    assert data["lat"] == 54.35 and data["lon"] == 18.65
    assert len(data["photos_from_json"]) == 2


def test_adapter_parse_offer_and_photos_with_mock_http(tmp_path):
    http = types.SimpleNamespace(get=_mock_get(HTML_OFFER))
    adapter = OtodomAdapter().with_deps(http=http, out_dir=tmp_path)

    url = "https://www.otodom.pl/pl/oferta/gdansk-super-mieszkanie-ID123456"
    offer = adapter.parse_offer(url)
    assert offer["url"] == url
    assert offer["price_amount"] == 499000.0
    photos = adapter.parse_photos(url)
    assert len(photos) == 2
    assert photos[0]["seq"] == 0
