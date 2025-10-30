# core/parse.py
from __future__ import annotations

from collections.abc import Iterable

from bs4 import BeautifulSoup
from lxml import html as lxml_html


def soup(html: str):
    # Najpierw szybszy lxml – jeśli środowisko lub input robi fikołki, zrób twardy fallback.
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def select_text(node: BeautifulSoup, css: str, default: str = "") -> str:
    el = node.select_one(css)
    return el.get_text(strip=True) if el else default

def select_attr(node: BeautifulSoup, css: str, attr: str, default: str = "") -> str:
    el = node.select_one(css)
    return el.get(attr, default) if el and el.has_attr(attr) else default

def select_all(node: BeautifulSoup, css: str) -> Iterable[BeautifulSoup]:
    return node.select(css)

def find_ld_json_all(html_text: str) -> list[str]:
    """Wyciąga wszystkie bloki <script type=application/ld+json> jako surowy JSON."""
    s = soup(html_text)
    out = []
    for sc in s.select('script[type="application/ld+json"]'):
        if sc.string:
            out.append(sc.string.strip())
    return out

def lxml_xpath(html_text: str, xpath: str) -> list[str]:
    """Gdy CSS nie wystarcza: szybkie XPath przez lxml."""
    tree = lxml_html.fromstring(html_text)
    return [n if isinstance(n, str) else n.text_content().strip() for n in tree.xpath(xpath)]
