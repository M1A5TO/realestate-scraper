# tests/test_images.py
from __future__ import annotations

import types

from scrapper.core.images import download_photo

# Minimalny JPEG: SOI + padding
MIN_JPEG = b"\xFF\xD8\xFF" + b"\x00" * 1024


class _Resp:
    def __init__(self, content=b"", headers=None, text=""):
        self.content = content
        self.headers = headers or {}
        self.text = text


def test_download_photo_head_get_and_hash(tmp_path):
    def head(url):
        return _Resp(headers={"Content-Type": "image/jpeg"})

    def get(url, accept=None):
        return _Resp(content=MIN_JPEG)

    http = types.SimpleNamespace(head=head, get=get)

    res = download_photo(
        http,
        "https://cdn.test/img.jpg",
        dest_root=tmp_path,
        source="otodom",
        offer_id="ID123456",
        seq=0,
    )
    assert res.status == "ok"
    assert res.bytes == len(MIN_JPEG)
    assert res.ext == "jpg"
    assert res.mime == "image/jpeg"
    assert res.sha256 and len(res.sha256) == 64
    assert res.path.exists()
    assert res.path == tmp_path / "otodom" / "ID123456" / "000.jpg"
