# core/images.py
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from .storage import photo_dir

# Dopuszczone typy MIME
ALLOWED_MIME = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}

@dataclass
class DownloadResult:
    path: Path
    bytes: int
    sha256: str
    mime: str
    ext: str
    status: str  # "ok" | "failed"

def _guess_ext_from_content_type(ct: str | None) -> str | None:
    if not ct:
        return None
    ct = ct.split(";")[0].strip().lower()
    return ALLOWED_MIME.get(ct)

def _sniff_ext_from_magic(b: bytes) -> str | None:
    # Minimalny sniff: JPEG, PNG, WebP
    if len(b) >= 3 and b[:3] == b"\xFF\xD8\xFF":
        return "jpg"
    if len(b) >= 8 and b[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if len(b) >= 12 and b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "webp"
    return None

def _atomic_write(target: Path, data: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)

def download_photo(http_client, url: str, dest_root: Path, source: str, offer_id: str, seq: int) -> DownloadResult:
    """
    Procedura: HEAD → weryfikacja MIME → GET → sniff → hash → zapis atomowy.
    Zwraca metadane do wpisu w photos.csv.
    """
    # 1) HEAD
    try:
        h = http_client.head(url)
        ct = h.headers.get("Content-Type")
        ext = _guess_ext_from_content_type(ct)
    except Exception:
        # Brak HEAD w niektórych CDN – przejdź dalej
        ext = None
        ct = None

    # 2) GET
    r = http_client.get(url, accept="image/*")
    data = r.content
    size = len(data)
    if size == 0:
        return DownloadResult(path=Path(), bytes=0, sha256="", mime=ct or "", ext=ext or "", status="failed")

    # 3) Sniff sygnatury
    sniff_ext = _sniff_ext_from_magic(data[:16])
    final_ext = (ext or sniff_ext or "jpg")
    # Wymuś zgodność zezwolonych formatów
    if final_ext not in {"jpg", "png", "webp"}:
        final_ext = "jpg"

    # 4) Hash
    sha256 = hashlib.sha256(data).hexdigest()

    # 5) Zapis atomowy
    dest = photo_dir(dest_root, source, offer_id) / f"{seq:03d}.{final_ext}"
    _atomic_write(dest, data)

    # 6) MIME docelowe (heurystyka)
    mime = {
        "jpg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
    }[final_ext]

    return DownloadResult(path=dest, bytes=size, sha256=sha256, mime=mime, ext=final_ext, status="ok")
