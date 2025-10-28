# core/storage.py
from __future__ import annotations

import csv
import os
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path

# ——————————————————————————————
# Blokada pliku: cross-platform, bez zależności
# ——————————————————————————————
if os.name == "nt":
    import msvcrt  # type: ignore[attr-defined]

    def _lock(f) -> None:
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock(f) -> None:
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
else:
    import fcntl  # type: ignore[import-not-found]

    def _lock(f) -> None:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

    def _unlock(f) -> None:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass


# ——————————————————————————————
# CSV append-safe: nagłówek raz, zapis atomowy
# ——————————————————————————————
def append_rows_csv(
    csv_path: Path | str,
    rows: Iterable[Mapping[str, object]],
    header: list[str],
) -> None:
    p = Path(csv_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    tmp_fd, tmp_name = tempfile.mkstemp(prefix=p.name + ".", dir=p.parent)
    try:
        write_header = not p.exists() or p.stat().st_size == 0
        with os.fdopen(tmp_fd, "w", newline="", encoding="utf-8") as tf:
            if p.exists():
                with p.open("r", newline="", encoding="utf-8") as rf:
                    _lock(rf)
                    tf.write(rf.read())
                    _unlock(rf)

            w = csv.DictWriter(tf, fieldnames=header, extrasaction="ignore")
            if write_header and (not p.exists() or p.stat().st_size == 0):
                w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in header})
        os.replace(tmp_name, p)
    except Exception:
        try:
            os.remove(tmp_name)
        except OSError:
            pass
        raise


# ——————————————————————————————
# Ścieżki i nazewnictwo artefaktów
# ——————————————————————————————
def offers_csv_path(out_dir: Path) -> Path:
    return out_dir / "offers.csv"


def urls_csv_path(out_dir: Path) -> Path:
    return out_dir / "urls.csv"


def photos_csv_path(out_dir: Path) -> Path:
    return out_dir / "photos.csv"


def photo_dir(img_root: Path, source: str, offer_id: str) -> Path:
    return img_root / source / offer_id


def photo_path(
    img_root: Path,
    source: str,
    offer_id: str,
    seq: int,
    ext: str = "jpg",
) -> Path:
    return photo_dir(img_root, source, offer_id) / f"{seq:03d}.{ext.lower()}"
