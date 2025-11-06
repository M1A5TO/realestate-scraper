# pipelines/run.py
from __future__ import annotations

from pathlib import Path

from scrapper.core.log import setup_json_logger
from scrapper.core.storage import offers_csv_path, photos_csv_path, urls_csv_path

from scrapper.pipelines.detail import run_otodom_detail
from scrapper.pipelines.discover import run_otodom_discover
from scrapper.pipelines.photos import run_otodom_photos

from scrapper.pipelines.detail import run_morizon_detail
from scrapper.pipelines.discover import run_morizon_discover
from scrapper.pipelines.photos import run_morizon_photos

def run_otodom_full(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    out_dir: Path,
    img_dir: Path,
    user_agent: str,
    timeout_s: int,
    rps: float,
    limit_photos: int | None = None,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
) -> dict[str, int]:
    log = setup_json_logger()
    out_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)

    run_otodom_discover(
        city=city,
        deal=deal,
        kind=kind,
        max_pages=max_pages,
        out_dir=out_dir,
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        http_proxy=http_proxy,
        https_proxy=https_proxy,
    )
    st2 = run_otodom_detail(
        urls_csv=urls_csv_path(out_dir),
        out_dir=out_dir,
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        http_proxy=http_proxy,
        https_proxy=https_proxy,
    )
    st3 = run_otodom_photos(
        offers_csv=offers_csv_path(out_dir),
        out_dir=out_dir,
        img_dir=img_dir,
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        limit_per_offer=limit_photos,
        http_proxy=http_proxy,
        https_proxy=https_proxy,
    )
    stats = {
        "discover_pages": max_pages,
        "offers_ok": st2.get("offers_ok", 0),
        "offers_fail": st2.get("offers_fail", 0),
        "photos_ok": st3.get("photos_ok", 0),
        "photos_fail": st3.get("photos_fail", 0),
        "urls_csv": int(urls_csv_path(out_dir).exists()),
        "offers_csv": int(offers_csv_path(out_dir).exists()),
        "photos_csv": int(photos_csv_path(out_dir).exists()),
    }
    log.info("run_full_done", extra={"extra": stats})
    return stats

def run_morizon_full(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    out_dir: Path,
    img_dir: Path,
    user_agent: str,
    timeout_s: float,
    rps: float,
    limit_photos: int | None,
    http_proxy: str | None,
    https_proxy: str | None,
) -> dict:
    log = setup_json_logger("scrapper")
    st1 = run_morizon_discover(
        city=city, deal=deal, kind=kind, max_pages=max_pages,
        out_dir=out_dir, user_agent=user_agent, timeout_s=timeout_s, rps=rps,
        http_proxy=http_proxy, https_proxy=https_proxy,
    )
    st2 = run_morizon_detail(
        urls_csv=urls_csv_path(out_dir), out_dir=out_dir,
        user_agent=user_agent, timeout_s=timeout_s, rps=rps,
        http_proxy=http_proxy, https_proxy=https_proxy,
    )
    st3 = run_morizon_photos(
        offers_csv=offers_csv_path(out_dir), out_dir=out_dir, img_dir=img_dir,
        user_agent=user_agent, timeout_s=timeout_s, rps=rps,
        limit_per_offer=limit_photos, http_proxy=http_proxy, https_proxy=https_proxy,
    )
    stats = {
        "discover_pages": max_pages,
        "offers_ok": st2.get("offers_ok", 0),
        "offers_fail": st2.get("offers_fail", 0),
        "photos_ok": st3.get("photos_ok", 0),
        "photos_fail": st3.get("photos_fail", 0),
        "urls_csv": int(urls_csv_path(out_dir).exists()),
        "offers_csv": int(offers_csv_path(out_dir).exists()),
        "photos_csv": int(photos_csv_path(out_dir).exists()),
    }
    log.info("run_full_done", extra={"extra": stats})
    return stats