# pipelines/photos.py
from __future__ import annotations

import csv
from pathlib import Path

from scrapper.adapters.otodom import OtodomAdapter
from scrapper.adapters.morizon import MorizonAdapter

from scrapper.core.http import HttpClient, build_proxies
from scrapper.core.log import setup_json_logger
from scrapper.core.storage import photos_csv_path


def _read_offers(offers_csv: Path) -> list[dict]:
    if not offers_csv.exists():
        return []
    with offers_csv.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_otodom_photos(
    *,
    offers_csv: Path,
    out_dir: Path,
    img_dir: Path,
    user_agent: str,
    timeout_s: int,
    rps: float,
    limit_per_offer: int | None = None,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
) -> dict[str, int]:
    log = setup_json_logger()
    offers = _read_offers(offers_csv)
    if not offers:
        log.info("photos_no_input", extra={"extra": {"offers_csv": str(offers_csv)}})
        return {"photos_ok": 0, "photos_fail": 0}

    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    adapter = OtodomAdapter().with_deps(http=http, out_dir=out_dir)

    ok = 0
    fail = 0
    try:
        for row in offers:
            offer_id = row.get("offer_id") or ""
            url = row.get("url") or ""
            if not offer_id or not url:
                continue
            try:
                plist = adapter.parse_photos(url)
            except Exception as e:
                fail += 1
                log.warning(
                    "photos_list_fail",
                    extra={"extra": {"offer_id": offer_id, "err": type(e).__name__}},
                )
                continue
            try:
                adapter.write_photo_links_csv(
                    offer_id=offer_id,
                    offer_url=url,
                    photo_list=plist,
                    limit=limit_per_offer,
                )
                ok += len(plist) if limit_per_offer is None else min(
                    len(plist), limit_per_offer
                )
            except Exception as e:
                fail += 1
                log.warning(
                    "photos_download_fail",
                    extra={"extra": {"offer_id": offer_id, "err": type(e).__name__}},
                )
        log.info(
            "photos_done",
            extra={
                "extra": {
                    "ok": ok,
                    "fail": fail,
                    "out": str(photos_csv_path(out_dir)),
                }
            },
        )
        return {"photos_ok": ok, "photos_fail": fail}
    finally:
        http.close()

def run_morizon_photos(
    *,
    offers_csv: Path,
    out_dir: Path,
    img_dir: Path,
    user_agent: str,
    timeout_s: float,
    rps: float,
    limit_per_offer: int | None,
    http_proxy: str | None,
    https_proxy: str | None,
) -> dict:
    log = setup_json_logger("scrapper")
    http = HttpClient(
        user_agent=user_agent, timeout_s=timeout_s, rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    ok = 0
    fail = 0
    adapter = MorizonAdapter().with_deps(http=http, out_dir=out_dir)

    try:
        for row in _read_offers(offers_csv):
            offer_id = row.get("offer_id") or ""
            url = row.get("url") or ""
            if not offer_id or not url:
                continue
            try:
                plist = adapter.parse_photos(url)
            except Exception as e:
                fail += 1
                log.warning("photos_list_fail",
                    extra={"extra":{"offer_id":offer_id,"err":type(e).__name__}})
                continue
            try:
                adapter.write_photo_links_csv(
                    offer_id=offer_id,
                    offer_url=url,
                    photo_list=plist,
                    limit=limit_per_offer,
                )

                ok += len(plist) if limit_per_offer is None else min(len(plist), limit_per_offer)
            except Exception as e:
                fail += 1
                log.warning("photos_download_fail",
                    extra={"extra":{"offer_id":offer_id,"err":type(e).__name__}})
        log.info("photos_done",
            extra={"extra":{"ok":ok,"fail":fail,"out":str(photos_csv_path(out_dir))}})
        return {"photos_ok": ok, "photos_fail": fail}
    finally:
        http.close()