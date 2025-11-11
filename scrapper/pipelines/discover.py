# pipelines/discover.py
from __future__ import annotations

from pathlib import Path

from scrapper.adapters.otodom import OtodomAdapter
from scrapper.adapters.morizon import MorizonAdapter
from scrapper.adapters.gratka import GratkaAdapter

from scrapper.core.http import HttpClient, build_proxies
from scrapper.core.log import setup_json_logger
from scrapper.core.storage import urls_csv_path


def run_otodom_discover(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    out_dir: Path,
    user_agent: str,
    timeout_s: int,
    rps: float,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
) -> dict[str, int]:
    log = setup_json_logger()
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    try:
        adapter = OtodomAdapter().with_deps(http=http, out_dir=out_dir)
        rows = adapter.discover(city=city, deal=deal, kind=kind, max_pages=max_pages)
        path = adapter.write_urls_csv(rows)
        stats = {"pages": max_pages, "urls_csv": int(urls_csv_path(out_dir).exists())}
        log.info("discover_done", extra={"extra": {"path": str(path)}})
        return stats
    finally:
        http.close()

def run_morizon_discover(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    out_dir: Path,
    user_agent: str,
    timeout_s: float,
    rps: float,
    http_proxy: str | None,
    https_proxy: str | None,
) -> dict:
    log = setup_json_logger("scrapper")
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    try:
        adapter = MorizonAdapter().with_deps(http=http, out_dir=out_dir)
        rows = adapter.discover(city=city, deal=deal, kind=kind, max_pages=max_pages)
        path = adapter.write_urls_csv(rows)
        stats = {"pages": max_pages, "urls_csv": int(urls_csv_path(out_dir).exists())}
        log.info("discover_done", extra={"extra": {"path": str(path)}})
        return stats
    finally:
        http.close()

def run_gratka_discover(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    out_dir: Path,
    user_agent: str,
    timeout_s: float,
    rps: float,
    http_proxy: str | None,
    https_proxy: str | None,
) -> dict:
    log = setup_json_logger("scrapper")
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    try:
        adapter = GratkaAdapter().with_deps(http=http, out_dir=out_dir)
        rows = adapter.discover(city=city, deal=deal, kind=kind, max_pages=max_pages)
        path = adapter.write_urls_csv(rows)
        stats = {"pages": max_pages, "urls_csv": int(urls_csv_path(out_dir).exists())}
        log.info("discover_done", extra={"extra": {"source": "gratka", **stats, "urls_path": str(path)}})
        return stats
    except Exception as e:
        log.warning("discover_fail", extra={"extra": {"source": "gratka", "err": type(e).__name__}})
        return {"pages": 0, "urls_csv": 0}