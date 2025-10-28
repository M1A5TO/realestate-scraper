# pipelines/detail.py
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from pydantic import ValidationError
from scrapper.adapters.otodom import OtodomAdapter
from scrapper.core.http import HttpClient, build_proxies
from scrapper.core.log import setup_json_logger
from scrapper.core.storage import offers_csv_path
from scrapper.core.validate import Offer

REQ_FIELDS = ["price_amount","city","area_m2","rooms","lat","lon","offer_id","source"]

def _is_complete(d: dict) -> bool:
    return all(d.get(k) not in (None, "") for k in REQ_FIELDS)

def _read_urls(urls_csv: Path) -> list[str]:
    if not urls_csv.exists():
        return []
    out: list[str] = []
    with urls_csv.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = row.get("offer_url")
            if u:
                out.append(u)
    return out


def run_otodom_detail(
    *,
    urls_csv: Path,
    out_dir: Path,
    user_agent: str,
    timeout_s: int,
    rps: float,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
) -> dict[str, int]:
    log = setup_json_logger()
    urls = _read_urls(urls_csv)
    if not urls:
        log.info("detail_no_input", extra={"extra": {"urls_csv": str(urls_csv)}})
        return {"offers_ok": 0, "offers_fail": 0}

    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    ok = 0
    fail = 0
    batch: list[dict] = []
    adapter = OtodomAdapter().with_deps(http=http, out_dir=out_dir)
    now = datetime.utcnow()

    try:
        for _i, u in enumerate(urls, 1):
            try:
                data = adapter.parse_offer(u)
                data.setdefault("first_seen", now)
                data.setdefault("last_seen", now)
                Offer(**data)  # walidacja typów/zakresów
                if _is_complete(data):
                    batch.append(data)
                    ok += 1
                else:
                    fail += 1
                    log.warning(
                        "detail_incomplete_skip",
                        extra={"extra": {
                            "url": u,
                            "missing": [k for k in REQ_FIELDS if data.get(k) in (None, "")]
                        }},
                    )
            except Exception as e:
                fail += 1
                err_name = type(e).__name__#duuuuuuuu
                log_extra = {"extra": {"url": u, "err": err_name}}#du
                if isinstance(e, ValidationError):
                    print(f"--- DEBUG: Błąd walidacji Pydantic dla URL: {u} ---")
                    try:
                        print(e.errors())
                    except AttributeError:
                        print(str(e))
                    log_extra["extra"]["validation_errors"] = e.errors()#duuuuu
                log.warning(
                    "detail_parse_fail",
                    extra={"extra": {"url": u, "err": type(e).__name__}},
                )
            if len(batch) >= 50:
                adapter.write_offers_csv(batch)
                batch.clear()
        if batch:
            adapter.write_offers_csv(batch)
            batch.clear()
        log.info(
            "detail_done",
            extra={
                "extra": {
                    "ok": ok,
                    "fail": fail,
                    "out": str(offers_csv_path(out_dir)),
                }
            },
        )
        return {"offers_ok": ok, "offers_fail": fail}
    finally:
        http.close()
