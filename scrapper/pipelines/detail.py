# pipelines/detail.py
from __future__ import annotations
import json, traceback, os
import csv
from datetime import datetime
from pathlib import Path
from pydantic import ValidationError

from scrapper.adapters.otodom import OtodomAdapter
from scrapper.adapters.morizon import MorizonAdapter

from scrapper.core.http import HttpClient, build_proxies
from scrapper.core.log import setup_json_logger, get_logger
from scrapper.core.storage import offers_csv_path
from scrapper.core.validate import Offer

REQ_FIELDS = ["price_amount","city","area_m2","rooms","lat","lon","offer_id","source"]
log = get_logger("scrapper")

def _is_complete(d: dict) -> bool:
    return all(d.get(k) not in (None, "") for k in REQ_FIELDS)

def _read_urls(urls_csv: Path) -> list[str]:
    if not urls_csv.exists():
        return []
    out: list[str] = []
    with urls_csv.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = row.get("offer_url") or row.get("url") or row.get("href")
            if u:
                out.append(u.strip())
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

def run_morizon_detail(
    *,
    urls_csv: Path,
    out_dir: Path,
    user_agent: str,
    timeout_s: float,
    rps: float,
    http_proxy: str | None,
    https_proxy: str | None,
    allow_incomplete: bool = False,   # NEW
    dump_debug: bool = True,          # NEW
) -> dict:
    import json, traceback
    from datetime import datetime

    log = setup_json_logger("scrapper")
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )

    urls = _read_urls(urls_csv)
    ok = 0
    fail = 0
    batch: list[dict] = []

    adapter = MorizonAdapter().with_deps(
        http=http,
        out_dir=out_dir,
        use_osm_geocode=True,   # fallback do OSM w razie braku geo
    )

    now = datetime.utcnow()
    dbg = (out_dir / "offers_debug.jsonl").open("a", encoding="utf-8") if dump_debug else None
    html_dir = out_dir / "debug_html"
    html_dir.mkdir(parents=True, exist_ok=True)

    try:
        for _i, u in enumerate(urls, 1):
            try:
                data = adapter.parse_offer(u)
                data.setdefault("first_seen", now)
                data.setdefault("last_seen", now)

                missing = [k for k in REQ_FIELDS if data.get(k) in (None, "")]
                if dump_debug and dbg:
                    dbg.write(json.dumps({"url": u, "missing": missing, "data": data}, ensure_ascii=False) + "\n")

                if not missing:
                    Offer(**data)                # walidacja
                    batch.append(data); ok += 1
                else:
                    if allow_incomplete:
                        batch.append(data); ok += 1
                        log.warning("detail_incomplete_keep", extra={"extra": {"url": u, "missing": missing}})
                    else:
                        fail += 1
                        log.warning("detail_incomplete_skip", extra={"extra": {"url": u, "missing": missing}})
            except Exception as e:
                fail += 1
                # zrzut problematycznego HTML
                try:
                    raw = adapter.http.get(u, accept="text/html").text
                    oid = _offer_id_from_url(u) if '_offer_id_from_url' in globals() else None
                    fname = (oid or f"err_{_i}").replace("/", "_") + ".html"
                    (html_dir / fname).write_text(raw, encoding="utf-8", errors="ignore")
                except Exception:
                    pass
                tb = traceback.format_exc()
                if dump_debug and dbg:
                    dbg.write(json.dumps({"url": u, "error": str(e), "traceback": tb}, ensure_ascii=False) + "\n")
                log.warning("detail_parse_fail", extra={"extra": {"url": u, "err": type(e).__name__}})

            if len(batch) >= 50:
                adapter.write_offers_csv(batch)
                batch.clear()

        if batch:
            adapter.write_offers_csv(batch)
            batch.clear()

        log.info("detail_done", extra={"extra": {"ok": ok, "fail": fail, "out": str(offers_csv_path(out_dir))}})
        return {"offers_ok": ok, "offers_fail": fail}
    finally:
        http.close()
        if dbg:
            dbg.close()

