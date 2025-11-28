# pipelines/detail.py
from __future__ import annotations
import json, traceback, os
import csv
from datetime import datetime
from pathlib import Path
from pydantic import ValidationError

from scrapper.adapters.otodom import OtodomAdapter
from scrapper.adapters.morizon import MorizonAdapter
from scrapper.adapters.gratka import GratkaAdapter

from scrapper.core.http import HttpClient, build_proxies
from scrapper.core.log import setup_json_logger, get_logger
from scrapper.core.storage import offers_csv_path, append_offer_row, append_rows_csv
from scrapper.core.validate import Offer

REQ_FIELDS = ["price_amount","city","area_m2","rooms","lat","lon","offer_id","source"]
OFFER_SCHEMA = ["offer_id","source","url","price_amount","price_currency","price_per_m2","city","lat","lon","area_m2","rooms"]

log = get_logger("scrapper")

def _project_offer_row(d: dict) -> dict:
    # wartości poza schema są ignorowane; brakujące uzupełniane pustką
    return {k: d.get(k, "") for k in OFFER_SCHEMA}

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

def _iso_or_same(x):
    try:
        from datetime import datetime
        if isinstance(x, datetime):
            return x.isoformat()
    except Exception:
        pass
    return x

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
    allow_incomplete: bool = False,   # zostawiamy przełącznik, ale domyślnie twarda walidacja
    dump_debug: bool = True,
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
    if not urls:
        log.info("detail_no_input", extra={"extra": {"source": "morizon", "urls_csv": str(urls_csv)}})
        return {"offers_ok": 0, "offers_fail": 0}

    ok = 0
    fail = 0
    batch: list[dict] = []

    adapter = MorizonAdapter().with_deps(
        http=http,
        out_dir=out_dir,
        use_osm_geocode=True,
    )

    now = datetime.utcnow()
    dbg = (out_dir / "offers_debug.jsonl").open("a", encoding="utf-8") if dump_debug else None
    html_dir = out_dir / "debug_html"
    html_dir.mkdir(parents=True, exist_ok=True)

    try:
        for idx, u in enumerate(urls, 1):
            try:
                d = adapter.parse_offer(u)
                # nie chcemy first_seen/last_seen w CSV, ale można trzymać do debugów
                d.setdefault("first_seen", now)
                d.setdefault("last_seen", now)

                missing = [k for k in REQ_FIELDS if d.get(k) in (None, "")]
                if dump_debug and dbg:
                    dbg.write(json.dumps(
                        {"source":"morizon","url": u, "missing": missing,
                         "data": {k: _iso_or_same(v) for k, v in d.items()}},
                        ensure_ascii=False
                    ) + "\n")

                if missing and not allow_incomplete:
                    fail += 1
                    log.warning("detail_incomplete_skip", extra={"extra": {"source":"morizon","url": u, "missing": missing}})
                    continue

                # Walidacja typów
                Offer(**d)

                # PROJEKCJA -> tylko docelowy schemat kolumn
                batch.append(_project_offer_row(d))
                ok += 1

            except ValidationError as e:
                fail += 1
                if dump_debug:
                    log.warning("detail_validate_fail", extra={"extra":{"source":"morizon","url":u,"err":"ValidationError","fields":list(e.errors())}})
            except Exception:
                fail += 1
                # zrzut HTML do analizy
                try:
                    raw = adapter.http.get(u, accept="text/html").text
                    fname = f"err_{idx}.html"
                    (html_dir / fname).write_text(raw, encoding="utf-8", errors="ignore")
                except Exception:
                    pass
                if dump_debug:
                    log.warning("detail_parse_fail", extra={"extra":{"source":"morizon","url":u,"err":"ParseError"}})

            # zapis wsadowy co 50
            if len(batch) >= 50:
                out_csv = offers_csv_path(out_dir)
                append_rows_csv(out_csv, batch, header=OFFER_SCHEMA)
                batch.clear()

        # flush końcowy
        if batch:
            out_csv = offers_csv_path(out_dir)
            append_rows_csv(out_csv, batch, header=OFFER_SCHEMA)
            batch.clear()

        log.info("detail_done", extra={"extra": {"source":"morizon","ok": ok, "fail": fail, "out": str(offers_csv_path(out_dir))}})
        return {"offers_ok": ok, "offers_fail": fail}
    finally:
        http.close()
        if dbg:
            dbg.close()


def run_gratka_detail(
    *,
    urls_csv: Path,
    out_dir: Path,
    user_agent: str,
    timeout_s: float,
    rps: float,
    http_proxy: str | None,
    https_proxy: str | None,
    allow_incomplete: bool = False,
    dump_debug: bool = True,
) -> dict:
    log = setup_json_logger("scrapper")
    # wczytaj URL-e
    def _read_urls(csv_path: Path) -> list[str]:
        if not csv_path.exists():
            return []
        out: list[str] = []
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                u = row.get("offer_url")
                if u:
                    out.append(u)
        return out

    urls = _read_urls(urls_csv)
    if not urls:
        log.info("detail_no_input", extra={"extra": {"source": "gratka", "urls_csv": str(urls_csv)}})
        return {"offers_ok": 0, "offers_fail": 0}

    http = HttpClient(
        user_agent=user_agent, timeout_s=timeout_s, rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    adapter = GratkaAdapter().with_deps(http=http, out_dir=out_dir, use_osm_geocode=True)
    ok = 0
    fail = 0
    batch: list[dict] = []


    for u in urls:
        try:
            d = adapter.parse_offer(u)
            d.setdefault("source", "gratka")
            missing = [k for k in REQ_FIELDS if d.get(k) in (None, "")]
            if missing:
                fail += 1
                if dump_debug:
                    log.warning("detail_incomplete_skip", extra={"extra":{"source":"gratka","url":u,"missing":missing}})
                continue
            Offer(**d)  # walidacja typów
            batch.append(_project_offer_row(d))  # <-- TUTAJ PROJEKCJA DO SCHEMATU
            ok += 1
        except ValidationError as e:
            fail += 1
            if dump_debug:
                log.warning("detail_validate_fail", extra={"extra":{"source":"gratka","url":u,"err":"ValidationError","fields":list(e.errors())}})
        except Exception as e:
            fail += 1
            if dump_debug:
                log.warning("detail_parse_fail", extra={"extra":{"source":"gratka","url":u,"err":type(e).__name__}})

        if len(batch) >= 50:
            out_csv = offers_csv_path(out_dir)
            append_rows_csv(out_csv, batch, header=OFFER_SCHEMA)  # <-- ZAPIS TYLKO TYCH KOLUMN I W TEJ KOLEJNOŚCI
            batch.clear()

    # flush końcowy
    if batch:
        out_csv = offers_csv_path(out_dir)
        append_rows_csv(out_csv, batch, header=OFFER_SCHEMA)



    log.info("detail_done", extra={"extra": {"source": "gratka", "ok": ok, "fail": fail}})
    return {"offers_ok": ok, "offers_fail": fail}

# --- TROJMIASTO ---

def run_trojmiasto_detail(
    *,
    urls_csv: Path,
    out_dir: Path,
    user_agent: str,
    timeout_s: float,
    rps: float,
    http_proxy: str | None,
    https_proxy: str | None,
    allow_incomplete: bool = False,
    dump_debug: bool = True,
) -> dict:
    # Import lokalny, by uniknąć cyklicznych zależności
    from scrapper.adapters.trojmiasto import TrojmiastoAdapter

    log = setup_json_logger("scrapper")
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )

    urls = _read_urls(urls_csv)
    if not urls:
        log.warning("detail_no_urls", extra={"extra": {"source": "trojmiasto", "urls_csv": str(urls_csv)}})
        return {"offers_ok": 0, "offers_fail": 0}

    ok = 0
    fail = 0
    batch: list[dict] = []

    adapter = TrojmiastoAdapter().with_deps(http=http, out_dir=out_dir)
    now = datetime.utcnow()

    dbg = (out_dir / "offers_debug.jsonl").open("a", encoding="utf-8") if dump_debug else None
    try:
        for u in urls:
            try:
                d = adapter.parse_offer(u)
                # metadane do debugów (nie trafią do CSV dzięki projekcji)
                d.setdefault("first_seen", now)
                d.setdefault("last_seen", now)

                missing = [k for k in REQ_FIELDS if d.get(k) in (None, "")]
                if dump_debug and dbg:
                    dbg.write(json.dumps(
                        {"source": "trojmiasto", "url": u, "missing": missing,
                         "data": {k: _iso_or_same(v) for k, v in d.items()}},
                        ensure_ascii=False
                    ) + "\n")

                if missing and not allow_incomplete:
                    fail += 1
                    log.warning("detail_incomplete_skip", extra={"extra": {"source": "trojmiasto", "url": u, "missing": missing}})
                    continue

                # Walidacja typów/zakresów
                Offer(**d)

                # PROJEKCJA → zapisujemy tylko docelowy schemat kolumn
                batch.append(_project_offer_row(d))
                ok += 1

            except ValidationError as e:
                fail += 1
                if dump_debug:
                    log.warning("detail_validate_fail", extra={"extra": {"source": "trojmiasto", "url": u, "err": "ValidationError", "fields": list(e.errors())}})
            except Exception as e:
                fail += 1
                if dump_debug:
                    log.warning("detail_parse_fail", extra={"extra": {"source": "trojmiasto", "url": u, "err": type(e).__name__}})

            # zapis wsadowy co 50
            if len(batch) >= 50:
                out_csv = offers_csv_path(out_dir)
                append_rows_csv(out_csv, batch, header=OFFER_SCHEMA)
                batch.clear()

        # flush końcowy
        if batch:
            out_csv = offers_csv_path(out_dir)
            append_rows_csv(out_csv, batch, header=OFFER_SCHEMA)
            batch.clear()

        log.info("detail_done", extra={"extra": {"source": "trojmiasto", "ok": ok, "fail": fail, "out": str(offers_csv_path(out_dir))}})
        return {"offers_ok": ok, "offers_fail": fail}
    finally:
        http.close()
        if dbg:
            dbg.close()