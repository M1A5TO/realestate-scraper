# scrapper/cli.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer,json

from scrapper.config import load_settings

from scrapper.pipelines.discover import run_morizon_discover
from scrapper.pipelines.detail import run_morizon_detail
from scrapper.pipelines.photos import run_morizon_photos
from scrapper.pipelines.run import run_morizon_full

from scrapper.pipelines.detail import run_otodom_detail
from scrapper.pipelines.discover import run_otodom_discover
from scrapper.pipelines.photos import run_otodom_photos
from scrapper.pipelines.run import run_otodom_full

from scrapper.core.storage import append_rows_csv, urls_csv_path, offers_csv_path, photos_csv_path

from scrapper.core.http import HttpClient
app = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode=None)
otodom = typer.Typer(help="Operacje dla źródła: Otodom", rich_markup_mode=None)
app.add_typer(otodom, name="otodom")

morizon = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode=None)
app.add_typer(morizon, name="morizon")

@otodom.command("discover")
def otodom_discover_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto", is_flag=False),
    deal: Optional[str] = typer.Option(
        None, "--deal", "-d", help="sprzedaz|wynajem", is_flag=False
    ),
    kind: Optional[str] = typer.Option(
        None, "--kind", "-k", help="mieszkanie|dom", is_flag=False
    ),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
) -> None:
    cfg = load_settings()
    st = run_otodom_discover(
        city=city or cfg.defaults.city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        max_pages=max_pages,
        out_dir=Path(cfg.io.out_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(st)


@otodom.command("detail")
def otodom_detail_cmd(
    in_urls: str = typer.Option(
        "data/out/urls.csv", "--in-urls", "-i", help="Ścieżka do CSV z URL-ami"
    ),
) -> None:
    cfg = load_settings()
    st = run_otodom_detail(
        urls_csv=Path(in_urls),
        out_dir=Path(cfg.io.out_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(st)


@otodom.command("photos")
def otodom_photos_cmd(
    in_offers: str = typer.Option(
        "data/out/offers.csv", "--in-offers", "-i", help="Ścieżka do CSV z ofertami"
    ),
    limit_photos: Optional[int] = typer.Option(
        None, "--limit-photos", "-l", help="Limit zdjęć na ofertę", is_flag=False
    ),
) -> None:
    cfg = load_settings()
    st = run_otodom_photos(
        offers_csv=Path(in_offers),
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_per_offer=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(st)


@otodom.command("full")
def otodom_full_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkanie|dom"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
    limit_photos: Optional[int] = typer.Option(
        None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"
    ),
    no_photos: bool = typer.Option(False, "--no-photos", help="Pomiń etap zdjęć"),
) -> None:
    cfg = load_settings()

    if no_photos:
        # discover → detail, bez zdjęć
        run_otodom_discover(
            city=city or cfg.defaults.city,
            deal=deal or cfg.defaults.deal,
            kind=kind or cfg.defaults.kind,
            max_pages=max_pages,
            out_dir=Path(cfg.io.out_dir),
            user_agent=cfg.http.user_agent,
            timeout_s=cfg.http.timeout_s,
            rps=cfg.http.rate_limit_rps,
            http_proxy=cfg.http.http_proxy,
            https_proxy=cfg.http.https_proxy,
        )
        st_detail = run_otodom_detail(
            urls_csv=urls_csv_path(Path(cfg.io.out_dir)),
            out_dir=Path(cfg.io.out_dir),
            user_agent=cfg.http.user_agent,
            timeout_s=cfg.http.timeout_s,
            rps=cfg.http.rate_limit_rps,
            http_proxy=cfg.http.http_proxy,
            https_proxy=cfg.http.https_proxy,
        )
        stats = {
            "discover_pages": max_pages,
            "offers_ok": st_detail.get("offers_ok", 0),
            "offers_fail": st_detail.get("offers_fail", 0),
            "photos_ok": 0,
            "photos_fail": 0,
            "urls_csv": int(urls_csv_path(Path(cfg.io.out_dir)).exists()),
            "offers_csv": int(offers_csv_path(Path(cfg.io.out_dir)).exists()),
            "photos_csv": 0,
        }
        typer.echo(stats)
        return

    st = run_otodom_full(
        city=city or cfg.defaults.city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        max_pages=max_pages,
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_photos=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(st)

# --- MORIZON ---

@morizon.command("discover")
def morizon_discover_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkanie|dom|dzialka|lokal"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
) -> None:
    cfg = load_settings()
    st = run_morizon_discover(
        city=city or cfg.defaults.city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        max_pages=max_pages,
        out_dir=Path(cfg.io.out_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(st)


@morizon.command("detail")
def morizon_detail_cmd(
    in_urls: str = typer.Option(
        "data/out/urls.csv", "--in-urls", "-i", help="Ścieżka do CSV z URL-ami"
    ),
    allow_incomplete: bool = typer.Option(False, "--allow-incomplete", help="Zapisuj także niekompletne wiersze"),
    no_debug: bool = typer.Option(False, "--no-debug", help="Nie zapisuj offers_debug.jsonl"),
) -> None:
    cfg = load_settings()
    st = run_morizon_detail(
        urls_csv=Path(in_urls),
        out_dir=Path(cfg.io.out_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
        allow_incomplete=allow_incomplete,
        dump_debug=not no_debug,
    )
    typer.echo(st)


@morizon.command("detail-one")
def morizon_detail_one(
    url: str = typer.Argument(..., help="Pełny URL oferty Morizon"),
    save_html: bool = typer.Option(True, help="Zapisz surowy HTML do data/out/debug_html"),
) -> None:
    cfg = load_settings()

    proxies = {}
    if cfg.http.http_proxy:
        proxies["http"] = cfg.http.http_proxy
    if cfg.http.https_proxy:
        proxies["https"] = cfg.http.https_proxy
    if not proxies:
        proxies = None

    from scrapper.adapters.morizon import MorizonAdapter

    http = HttpClient(
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        proxies=proxies,
    )
    adapter = MorizonAdapter().with_deps(http=http, out_dir=Path(cfg.io.out_dir), use_osm_geocode=True)

    if save_html:
        raw = http.get(url, accept="text/html").text
        dbg_dir = Path(cfg.io.out_dir) / "debug_html"
        dbg_dir.mkdir(parents=True, exist_ok=True)
        (dbg_dir / "detail_one.html").write_text(raw, encoding="utf-8")

    data = adapter.parse_offer(url)
    print(json.dumps(data, ensure_ascii=False, indent=2))
    http.close()


@morizon.command("photos")
def morizon_photos_cmd(
    in_offers: str = typer.Option(
        "data/out/offers.csv", "--in-offers", "-i", help="Ścieżka do CSV z ofertami"
    ),
    limit_photos: Optional[int] = typer.Option(
        None, "--limit-photos", "-l", help="Limit zdjęć na ofertę", min=1
    ),
) -> None:
    cfg = load_settings()
    st = run_morizon_photos(
        offers_csv=Path(in_offers),
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_per_offer=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(st)


@morizon.command("full")
def morizon_full_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkanie|dom|dzialka|lokal"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę", min=1),
    no_photos: bool = typer.Option(False, "--no-photos", help="Pomiń etap zdjęć"),
) -> None:
    cfg = load_settings()

    if no_photos:
        # discover → detail (bez zdjęć)
        run_morizon_discover(
            city=city or cfg.defaults.city,
            deal=deal or cfg.defaults.deal,
            kind=kind or cfg.defaults.kind,
            max_pages=max_pages,
            out_dir=Path(cfg.io.out_dir),
            user_agent=cfg.http.user_agent,
            timeout_s=cfg.http.timeout_s,
            rps=cfg.http.rate_limit_rps,
            http_proxy=cfg.http.http_proxy,
            https_proxy=cfg.http.https_proxy,
        )
        st_detail = run_morizon_detail(
            urls_csv=urls_csv_path(Path(cfg.io.out_dir)),
            out_dir=Path(cfg.io.out_dir),
            user_agent=cfg.http.user_agent,
            timeout_s=cfg.http.timeout_s,
            rps=cfg.http.rate_limit_rps,
            http_proxy=cfg.http.http_proxy,
            https_proxy=cfg.http.https_proxy,
            allow_incomplete=False,
            dump_debug=True,
        )
        stats = {
            "discover_pages": max_pages,
            "offers_ok": st_detail.get("offers_ok", 0),
            "offers_fail": st_detail.get("offers_fail", 0),
            "photos_ok": 0,
            "photos_fail": 0,
            "urls_csv": int(urls_csv_path(Path(cfg.io.out_dir)).exists()),
            "offers_csv": int(offers_csv_path(Path(cfg.io.out_dir)).exists()),
            "photos_csv": 0,
        }
        typer.echo(stats)
        return

    st = run_morizon_full(
        city=city or cfg.defaults.city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        max_pages=max_pages,
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_photos=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(st)



if __name__ == "__main__":
    app()
