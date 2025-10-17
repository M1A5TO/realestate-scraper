# scrapper/cli.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from scrapper.config import load_settings
from scrapper.pipelines.detail import run_otodom_detail
from scrapper.pipelines.discover import run_otodom_discover
from scrapper.pipelines.photos import run_otodom_photos
from scrapper.pipelines.run import run_otodom_full

app = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode=None)
otodom = typer.Typer(help="Operacje dla źródła: Otodom", rich_markup_mode=None)
app.add_typer(otodom, name="otodom")


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
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto", is_flag=False),
    deal: Optional[str] = typer.Option(
        None, "--deal", "-d", help="sprzedaz|wynajem", is_flag=False
    ),
    kind: Optional[str] = typer.Option(
        None, "--kind", "-k", help="mieszkanie|dom", is_flag=False
    ),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
    limit_photos: Optional[int] = typer.Option(
        None, "--limit-photos", "-l", help="Limit zdjęć na ofertę", is_flag=False
    ),
) -> None:
    cfg = load_settings()
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


if __name__ == "__main__":
    app()
