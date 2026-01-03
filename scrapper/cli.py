# scrapper/cli.py
from __future__ import annotations
from pathlib import Path
from typing import Optional
import json
import re
import time
import typer

from scrapper.config import load_settings

# Pipelines
from scrapper.pipelines.discover import run_otodom_discover, run_morizon_discover, run_gratka_discover, run_trojmiasto_discover
from scrapper.pipelines.detail import run_otodom_detail, run_morizon_detail, run_gratka_detail, run_trojmiasto_detail
from scrapper.pipelines.photos import run_otodom_photos, run_morizon_photos, run_gratka_photos, run_trojmiasto_photos
from scrapper.pipelines.run import run_otodom_full, run_morizon_full, run_gratka_full, run_trojmiasto_full

# I/O helpers tylko do ścieżek
from scrapper.core.storage import urls_csv_path, offers_csv_path, photos_csv_path

# Używane wyłącznie w komendzie detail-one
from scrapper.core.http import HttpClient
from scrapper.adapters.morizon import MorizonAdapter

from scrapper.pipelines.stream import (
    run_otodom_stream, 
    run_morizon_stream, 
    run_gratka_stream, 
    run_trojmiasto_stream
)
VOIVODESHIPS = [
    "dolnoslaskie",
    "kujawsko-pomorskie",
    "lubelskie",
    "lubuskie",
    "lodzkie",
    "malopolskie",
    "mazowieckie",
    "opolskie",
    "podkarpackie",
    "podlaskie",
    "pomorskie",
    "slaskie",
    "swietokrzyskie",
    "warminsko-mazurskie",
    "wielkopolskie",
    "zachodniopomorskie",
]

VOIVODESHIPS2 = [
    "warszawa",
    "krakow",
    "lodz",
    "wroclaw",
    "poznan",
    "gdansk",
    "szczecin",
    "bydgoszcz",
    "lublin",
    "bialystok",
    "katowice",
    "gdynia",
    "czestochowa",
    "radom",
    "sosnowiec",
    "torun",
    "kielce",
    "rzeszow",
    "gliwice",
    "zabrze",
    "olsztyn",
    "opole",
    "zielona-gora",
    "gorzow-wielkopolski",
    "plock",
    "elblag",
    "walbrzych",
    "tarnow",
    "koszalin",
    "legnica",
    "grudziadz",
    "slupsk",
    "jaworzno",
    "nowy-sacz",
    "jelenia-gora",
    "konin",
    "piotrkow-trybunalski",
    "inowroclaw",
    "ostrow-wielkopolski",
    "gniezno",
    "chelm",
    "przemysl",
    "zamosc",
    "reda",
]


def _load_done_regions(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return set()
    out: set[str] = set()
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.add(line)
    return out


def _append_done_region(path: Path, region: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(region.strip() + "\n")


def _write_done_regions(path: Path, regions: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    txt = "\n".join(sorted({r.strip() for r in regions if r and r.strip()}))
    if txt:
        txt += "\n"
    path.write_text(txt, encoding="utf-8")


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


_LIVE_ALL_START_RE = re.compile(r"\[LIVE-ALL\]\s*start\s+region=(?P<region>[a-z0-9\-]+)", re.I)
_LIVE_ALL_DONE_RE = re.compile(r"\[LIVE-ALL\]\s*done\s+region=(?P<region>[a-z0-9\-]+)", re.I)


def _extract_page_from_url(url: str) -> int | None:
    if not url:
        return None
    m = re.search(r"[\?&]page=(\d+)", url)
    if not m:
        m = re.search(r"/\?page=(\d+)", url)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _flatten_extra(obj: dict) -> dict:
    extra = obj.get("extra")
    if isinstance(extra, dict) and "extra" in extra and isinstance(extra.get("extra"), dict):
        # czasem mamy extra={"extra": {...}}
        return extra.get("extra")  # type: ignore[return-value]
    return extra if isinstance(extra, dict) else {}


def _parse_live_all_log(log_path: Path, *, strict_errors: bool) -> dict[str, dict]:
    """Zwraca per-region: done(bool), last_page_done(int), stop_reason(str|None).

    Heurystyka:
      - region uznany za done tylko jeśli wystąpiło "[LIVE-ALL] done region=..." i nie było discover_fetch_fail
      - strict_errors=True dodatkowo wymaga braku logów level=ERROR w regionie
      - last_page_done wyciągamy z discover_page_done.page (max)
    """
    regions: dict[str, dict] = {}
    current: str | None = None

    def ensure(r: str) -> dict:
        regions.setdefault(
            r,
            {
                "saw_done": False,
                "had_fetch_fail": False,
                "had_error": False,
                "last_page_done": 0,
                "stop_reason": None,
            },
        )
        return regions[r]

    for raw in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line:
            continue

        m = _LIVE_ALL_START_RE.search(line)
        if m:
            current = m.group("region").lower()
            ensure(current)
            continue

        m = _LIVE_ALL_DONE_RE.search(line)
        if m:
            r = m.group("region").lower()
            ensure(r)["saw_done"] = True
            current = None
            continue

        # JSON log line
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
            except Exception:
                obj = None

            if isinstance(obj, dict):
                msg = obj.get("msg")
                level = str(obj.get("level") or "").upper()
                extra = _flatten_extra(obj)

                if current:
                    st = ensure(current)
                    if strict_errors and level == "ERROR":
                        st["had_error"] = True

                    if msg == "discover_page_done":
                        try:
                            st["last_page_done"] = max(int(st["last_page_done"]), int(extra.get("page") or 0))
                        except Exception:
                            pass

                    if msg == "discover_fetch_fail":
                        st["had_fetch_fail"] = True
                        st["stop_reason"] = "fetch_fail"
                        url = str(extra.get("url") or "")
                        fail_page = _extract_page_from_url(url)
                        if fail_page and fail_page > 1:
                            st["last_page_done"] = max(int(st["last_page_done"]), fail_page - 1)
                continue

        # Non-JSON (fallback): jeśli w regionie pojawia się 'discover_fetch_fail'
        if current and "discover_fetch_fail" in line:
            st = ensure(current)
            st["had_fetch_fail"] = True
            st["stop_reason"] = "fetch_fail"
            continue

        if current and strict_errors and ("\"level\": \"ERROR\"" in line or line.startswith("ERROR")):
            ensure(current)["had_error"] = True

    out: dict[str, dict] = {}
    for r, st in regions.items():
        done = bool(st.get("saw_done")) and not bool(st.get("had_fetch_fail"))
        if strict_errors and bool(st.get("had_error")):
            done = False
        out[r] = {
            "done": done,
            "last_page_done": int(st.get("last_page_done") or 0),
            "stop_reason": st.get("stop_reason"),
        }
    return out


def _sync_live_all_from_log(
    *,
    source: str,
    log_path: Path,
    out_dir: Path,
    strict_errors: bool,
) -> None:
    done_path = out_dir / f"{source}_live_all_done.txt"
    state_path = out_dir / f"{source}_live_all_state.json"

    done = _load_done_regions(done_path)
    state = _load_json(state_path)
    parsed = _parse_live_all_log(log_path, strict_errors=strict_errors)

    for region, st in parsed.items():
        state.setdefault(region, {})
        if not isinstance(state.get(region), dict):
            state[region] = {}

        state[region]["last_page_done"] = int(st.get("last_page_done") or 0)
        state[region]["stop_reason"] = st.get("stop_reason")
        state[region]["done"] = bool(st.get("done"))

        if st.get("done"):
            done.add(region)

    _save_json(state_path, state)
    _write_done_regions(done_path, done)

app = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode=None)

otodom = typer.Typer(help="Operacje dla źródła: Otodom", rich_markup_mode=None)
morizon = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode=None)
gratka = typer.Typer(help="Scraper Gratka")
trojmiasto = typer.Typer(help="Scraper Trojmiasto.pl", rich_markup_mode=None)

app.add_typer(otodom, name="otodom")
app.add_typer(morizon, name="morizon")
app.add_typer(gratka, name="gratka")
app.add_typer(trojmiasto, name="trojmiasto")

# ---------------- OTODOM ----------------

@otodom.command("discover")
def otodom_discover_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkanie|dom"),
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
    in_urls: str = typer.Option("data/out/urls.csv", "--in-urls", "-i", help="Ścieżka do CSV z URL-ami"),
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
    in_offers: str = typer.Option("data/out/offers.csv", "--in-offers", "-i", help="Ścieżka do CSV z ofertami"),
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
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
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
    no_photos: bool = typer.Option(False, "--no-photos", help="Pomiń etap zdjęć"),
) -> None:
    cfg = load_settings()
    if no_photos:
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

@otodom.command("live")
def otodom_live(
    limit: Optional[int] = None,  # Opcjonalny limit ofert
    city: Optional[str] = None,   # Opcjonalne miasto (brak = cała Polska)
    deal: Optional[str] = None,
    kind: Optional[str] = None,
):
    """
    Tryb LIVE: Pobiera oferty z Otodom.
    Brak --city = Cała Polska.
    Brak --limit = Nieskończoność.
    """
    cfg = load_settings()
    
    # Jeśli user nie podał miasta, przekazujemy None (Adapter obsłuży to jako 'cala-polska')
    target_city = city 

    run_otodom_stream(
        city=target_city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        limit=limit,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )

# ---------------- MORIZON ----------------

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
    in_urls: str = typer.Option("data/out/urls.csv", "--in-urls", "-i", help="Ścieżka do CSV z URL-ami"),
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
    in_offers: str = typer.Option("data/out/offers.csv", "--in-offers", "-i", help="Ścieżka do CSV z ofertami"),
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
) -> None:
    cfg = load_settings()
    st = run_morizon_photos(
        offers_csv=Path(in_offers),
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir),  # ignorowane w trybie link-only
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
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
    no_photos: bool = typer.Option(False, "--no-photos", help="Pomiń etap zdjęć"),
) -> None:
    cfg = load_settings()
    if no_photos:
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

@morizon.command("live")
def morizon_live(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limit ofert"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
    city: Optional[str] = typer.Option(None, "--city", "-c"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k"),
):
    """
    Tryb LIVE: Pobiera oferty z Otodom.
    Brak --city = Cała Polska.
    Brak --limit = Nieskończoność.
    """
    cfg = load_settings()
    
    # Jeśli user nie podał miasta, przekazujemy None (Adapter obsłuży to jako 'cala-polska')
    target_city = city 

    run_morizon_stream(
        city=target_city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        limit=limit,
        max_pages=max_pages,   # ← DODAJ TO
        start_page=1,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )

@morizon.command("live-all")
def morizon_live_all(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limit ofert"),
    max_pages: int = typer.Option(200, "--max-pages", "-p", min=1, show_default=True),
    deal: Optional[str] = typer.Option(None, "--deal", "-d"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k"),
    retry_rounds: int = typer.Option(
        0,
        "--retry-rounds",
        min=0,
        show_default=True,
        help="Ile dodatkowych rund wykonać po zakończeniu listy województw, aby dokończyć regiony z fetch_fail",
    ),
    retry_sleep_s: int = typer.Option(
        120,
        "--retry-sleep-s",
        min=0,
        show_default=True,
        help="Ile sekund czekać między rundami retry",
    ),
):
    """
    Tryb LIVE-ALL:
    Działa identycznie jak `live`, ale po kolei dla wszystkich województw.
    """
    cfg = load_settings()

    done_path = Path(cfg.io.out_dir) / "morizon_live_all_done.txt"  # pełne województwa
    state_path = Path(cfg.io.out_dir) / "morizon_live_all_state.json"  # resume per-strona

    done = _load_done_regions(done_path)
    state = _load_json(state_path)

    # kompatybilność: jeśli done.txt już ma wpisy, traktuj je jako ukończone
    for r in done:
        state.setdefault(r, {})
        if isinstance(state.get(r), dict):
            state[r].setdefault("done", True)
            state[r].setdefault("last_page_done", 0)

    if done or state:
        remaining = 0
        for r in VOIVODESHIPS:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining += 1
        typer.echo(f"[LIVE-ALL] resume enabled: remaining={remaining} state={state_path}")

    # Wykonaj rundę 0 + opcjonalne rundy retry.
    max_round = int(retry_rounds or 0)
    for round_idx in range(0, max_round + 1):
        if round_idx > 0 and retry_sleep_s > 0:
            typer.echo(f"[LIVE-ALL] retry round={round_idx}/{max_round}: sleeping {retry_sleep_s}s")
            time.sleep(retry_sleep_s)

        had_fetch_fail = False
        remaining_before = 0
        for r in VOIVODESHIPS:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining_before += 1

        typer.echo(f"[LIVE-ALL] round={round_idx} remaining={remaining_before}")
        if remaining_before == 0:
            break

        for region in VOIVODESHIPS:
            r_state = state.get(region) if isinstance(state.get(region), dict) else {}
            is_done = bool(region in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if is_done:
                typer.echo(f"[LIVE-ALL] skip region={region} (already done)")
                continue

            last_page_done = 0
            if isinstance(r_state, dict):
                try:
                    last_page_done = int(r_state.get("last_page_done") or 0)
                except Exception:
                    last_page_done = 0
            start_page = max(1, last_page_done + 1)

            typer.echo(f"[LIVE-ALL] start region={region}")

            try:
                st = run_morizon_stream(
                    city=region,
                    deal=deal or cfg.defaults.deal,
                    kind=kind or cfg.defaults.kind,
                    limit=limit,
                    max_pages=max_pages,
                    start_page=start_page,
                    user_agent=cfg.http.user_agent,
                    timeout_s=cfg.http.timeout_s,
                    rps=cfg.http.rate_limit_rps,
                    http_proxy=cfg.http.http_proxy,
                    https_proxy=cfg.http.https_proxy,
                )
            except Exception as e:
                typer.echo(f"[LIVE-ALL] fail region={region} err={type(e).__name__}: {e}")
                had_fetch_fail = True
                continue

            # Aktualizacja stanu (nie oznaczaj jako done, jeśli discover przerwał na fetch_fail)
            processed = int((st or {}).get("processed_offers", 0)) if isinstance(st, dict) else 0
            last_done = int((st or {}).get("discover_last_page_done", 0)) if isinstance(st, dict) else 0
            stop_reason = (st or {}).get("discover_stop_reason") if isinstance(st, dict) else None

            state.setdefault(region, {})
            if not isinstance(state.get(region), dict):
                state[region] = {}

            state[region]["last_page_done"] = max(last_page_done, last_done)
            state[region]["stop_reason"] = stop_reason
            state[region]["processed_offers_last_run"] = processed

            if stop_reason == "fetch_fail":
                had_fetch_fail = True
                state[region]["done"] = False
                _save_json(state_path, state)
                typer.echo(
                    f"[LIVE-ALL] incomplete region={region} (fetch_fail); will resume from page={state[region]['last_page_done'] + 1}"
                )
                continue

            # Jeśli nie było fetch_fail, uznaj region za ukończony (nawet jeśli 0 ofert, np. brak wyników)
            state[region]["done"] = True
            _save_json(state_path, state)
            _append_done_region(done_path, region)
            done.add(region)
            typer.echo(f"[LIVE-ALL] done region={region}")

        if not had_fetch_fail:
            break


@morizon.command("live-all-cities")
def morizon_live_all_cities(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limit ofert"),
    max_pages: int = typer.Option(200, "--max-pages", "-p", min=1, show_default=True),
    deal: Optional[str] = typer.Option(None, "--deal", "-d"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k"),
    retry_rounds: int = typer.Option(
        0,
        "--retry-rounds",
        min=0,
        show_default=True,
        help="Ile dodatkowych rund wykonać po zakończeniu listy miast, aby dokończyć miasta z fetch_fail",
    ),
    retry_sleep_s: int = typer.Option(
        120,
        "--retry-sleep-s",
        min=0,
        show_default=True,
        help="Ile sekund czekać między rundami retry",
    ),
):
    """LIVE-ALL po miastach (VOIVODESHIPS2) z resume + retry."""
    cfg = load_settings()

    done_path = Path(cfg.io.out_dir) / "morizon_live_all_cities_done.txt"
    state_path = Path(cfg.io.out_dir) / "morizon_live_all_cities_state.json"

    done = _load_done_regions(done_path)
    state = _load_json(state_path)

    for r in done:
        state.setdefault(r, {})
        if isinstance(state.get(r), dict):
            state[r].setdefault("done", True)
            state[r].setdefault("last_page_done", 0)

    if done or state:
        remaining = 0
        for r in VOIVODESHIPS2:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining += 1
        typer.echo(f"[LIVE-ALL-CITIES] resume enabled: remaining={remaining} state={state_path}")

    max_round = int(retry_rounds or 0)
    for round_idx in range(0, max_round + 1):
        if round_idx > 0 and retry_sleep_s > 0:
            typer.echo(f"[LIVE-ALL-CITIES] retry round={round_idx}/{max_round}: sleeping {retry_sleep_s}s")
            time.sleep(retry_sleep_s)

        had_fetch_fail = False
        remaining_before = 0
        for r in VOIVODESHIPS2:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining_before += 1

        typer.echo(f"[LIVE-ALL-CITIES] round={round_idx} remaining={remaining_before}")
        if remaining_before == 0:
            break

        for city in VOIVODESHIPS2:
            r_state = state.get(city) if isinstance(state.get(city), dict) else {}
            is_done = bool(city in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if is_done:
                typer.echo(f"[LIVE-ALL-CITIES] skip city={city} (already done)")
                continue

            last_page_done = 0
            if isinstance(r_state, dict):
                try:
                    last_page_done = int(r_state.get("last_page_done") or 0)
                except Exception:
                    last_page_done = 0
            start_page = max(1, last_page_done + 1)

            typer.echo(f"[LIVE-ALL-CITIES] start city={city}")

            try:
                st = run_morizon_stream(
                    city=city,
                    deal=deal or cfg.defaults.deal,
                    kind=kind or cfg.defaults.kind,
                    limit=limit,
                    max_pages=max_pages,
                    start_page=start_page,
                    user_agent=cfg.http.user_agent,
                    timeout_s=cfg.http.timeout_s,
                    rps=cfg.http.rate_limit_rps,
                    http_proxy=cfg.http.http_proxy,
                    https_proxy=cfg.http.https_proxy,
                )
            except Exception as e:
                typer.echo(f"[LIVE-ALL-CITIES] fail city={city} err={type(e).__name__}: {e}")
                had_fetch_fail = True
                continue

            processed = int((st or {}).get("processed_offers", 0)) if isinstance(st, dict) else 0
            last_done = int((st or {}).get("discover_last_page_done", 0)) if isinstance(st, dict) else 0
            stop_reason = (st or {}).get("discover_stop_reason") if isinstance(st, dict) else None

            state.setdefault(city, {})
            if not isinstance(state.get(city), dict):
                state[city] = {}

            state[city]["last_page_done"] = max(last_page_done, last_done)
            state[city]["stop_reason"] = stop_reason
            state[city]["processed_offers_last_run"] = processed

            if stop_reason == "fetch_fail":
                had_fetch_fail = True
                state[city]["done"] = False
                _save_json(state_path, state)
                typer.echo(
                    f"[LIVE-ALL-CITIES] incomplete city={city} (fetch_fail); will resume from page={state[city]['last_page_done'] + 1}"
                )
                continue

            state[city]["done"] = True
            _save_json(state_path, state)
            _append_done_region(done_path, city)
            done.add(city)
            typer.echo(f"[LIVE-ALL-CITIES] done city={city}")

        if not had_fetch_fail:
            break


@morizon.command("sync-done-from-log")
def morizon_sync_done_from_log(
    log_path: Path = typer.Argument(..., help="Ścieżka do pliku z logiem/wyjściem konsoli z uruchomienia live-all"),
    strict_errors: bool = typer.Option(
        False,
        "--strict-errors",
        help="Jeśli ustawione, region jest DONE tylko gdy nie było żadnych logów level=ERROR w tym regionie",
    ),
):
    """Uzupełnia morizon_live_all_state.json i morizon_live_all_done.txt na podstawie zapisanego logu."""
    cfg = load_settings()
    _sync_live_all_from_log(source="morizon", log_path=log_path, out_dir=Path(cfg.io.out_dir), strict_errors=strict_errors)
    typer.echo({"ok": True, "source": "morizon", "log_path": str(log_path)})

# ---------------- GRATKA ----------------

@gratka.command("discover")
def gratka_discover_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkania|domy|dzialki|lokale"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
) -> None:
    cfg = load_settings()
    st = run_gratka_discover(
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
    typer.echo(json.dumps(st, ensure_ascii=False))

@gratka.command("detail")
def gratka_detail_cmd(
    in_urls: str = typer.Option("data/out/urls.csv", "--in-urls", "-i", help="Ścieżka do CSV z URL-ami"),
    no_debug: bool = typer.Option(False, "--no-debug", help="Nie zapisuj plików debug"),
) -> None:
    cfg = load_settings()
    st = run_gratka_detail(
        urls_csv=Path(in_urls),
        out_dir=Path(cfg.io.out_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
        allow_incomplete=False,   # twarda walidacja REQ_FIELDS
        dump_debug=not no_debug,
    )
    typer.echo(json.dumps(st, ensure_ascii=False))

@gratka.command("photos")
def gratka_photos_cmd(
    in_offers: str = typer.Option("data/out/offers.csv", "--in-offers", "-i", help="Ścieżka do CSV z ofertami"),
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
) -> None:
    cfg = load_settings()
    st = run_gratka_photos(
        offers_csv=Path(in_offers),
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir) if getattr(cfg.io, "img_dir", None) else None,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_per_offer=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(json.dumps(st, ensure_ascii=False))

@gratka.command("full")
def gratka_full_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkania|domy|dzialki|lokale"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
    no_photos: bool = typer.Option(False, "--no-photos", help="Pomiń etap zdjęć"),
) -> None:
    cfg = load_settings()
    if no_photos:
        # discover -> detail
        run_gratka_discover(
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
        st_detail = run_gratka_detail(
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
        typer.echo(json.dumps(stats, ensure_ascii=False))
        return

    st = run_gratka_full(
        city=city or cfg.defaults.city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        max_pages=max_pages,
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir) if getattr(cfg.io, "img_dir", None) else None,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_photos=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(json.dumps(st, ensure_ascii=False))

@gratka.command("live")
def gratka_live(
    limit: Optional[int] = None,  # Opcjonalny limit ofert
    city: Optional[str] = None,   # Opcjonalne miasto (brak = cała Polska)
    deal: Optional[str] = None,
    kind: Optional[str] = None,
    max_pages: int = typer.Option(1, "--max-pages", min=1),
):
    """
    Tryb LIVE: Pobiera oferty z Otodom.
    Brak --city = Cała Polska.
    Brak --limit = Nieskończoność.
    """
    cfg = load_settings()
    
    # Jeśli user nie podał miasta, przekazujemy None (Adapter obsłuży to jako 'cala-polska')
    target_city = city 

    run_gratka_stream(
        city=target_city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        limit=limit,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
        max_pages=max_pages,
        start_page=1,
    )

@gratka.command("live-all")
def gratka_live_all(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limit ofert"),
    max_pages: int = typer.Option(200, "--max-pages", "-p", min=1, show_default=True),
    deal: Optional[str] = typer.Option(None, "--deal", "-d"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k"),
    retry_rounds: int = typer.Option(
        0,
        "--retry-rounds",
        min=0,
        show_default=True,
        help="Ile dodatkowych rund wykonać po zakończeniu listy województw, aby dokończyć regiony z fetch_fail",
    ),
    retry_sleep_s: int = typer.Option(
        120,
        "--retry-sleep-s",
        min=0,
        show_default=True,
        help="Ile sekund czekać między rundami retry",
    ),
):
    """
    Tryb LIVE-ALL:
    Działa identycznie jak `live`, ale po kolei dla wszystkich województw.
    """
    cfg = load_settings()

    done_path = Path(cfg.io.out_dir) / "gratka_live_all_done.txt"
    state_path = Path(cfg.io.out_dir) / "gratka_live_all_state.json"

    done = _load_done_regions(done_path)
    state = _load_json(state_path)

    # kompatybilność: jeśli done.txt już ma wpisy, traktuj je jako ukończone
    for r in done:
        state.setdefault(r, {})
        if isinstance(state.get(r), dict):
            state[r].setdefault("done", True)
            state[r].setdefault("last_page_done", 0)

    if done or state:
        remaining = 0
        for r in VOIVODESHIPS:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining += 1
        typer.echo(f"[LIVE-ALL] resume enabled: remaining={remaining} state={state_path}")

    # Wykonaj rundę 0 + opcjonalne rundy retry.
    max_round = int(retry_rounds or 0)
    for round_idx in range(0, max_round + 1):
        if round_idx > 0 and retry_sleep_s > 0:
            typer.echo(f"[LIVE-ALL] retry round={round_idx}/{max_round}: sleeping {retry_sleep_s}s")
            time.sleep(retry_sleep_s)

        had_fetch_fail = False
        remaining_before = 0
        for r in VOIVODESHIPS:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining_before += 1

        typer.echo(f"[LIVE-ALL] round={round_idx} remaining={remaining_before}")
        if remaining_before == 0:
            break

        for region in VOIVODESHIPS:
            r_state = state.get(region) if isinstance(state.get(region), dict) else {}
            is_done = bool(region in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if is_done:
                typer.echo(f"[LIVE-ALL] skip region={region} (already done)")
                continue

            last_page_done = 0
            if isinstance(r_state, dict):
                try:
                    last_page_done = int(r_state.get("last_page_done") or 0)
                except Exception:
                    last_page_done = 0
            start_page = max(1, last_page_done + 1)

            typer.echo(f"[LIVE-ALL] start region={region}")

            try:
                st = run_gratka_stream(
                    city=region,
                    deal=deal or cfg.defaults.deal,
                    kind=kind or cfg.defaults.kind,
                    limit=limit,
                    max_pages=max_pages,
                    start_page=start_page,
                    user_agent=cfg.http.user_agent,
                    timeout_s=cfg.http.timeout_s,
                    rps=cfg.http.rate_limit_rps,
                    http_proxy=cfg.http.http_proxy,
                    https_proxy=cfg.http.https_proxy,
                )
            except Exception as e:
                typer.echo(f"[LIVE-ALL] fail region={region} err={type(e).__name__}: {e}")
                # traktuj jak niedokończone; spróbujemy w kolejnych rundach
                had_fetch_fail = True
                continue

            processed = int((st or {}).get("processed_offers", 0)) if isinstance(st, dict) else 0
            last_done = int((st or {}).get("discover_last_page_done", 0)) if isinstance(st, dict) else 0
            stop_reason = (st or {}).get("discover_stop_reason") if isinstance(st, dict) else None

            state.setdefault(region, {})
            if not isinstance(state.get(region), dict):
                state[region] = {}

            state[region]["last_page_done"] = max(last_page_done, last_done)
            state[region]["stop_reason"] = stop_reason
            state[region]["processed_offers_last_run"] = processed

            if stop_reason == "fetch_fail":
                had_fetch_fail = True
                state[region]["done"] = False
                _save_json(state_path, state)
                typer.echo(
                    f"[LIVE-ALL] incomplete region={region} (fetch_fail); will resume from page={state[region]['last_page_done'] + 1}"
                )
                continue

            state[region]["done"] = True
            _save_json(state_path, state)
            _append_done_region(done_path, region)
            done.add(region)
            typer.echo(f"[LIVE-ALL] done region={region}")

        if not had_fetch_fail:
            # Wszystkie niedokończone regiony domknęły się w tej rundzie
            break


@gratka.command("live-all-cities")
def gratka_live_all_cities(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limit ofert"),
    max_pages: int = typer.Option(200, "--max-pages", "-p", min=1, show_default=True),
    deal: Optional[str] = typer.Option(None, "--deal", "-d"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k"),
    retry_rounds: int = typer.Option(
        0,
        "--retry-rounds",
        min=0,
        show_default=True,
        help="Ile dodatkowych rund wykonać po zakończeniu listy miast, aby dokończyć miasta z fetch_fail",
    ),
    retry_sleep_s: int = typer.Option(
        120,
        "--retry-sleep-s",
        min=0,
        show_default=True,
        help="Ile sekund czekać między rundami retry",
    ),
):
    """LIVE-ALL po miastach (VOIVODESHIPS2) z resume + retry."""
    cfg = load_settings()

    done_path = Path(cfg.io.out_dir) / "gratka_live_all_cities_done.txt"
    state_path = Path(cfg.io.out_dir) / "gratka_live_all_cities_state.json"

    done = _load_done_regions(done_path)
    state = _load_json(state_path)

    for r in done:
        state.setdefault(r, {})
        if isinstance(state.get(r), dict):
            state[r].setdefault("done", True)
            state[r].setdefault("last_page_done", 0)

    if done or state:
        remaining = 0
        for r in VOIVODESHIPS2:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining += 1
        typer.echo(f"[LIVE-ALL-CITIES] resume enabled: remaining={remaining} state={state_path}")

    max_round = int(retry_rounds or 0)
    for round_idx in range(0, max_round + 1):
        if round_idx > 0 and retry_sleep_s > 0:
            typer.echo(f"[LIVE-ALL-CITIES] retry round={round_idx}/{max_round}: sleeping {retry_sleep_s}s")
            time.sleep(retry_sleep_s)

        had_fetch_fail = False
        remaining_before = 0
        for r in VOIVODESHIPS2:
            r_state = state.get(r) if isinstance(state.get(r), dict) else {}
            is_done = bool(r in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if not is_done:
                remaining_before += 1

        typer.echo(f"[LIVE-ALL-CITIES] round={round_idx} remaining={remaining_before}")
        if remaining_before == 0:
            break

        for city in VOIVODESHIPS2:
            r_state = state.get(city) if isinstance(state.get(city), dict) else {}
            is_done = bool(city in done or (isinstance(r_state, dict) and r_state.get("done") is True))
            if is_done:
                typer.echo(f"[LIVE-ALL-CITIES] skip city={city} (already done)")
                continue

            last_page_done = 0
            if isinstance(r_state, dict):
                try:
                    last_page_done = int(r_state.get("last_page_done") or 0)
                except Exception:
                    last_page_done = 0
            start_page = max(1, last_page_done + 1)

            typer.echo(f"[LIVE-ALL-CITIES] start city={city}")

            try:
                st = run_gratka_stream(
                    city=city,
                    deal=deal or cfg.defaults.deal,
                    kind=kind or cfg.defaults.kind,
                    limit=limit,
                    max_pages=max_pages,
                    start_page=start_page,
                    user_agent=cfg.http.user_agent,
                    timeout_s=cfg.http.timeout_s,
                    rps=cfg.http.rate_limit_rps,
                    http_proxy=cfg.http.http_proxy,
                    https_proxy=cfg.http.https_proxy,
                )
            except Exception as e:
                typer.echo(f"[LIVE-ALL-CITIES] fail city={city} err={type(e).__name__}: {e}")
                had_fetch_fail = True
                continue

            processed = int((st or {}).get("processed_offers", 0)) if isinstance(st, dict) else 0
            last_done = int((st or {}).get("discover_last_page_done", 0)) if isinstance(st, dict) else 0
            stop_reason = (st or {}).get("discover_stop_reason") if isinstance(st, dict) else None

            state.setdefault(city, {})
            if not isinstance(state.get(city), dict):
                state[city] = {}

            state[city]["last_page_done"] = max(last_page_done, last_done)
            state[city]["stop_reason"] = stop_reason
            state[city]["processed_offers_last_run"] = processed

            if stop_reason == "fetch_fail":
                had_fetch_fail = True
                state[city]["done"] = False
                _save_json(state_path, state)
                typer.echo(
                    f"[LIVE-ALL-CITIES] incomplete city={city} (fetch_fail); will resume from page={state[city]['last_page_done'] + 1}"
                )
                continue

            state[city]["done"] = True
            _save_json(state_path, state)
            _append_done_region(done_path, city)
            done.add(city)
            typer.echo(f"[LIVE-ALL-CITIES] done city={city}")

        if not had_fetch_fail:
            break


@gratka.command("sync-done-from-log")
def gratka_sync_done_from_log(
    log_path: Path = typer.Argument(..., help="Ścieżka do pliku z logiem/wyjściem konsoli z uruchomienia live-all"),
    strict_errors: bool = typer.Option(
        False,
        "--strict-errors",
        help="Jeśli ustawione, region jest DONE tylko gdy nie było żadnych logów level=ERROR w tym regionie",
    ),
):
    """Uzupełnia gratka_live_all_state.json i gratka_live_all_done.txt na podstawie zapisanego logu."""
    cfg = load_settings()
    _sync_live_all_from_log(source="gratka", log_path=log_path, out_dir=Path(cfg.io.out_dir), strict_errors=strict_errors)
    typer.echo({"ok": True, "source": "gratka", "log_path": str(log_path)})

# ---------------- TROJMIASTO ----------------

@trojmiasto.command("discover")
def trojmiasto_discover_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto (ignorowane, Trojmiasto.pl domyślnie scrapuje Trójmiasto)"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkanie|dom|dzialka|lokal"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
) -> None:
    cfg = load_settings()
    st = run_trojmiasto_discover(
        city=city or cfg.defaults.city, # Przekazujemy, choć adapter może ignorować
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
    typer.echo(json.dumps(st, ensure_ascii=False))

@trojmiasto.command("detail")
def trojmiasto_detail_cmd(
    in_urls: str = typer.Option("data/out/urls.csv", "--in-urls", "-i", help="Ścieżka do CSV z URL-ami"),
    no_debug: bool = typer.Option(False, "--no-debug", help="Nie zapisuj plików debug"),
) -> None:
    cfg = load_settings()
    st = run_trojmiasto_detail(
        urls_csv=Path(in_urls),
        out_dir=Path(cfg.io.out_dir),
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
        allow_incomplete=False,   # Wymuszamy kompletne dane
        dump_debug=not no_debug,
    )
    typer.echo(json.dumps(st, ensure_ascii=False))

@trojmiasto.command("photos")
def trojmiasto_photos_cmd(
    in_offers: str = typer.Option("data/out/offers.csv", "--in-offers", "-i", help="Ścieżka do CSV z ofertami"),
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
) -> None:
    cfg = load_settings()
    st = run_trojmiasto_photos(
        offers_csv=Path(in_offers),
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir) if getattr(cfg.io, "img_dir", None) else None,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_per_offer=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(json.dumps(st, ensure_ascii=False))

@trojmiasto.command("full")
def trojmiasto_full_cmd(
    city: Optional[str] = typer.Option(None, "--city", "-c", help="Miasto (ignorowane)"),
    deal: Optional[str] = typer.Option(None, "--deal", "-d", help="sprzedaz|wynajem"),
    kind: Optional[str] = typer.Option(None, "--kind", "-k", help="mieszkanie|dom|dzialka|lokal"),
    max_pages: int = typer.Option(1, "--max-pages", "-p", min=1, show_default=True),
    limit_photos: Optional[int] = typer.Option(None, "--limit-photos", "-l", help="Limit zdjęć na ofertę"),
    no_photos: bool = typer.Option(False, "--no-photos", help="Pomiń etap zdjęć"),
) -> None:
    cfg = load_settings()
    if no_photos:
        # discover -> detail
        run_trojmiasto_discover(
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
        st_detail = run_trojmiasto_detail(
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
        typer.echo(json.dumps(stats, ensure_ascii=False))
        return

    st = run_trojmiasto_full(
        city=city or cfg.defaults.city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        max_pages=max_pages,
        out_dir=Path(cfg.io.out_dir),
        img_dir=Path(cfg.io.img_dir) if getattr(cfg.io, "img_dir", None) else None,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        limit_photos=limit_photos,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
    )
    typer.echo(json.dumps(st, ensure_ascii=False))

@trojmiasto.command("live")
def trojmiasto_live(
    limit: Optional[int] = None,  # Opcjonalny limit ofert
    city: Optional[str] = None,   # Opcjonalne miasto (brak = cała Polska)
    deal: Optional[str] = None,
    kind: Optional[str] = None,
    max_pages: int = typer.Option(1, "--max-pages", min=1),
):
    """
    Tryb LIVE: Pobiera oferty z Otodom.
    Brak --city = Cała Polska.
    Brak --limit = Nieskończoność.
    """
    cfg = load_settings()
    
    # Jeśli user nie podał miasta, przekazujemy None (Adapter obsłuży to jako 'cala-polska')
    target_city = city 

    run_trojmiasto_stream(
        city=target_city,
        deal=deal or cfg.defaults.deal,
        kind=kind or cfg.defaults.kind,
        limit=limit,
        user_agent=cfg.http.user_agent,
        timeout_s=cfg.http.timeout_s,
        rps=cfg.http.rate_limit_rps,
        http_proxy=cfg.http.http_proxy,
        https_proxy=cfg.http.https_proxy,
        max_pages=max_pages,
    )

if __name__ == "__main__":
    app()


