# scrapper/pipelines/stream.py
from __future__ import annotations
from pathlib import Path  # <--- WAŻNY IMPORT
from scrapper.core.http import HttpClient, build_proxies
from scrapper.core.backend import BackendClient
from scrapper.core.log import setup_json_logger
from scrapper.config import load_settings
from scrapper.core.validate import Offer

# Import adaptera Otodom
from scrapper.adapters.otodom import OtodomAdapter
from scrapper.adapters.morizon import MorizonAdapter
from scrapper.adapters.gratka import GratkaAdapter
from scrapper.adapters.trojmiasto import TrojmiastoAdapter

def process_single_offer(url: str, adapter, backend, log, save_html: bool = False):
    """
    Przetwarza jedną ofertę od A do Z.
    save_html=True -> Zapisuje plik .html na dysku do inspekcji.
    """
    offer_id = "unknown"
    
    try:
        # --- DEBUG SAVE (Pobranie i zapis HTML) ---
        if save_html:
            try:
                # Musimy pobrać treść ponownie (lub adapter musiałby ją zwracać)
                # Dla testów (max_pages=1) dodatkowe zapytanie nie szkodzi.
                raw_html = adapter.http.get(url).text
                
                # Wyciągamy ID z URL-a dla nazwy pliku (prosta heurystyka)
                temp_id = url.split("-ID")[-1] if "-ID" in url else url.split("/")[-1]
                temp_id = temp_id[:20] # Bezpieczna długość
                
                debug_dir = Path("data/out/debug_html")
                debug_dir.mkdir(parents=True, exist_ok=True)
                (debug_dir / f"{temp_id}.html").write_text(raw_html, encoding="utf-8")
            except Exception as e:
                log.warning("stream_debug_save_fail", extra={"err": str(e)})
        # ------------------------------------------

        # 1. Pobierz szczegóły (Detail)
        data = adapter.parse_offer(url)
        # Szybka weryfikacja ---
        if not data.get('lat') or not data.get('lon'):
            log.warning("stream_skip_no_geo", extra={"offer_id": data.get("offer_id")})
            return # Przerywamy, szkoda czasu na pytanie backendu
        # ------------------------------------
        offer_id = data.get("offer_id", "unknown")
        
        # Walidacja (Pydantic)
        try:
            Offer(**data)
        except Exception:
            log.warning("stream_validate_fail", extra={"offer_id": offer_id})
            return

        # 2. Sprawdź duplikat (Backend)
        if backend.check_duplicate(data):
            log.info("stream_duplicate_skip", extra={"offer_id": offer_id})
            return 

        # 3. Wyślij do bazy (Backend)
        db_id = backend.create_apartment(data)
        
        if not db_id:
            # Tu trafiają oferty odrzucone przez backend (np. cena=0)
            log.warning("stream_create_fail", extra={"offer_id": offer_id})
            return

        log.info("stream_create_success", extra={"offer_id": offer_id, "db_id": db_id})

        # 4. Zdjęcia (Photos)
        try:
            photos = adapter.parse_photos(url)
            uploaded_count = 0

            # Print diagnostyczny
            print(f"\n--- DEBUG PHOTOS: Znaleziono {len(photos)} zdjęć dla {offer_id} ---")

            # Limit 10 zdjęć na ofertę dla testów
            for photo_meta in photos: 
                # Tu wywołujemy backend (diagnostyczny)
                success = backend.upload_photo(db_id, photo_meta['url'])
                if success:
                    uploaded_count += 1
                else:
                    print(f"--- DEBUG: Błąd wysyłania zdjęcia: {photo_meta['url']}")
            
            log.info("stream_photos_done", extra={"db_id": db_id, "count": uploaded_count})

        except Exception as e:
            # TO JEST NAJWAŻNIEJSZE - wypisz pełny błąd(diagnostyczny)
            import traceback
            traceback.print_exc()
            log.warning("stream_photos_fail", extra={"err": str(e), "offer_id": offer_id})
    except Exception as e:
        log.error("stream_offer_fail", extra={"url": url, "err": str(e)})


def run_otodom_stream(
    *,
    city: str | None,       # Może być None (Cała Polska)
    deal: str,
    kind: str,
    limit: int | None,      # Limit OFERT (a nie stron)
    user_agent: str,
    timeout_s: int,
    rps: float,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
):
    """
    Główna pętla strumieniowa dla Otodom.
    """
    log = setup_json_logger()
    cfg = load_settings()
    backend = BackendClient(api_url=cfg.http.api_url)
    
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    
    try:
        adapter = OtodomAdapter().with_deps(http=http, out_dir=cfg.io.out_dir)

        log.info("stream_start", extra={"source": "otodom", "city": city, "limit": limit})
        
        # Wywołujemy discover z max_pages=None (nieskończoność), sterujemy limitem ofert w pętli
        rows = adapter.discover(city=city, deal=deal, kind=kind, max_pages=None)
        
        processed_count = 0

        for row in rows:
            # --- HAMULEC (Limit Ofert) ---
            if limit is not None and processed_count >= limit:
                log.info("stream_limit_reached", extra={"limit": limit})
                break
            # -----------------------------

            url = row.get('url') or row.get('offer_url')
            if not url: continue
            
            # Przetwarzanie jednej oferty
            process_single_offer(url, adapter, backend, log, save_html=False)
            
            processed_count += 1

    finally:
        http.close()

def run_morizon_stream(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    user_agent: str,
    timeout_s: int,
    rps: float,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
):
    """Główna pętla strumieniowa dla Morizon."""
    log = setup_json_logger()
    cfg = load_settings()
    backend = BackendClient(api_url=cfg.http.api_url)
    
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    
    try:
        # ZMIANA ADAPTERA TUTAJ:
        adapter = MorizonAdapter().with_deps(http=http, out_dir=cfg.io.out_dir)

        for page in range(1, max_pages + 1):
            log.info("stream_page_start", extra={"page": page, "source": "morizon"})
            
            # Parametry discover mogą się różnić w zależności od adaptera!
            # Morizon/Gratka/Trojmiasto też przyjmują city, deal, kind, max_pages
            rows = adapter.discover(city=city, deal=deal, kind=kind, max_pages=max_pages)
            
            log.info("stream_discovered_rows", extra={"count": "generator"}) # Pamiętaj o generatorze!

            for row in rows:
                url = row.get('url') or row.get('offer_url')
                if not url: continue
                
                # Używamy tej samej uniwersalnej funkcji process_single_offer!
                process_single_offer(url, adapter, backend, log, save_html=False) # save_html=False dla produkcji
            break 
    finally:
        http.close()

def run_gratka_stream(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    user_agent: str,
    timeout_s: int,
    rps: float,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
):
    """
    Główna pętla strumieniowa dla Gratka.pl.
    """
    log = setup_json_logger()
    cfg = load_settings()
    backend = BackendClient(api_url=cfg.http.api_url)
    
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    
    try:
        # GratkaAdapter wymaga out_dir (choćby do cache'owania)
        adapter = GratkaAdapter().with_deps(http=http, out_dir=cfg.io.out_dir)

        for page in range(1, max_pages + 1):
            log.info("stream_page_start", extra={"page": page, "source": "gratka"})
            
            # Pobieramy linki (GratkaAdapter obsługuje te same parametry)
            # Uwaga: Gratka może zwracać generator, więc rzutujemy na listę dla bezpieczeństwa
            # lub iterujemy bezpośrednio.
            rows = adapter.discover(city=city, deal=deal, kind=kind, max_pages=max_pages)
            
            log.info("stream_start_processing_rows", extra={"source": "gratka"})

            for row in rows:
                url = row.get('url') or row.get('offer_url')
                
                if not url:
                    log.warning("stream_missing_url", extra={"row": row})
                    continue
                
                # Używamy uniwersalnej funkcji przetwarzania (tej samej co dla Otodom!)
                process_single_offer(url, adapter, backend, log, save_html=False)
                
            # Adapter Gratki w discover() też iteruje po stronach, więc jedna iteracja wystarczy
            break 

    finally:
        http.close()

def run_trojmiasto_stream(
    *,
    city: str,
    deal: str,
    kind: str,
    max_pages: int,
    user_agent: str,
    timeout_s: int,
    rps: float,
    http_proxy: str | None = None,
    https_proxy: str | None = None,
):
    """
    Główna pętla strumieniowa dla Trojmiasto.pl.
    """
    log = setup_json_logger()
    cfg = load_settings()
    backend = BackendClient(api_url=cfg.http.api_url)
    
    http = HttpClient(
        user_agent=user_agent,
        timeout_s=timeout_s,
        rps=rps,
        proxies=build_proxies(http_proxy, https_proxy),
    )
    
    try:
        # TrojmiastoAdapter też wymaga out_dir
        adapter = TrojmiastoAdapter().with_deps(http=http, out_dir=cfg.io.out_dir)

        for page in range(1, max_pages + 1):
            log.info("stream_page_start", extra={"page": page, "source": "trojmiasto"})
            
            # Pobieranie linków
            rows = adapter.discover(city=city, deal=deal, kind=kind, max_pages=max_pages)
            
            log.info("stream_start_processing_rows", extra={"source": "trojmiasto"})

            for row in rows:
                url = row.get('url') or row.get('offer_url')
                
                if not url:
                    log.warning("stream_missing_url", extra={"row": row})
                    continue
                
                # Przetwarzanie oferty (pobranie -> backend -> zdjęcia)
                process_single_offer(url, adapter, backend, log, save_html=False)
                
            break 

    finally:
        http.close()