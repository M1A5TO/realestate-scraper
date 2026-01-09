# realestate-scraper 

Projekt do scrapowania ofert nieruchomości i wysyłania ich do naszego backendu (API: https://api.matiko.ovh/docs#/).
Uruchamiasz komendy CLI. W zależności od trybu, scraper zapisuje pliki pośrednie (urls.csv, offers.csv, photos.csv) oraz może strumieniować oferty do backendu.

## Wymagania

- Python 3.10+ (polecane 3.11+)
- (Opcjonalnie) cloudflared, jeśli potrzebujesz tunelu do RabbitMQ

## Instalacja

Klonowanie i wejście na branch:
```bash
git clone https://github.com/M1A5TO/realestate-scraper.git
```

Wirtualne środowisko i instalacja:
Windows (PowerShell):
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -r requirements.txt
```

Linux/macOS:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

## Konfiguracja

U nas główna konfiguracja jest w kodzie, w `scrapper/config.py` (domyślne wartości).
CLI ładuje ustawienia przez `load_settings()` i korzysta z nich jako defaultów.

Jeżeli chcesz zmienić domyślne parametry (np. katalog wyjściowy, timeout, rate limit, domyślne miasto), robisz to w configu.

## Tunel do RabbitMQ (cloudflared)

Jeżeli w Twoim uruchomieniu scraper ma łączyć się z RabbitMQ przez Cloudflare Access, musisz odpalić tunel i zostawić go włączonego w osobnym oknie.

Windows (PowerShell):
```powershell
.\cloudflared.exe access tcp --hostname rabbitmq.matiko.ovh --listener localhost:5672
```

Uwaga: to dotyczy połączenia do RabbitMQ. Samo HTTP do API może działać niezależnie, ale jeśli pipeline zakłada publikację do kolejki, tunel musi być aktywny.

## CLI: szybki help

Lista wszystkich komend:
```bash
python -m scrapper.cli --help
```

Help dla konkretnego źródła:
```bash
python -m scrapper.cli <zrodlo> --help
```

Źródła dostępne w CLI:
- otodom
- morizon
- gratka
- trojmiasto

Pipeline’y:
- discover  -> pobiera listę URL-i (zapisuje urls.csv)
- detail    -> pobiera szczegóły ofert z urls.csv (zapisuje offers.csv)
- photos    -> pobiera/wysyła zdjęcia na podstawie offers.csv (zapisuje photos.csv i/lub obrazy)
- full      -> discover + detail + photos w jednym kroku
- live      -> streaming: discover -> parse -> wysyłka do backendu (+ zdjęcia + rabbitmq)
- live-all  -> live po kolei dla województw + resume (tylko morizon, gratka)
- live-all-cities -> live po kolei dla miast + resume (tylko morizon, gratka)

## Filtr ostatnich N dni (morizon + gratka)

Działa dla: live / live-all / live-all-cities.
- domyślnie: `--last-days 30`
- wyłączenie filtra: `--last-days 0` (pełny skan)

## Resume (morizon + gratka)

Resume działa tylko dla `live-all` i `live-all-cities`.

Checkpointy są rozdzielone per `--last-days`:
- `last-days=30` -> pliki stanu mają suffix `*_last30d_*`
- `last-days=0`  -> stare nazwy bez suffixu (pełny skan)

Wznowienie po crashu:
odpal identyczną komendę jeszcze raz (to samo źródło, tryb, `--last-days`).

## Przykładowe komendy (copy/paste)

### OTODOM

DISCOVER:
```bash
python -m scrapper.cli otodom discover --city "Gdańsk" --deal sprzedaz --kind mieszkanie --max-pages 5
```

DETAIL:
```bash
python -m scrapper.cli otodom detail --in-urls data/out/urls.csv
```

PHOTOS:
```bash
python -m scrapper.cli otodom photos --in-offers data/out/offers.csv --limit-photos 10
```

FULL:
```bash
python -m scrapper.cli otodom full --city "Gdańsk" --deal sprzedaz --kind mieszkanie --max-pages 5
```

LIVE (stream do backendu):
```bash
python -m scrapper.cli otodom live --city "Gdańsk" --deal sprzedaz --kind mieszkanie --limit 200
```

### MORIZON

DISCOVER / DETAIL / PHOTOS / FULL:
```bash
python -m scrapper.cli morizon discover --city "Gdańsk" --deal sprzedaz --kind mieszkanie --max-pages 5
python -m scrapper.cli morizon detail --in-urls data/out/urls.csv
python -m scrapper.cli morizon photos --in-offers data/out/offers.csv --limit-photos 10
python -m scrapper.cli morizon full --city "Gdańsk" --deal sprzedaz --kind mieszkanie --max-pages 5
```

LIVE (po mieście albo cały kraj):
```bash
python -m scrapper.cli morizon live --city "Gdańsk" --deal sprzedaz --kind mieszkanie --max-pages 5
python -m scrapper.cli morizon live --city "Gdańsk" --deal sprzedaz --kind mieszkanie --max-pages 5 --last-days 30
```

LIVE-ALL (województwa + resume):
```bash
python -m scrapper.cli morizon live-all --max-pages 200
python -m scrapper.cli morizon live-all --max-pages 200 --last-days 0
python -m scrapper.cli morizon live-all --max-pages 200 --last-days 30
```

Retry rundy (opcjonalnie, jeśli były fetch_fail):
```bash
python -m scrapper.cli morizon live-all --max-pages 200 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli morizon live-all --max-pages 200 --last-days 30 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli morizon live-all --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120
```

LIVE-ALL-CITIES (miasta + resume):
```bash
python -m scrapper.cli morizon live-all-cities --max-pages 200
python -m scrapper.cli morizon live-all-cities --max-pages 200 --last-days 0
python -m scrapper.cli morizon live-all-cities --max-pages 200 --last-days 30
```

Retry rundy (opcjonalnie):
```bash
python -m scrapper.cli morizon live-all-cities --max-pages 200 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli morizon live-all-cities --max-pages 200 --last-days 30 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli morizon live-all-cities --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120
```

Limit ofert (testy / przyspieszenie):
```bash
python -m scrapper.cli morizon live-all --max-pages 200 --last-days 30 --limit 500 --retry-rounds 200 --retry-sleep-s 120
```

Sync stanu z zapisanego logu:
```bash
python -m scrapper.cli morizon sync-done-from-log path\to\log.txt
```

### GRATKA

DISCOVER / DETAIL / PHOTOS / FULL:
```bash
python -m scrapper.cli gratka discover --city "Gdańsk" --deal sprzedaz --kind mieszkania --max-pages 5
python -m scrapper.cli gratka detail --in-urls data/out/urls.csv
python -m scrapper.cli gratka photos --in-offers data/out/offers.csv --limit-photos 10
python -m scrapper.cli gratka full --city "Gdańsk" --deal sprzedaz --kind mieszkania --max-pages 5
```

LIVE:
```bash
python -m scrapper.cli gratka live --city "Gdańsk" --deal sprzedaz --kind mieszkania --max-pages 5
python -m scrapper.cli gratka live --city "Gdańsk" --deal sprzedaz --kind mieszkania --max-pages 5 --last-days 30
```

LIVE-ALL (województwa + resume):
```bash
python -m scrapper.cli gratka live-all --max-pages 200
python -m scrapper.cli gratka live-all --max-pages 200 --last-days 0
python -m scrapper.cli gratka live-all --max-pages 200 --last-days 30
```

Retry rundy (opcjonalnie):
```bash
python -m scrapper.cli gratka live-all --max-pages 200 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli gratka live-all --max-pages 200 --last-days 30 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli gratka live-all --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120
```

LIVE-ALL-CITIES (miasta + resume):
```bash
python -m scrapper.cli gratka live-all-cities --max-pages 200
python -m scrapper.cli gratka live-all-cities --max-pages 200 --last-days 0
python -m scrapper.cli gratka live-all-cities --max-pages 200 --last-days 30
```

Retry rundy (opcjonalnie):
```bash
python -m scrapper.cli gratka live-all-cities --max-pages 200 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli gratka live-all-cities --max-pages 200 --last-days 30 --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli gratka live-all-cities --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120
```

Sync stanu z zapisanego logu:
```bash
python -m scrapper.cli gratka sync-done-from-log path\to\log.txt
```

### TROJMIASTO.PL

DISCOVER / DETAIL / PHOTOS / FULL:
```bash
python -m scrapper.cli trojmiasto discover --max-pages 5
python -m scrapper.cli trojmiasto detail --in-urls data/out/urls.csv
python -m scrapper.cli trojmiasto photos --in-offers data/out/offers.csv --limit-photos 10
python -m scrapper.cli trojmiasto full --max-pages 5
```

LIVE:
```bash
python -m scrapper.cli trojmiasto live --limit 200
```

## Zalecane uruchomienie (maksymalna liczba ofert)

Jeśli celem jest zebranie możliwie pełnej bazy ofert, odpal poniższe komendy. Warianty z `--last-days 0` robią pełny skan (bez filtra ostatnich dni) i mogą działać długo.

```bash
# trojmiasto.pl (Trójmiasto) – limit ofert w streamie
python -m scrapper.cli trojmiasto live --limit 200

# otodom – streaming do backendu
python -m scrapper.cli otodom live --city "Gdańsk" --deal sprzedaz --kind mieszkanie --limit 200

# gratka – cały kraj + pełny skan + retry
python -m scrapper.cli gratka live-all-cities --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli gratka live-all --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120

# morizon – cały kraj + pełny skan + retry
python -m scrapper.cli morizon live-all-cities --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120
python -m scrapper.cli morizon live-all --max-pages 200 --last-days 0  --retry-rounds 200 --retry-sleep-s 120


## Backend (API)

Swagger:
https://api.matiko.ovh/docs#/
