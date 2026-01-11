import csv
import time
import sys
import unicodedata
import re
from pathlib import Path

import requests

API_BASE_URL = "https://api.matiko.ovh"
START_ID = 1
MAX_EMPTY_STREAK = 200
TIMEOUT_S = 3

# CSV z mapowaniem miast:
# wejscie  -> klucz (bez polskich znaków, małe litery; dopuszczalne "-")
# miasto   -> poprawna nazwa (z polskimi znakami)
CITY_CSV = Path(__file__).with_name("miasta_z_polskimi_znakami_normalized2.csv")
if not CITY_CSV.exists():
    CITY_CSV = Path("miasta_z_polskimi_znakami_normalized2.csv")


_DASHES = {
    "\u2010": "-",  # hyphen
    "\u2011": "-",  # non-breaking hyphen
    "\u2012": "-",  # figure dash
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
    "\u2212": "-",  # minus sign
}


def city_key(value: str) -> str:
    """
    Normalizuje nazwę miasta do klucza porównawczego:
    1) małe litery
    2) bez znaków diakrytycznych
    3) różne typy myślników -> "-"
    4) porządkuje spacje i spacje wokół "-"
    """
    if not value:
        return ""

    s = value.strip().lower()

    # ujednolicenie "myślników"
    s = "".join(_DASHES.get(ch, ch) for ch in s)

    # usuwanie znaków diakrytycznych (NFKD + stripping combining marks)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # ł nie jest znakiem łączącym, więc poprawiamy ręcznie
    s = s.replace("ł", "l")

    # normalizacja spacji (w tym wokół myślnika)
    s = re.sub(r"\s*-\s*", "-", s)
    s = " ".join(s.split())

    return s


def load_city_map(path: Path) -> dict[str, str]:
    """
    Ładuje mapę: city_key(wejscie) -> miasto
    Pomija klucze niejednoznaczne (gdy ten sam 'wejscie' ma różne 'miasto').
    """
    candidates: dict[str, set[str]] = {}

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            print("CITY_CSV does not have columns.")
            sys.exit(1)

        # obsługa nagłówków niezależnie od wielkości liter
        cols = {name.strip().lower(): name for name in reader.fieldnames}
        if "wejscie" not in cols or "miasto" not in cols:
            print("CITY_CSV must contain columns: 'wejscie' and 'miasto'.")
            sys.exit(1)

        in_col = cols["wejscie"]
        out_col = cols["miasto"]

        for row in reader:
            raw_in = (row.get(in_col) or "").strip()
            official = (row.get(out_col) or "").strip()
            if not raw_in or not official:
                continue

            key = city_key(raw_in)
            if not key:
                continue

            candidates.setdefault(key, set()).add(official)

    out: dict[str, str] = {}
    ambiguous = []

    for key, names in candidates.items():
        if len(names) == 1:
            out[key] = next(iter(names))
        else:
            ambiguous.append((key, sorted(names)))

    if ambiguous:
        print(f"CITY_CSV ambiguous keys skipped: {len(ambiguous)}")

    return out


def clean_database():
    city_map = load_city_map(CITY_CSV)

    apartment_id = START_ID
    city_updated_count = 0
    consecutive_404 = 0

    print("Starting city normalization")
    print(f"API: {API_BASE_URL}")
    print(f"CSV: {CITY_CSV} (keys={len(city_map)})")
    print("=" * 60)

    with requests.Session() as session:
        while True:
            try:
                url = f"{API_BASE_URL}/apartments/{apartment_id}"
                response = session.get(url, timeout=TIMEOUT_S)

                if response.status_code == 401:
                    print("HTTP 401: authorization required")
                    break
                if response.status_code == 403:
                    print("HTTP 403: forbidden")
                    break

                if response.status_code == 404:
                    consecutive_404 += 1
                    if consecutive_404 % 20 == 0:
                        print(".", end="", flush=True)
                    if consecutive_404 >= MAX_EMPTY_STREAK:
                        print(f"\nEnd of database. Last checked id: {apartment_id}")
                        break
                    apartment_id += 1
                    continue

                if response.status_code != 200:
                    print(f"\nHTTP {response.status_code} for id {apartment_id}")
                    apartment_id += 1
                    continue

                consecutive_404 = 0
                data = response.json() if response.content else {}

                city = (data.get("city") or "").strip()
                if city:
                    target_city = city_map.get(city_key(city))
                    if target_city and target_city != city:
                        patch = session.patch(url, json={"city": target_city}, timeout=TIMEOUT_S)
                        if patch.status_code in (200, 204):
                            city_updated_count += 1
                            print(f"\nid {apartment_id}: city updated '{city}' -> '{target_city}'")
                        else:
                            print(
                                f"\nid {apartment_id}: city update failed "
                                f"(HTTP {patch.status_code}) '{city}' -> '{target_city}'"
                            )

                if apartment_id % 200 == 0:
                    print(f"\nprogress id {apartment_id}, city_updated={city_updated_count}", end="")

            except KeyboardInterrupt:
                print("\nInterrupted.")
                sys.exit(0)
            except Exception as e:
                print(f"\nError at id {apartment_id}: {e}")
                time.sleep(1)

            apartment_id += 1


if __name__ == "__main__":
    clean_database()
