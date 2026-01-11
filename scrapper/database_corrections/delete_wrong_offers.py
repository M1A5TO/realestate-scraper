import csv
import time
import sys
import unicodedata
import requests

API_BASE_URL = "https://api.matiko.ovh"
START_ID = 1
MAX_EMPTY_STREAK = 200
TIMEOUT_S = 3

MIN_PRICE = 50000.0
MAX_PRICE = 18000000.0
MIN_FOOTAGE = 10.0
MAX_FOOTAGE = 500.0

TERYT_CSV = "teryt_potwierdzone.csv"


def city_key(city: str) -> str:
    if not city:
        return ""
    city = city.strip().lower()
    city = unicodedata.normalize("NFKD", city)
    city = "".join(ch for ch in city if not unicodedata.combining(ch))
    city = city.replace("ł", "l").replace("Ł", "l")
    city = city.replace("-", " ")
    city = " ".join(city.split())
    return city


def load_teryt_map(path: str) -> dict[str, str]:
    # Zbieramy wszystkie możliwe oficjalne nazwy dla danego klucza.
    # Potem do finalnej mapy przepuszczamy tylko klucze jednoznaczne (1 nazwa).
    candidates: dict[str, set[str]] = {}

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            print("TERYT_CSV does not have columns.")
            sys.exit(1)

        if "Wejście" not in reader.fieldnames or "Nazwa w TERYT" not in reader.fieldnames:
            print("TERYT_CSV must contain columns: 'Wejście' and 'Nazwa w TERYT'.")
            sys.exit(1)

        for row in reader:
            raw_in = (row.get("Wejście") or "").strip()
            official = (row.get("Nazwa w TERYT") or "").strip()
            if not raw_in or not official:
                continue

            key = city_key(raw_in)
            if not key:
                continue

            candidates.setdefault(key, set()).add(official)

    # Finalna mapa: tylko klucze jednoznaczne
    out: dict[str, str] = {}
    ambiguous = []

    for key, names in candidates.items():
        if len(names) == 1:
            out[key] = next(iter(names))
        else:
            ambiguous.append((key, sorted(names)))

    # Informacyjnie (nie wpływa na działanie): ile kluczy pominięto
    if ambiguous:
        print(f"TERYT ambiguous keys skipped: {len(ambiguous)}")
        # Jeśli chcesz wypisać je wszystkie, odkomentuj:
        # for key, names in ambiguous:
        #     print(f"  {key} -> {names}")

    return out



def to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def should_delete(price: float | None, footage: float | None) -> tuple[bool, str]:
    # DELETE tylko, gdy wartości są poza zakresem.
    # Brak wartości (None) nie powoduje usunięcia.

    if price is not None:
        if price < MIN_PRICE:
            return True, "price_too_low"
        if price > MAX_PRICE:
            return True, "price_too_high"

    if footage is not None:
        if footage < MIN_FOOTAGE:
            return True, "footage_too_low"
        if footage > MAX_FOOTAGE:
            return True, "footage_too_high"

    return False, ""


def clean_database():
    teryt_map = load_teryt_map(TERYT_CSV)

    apartment_id = START_ID
    deleted_count = 0
    city_updated_count = 0
    consecutive_404 = 0

    print("Starting database cleanup")
    print(f"API: {API_BASE_URL}")
    print(f"Price range: {MIN_PRICE}..{MAX_PRICE} (delete only if outside)")
    print(f"Footage range: {MIN_FOOTAGE}..{MAX_FOOTAGE} (delete only if outside)")
    print("-" * 60)

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

                city = (data.get("city") or "").strip() or None
                price = to_float(data.get("price"))
                footage = to_float(data.get("footage"))

                # Miasto: patch tylko jeśli dopasowanie do TERYT istnieje
                if city:
                    city_official = teryt_map.get(city_key(city))
                    if city_official and city_official != city:
                        patch = session.patch(url, json={"city": city_official}, timeout=TIMEOUT_S)
                        if patch.status_code in (200, 204):
                            city_updated_count += 1
                            print(f"\nid {apartment_id}: city updated '{city}' -> '{city_official}'")
                        else:
                            print(f"\nid {apartment_id}: city update failed (HTTP {patch.status_code}) '{city}' -> '{city_official}'")

                # DELETE tylko za price/footage poza zakresem
                delete_it, reason = should_delete(price, footage)
                if delete_it:
                    print(f"\nid {apartment_id}: {reason} (price={price}, footage={footage}) -> delete", end="")
                    del_res = session.delete(url, timeout=TIMEOUT_S)
                    if del_res.status_code in (200, 204):
                        deleted_count += 1
                        print(" OK")
                    else:
                        print(f" FAIL (HTTP {del_res.status_code})")

                if apartment_id % 50 == 0:
                    print(f"\nprogress id {apartment_id}, deleted={deleted_count}, city_updated={city_updated_count}", end="")

            except KeyboardInterrupt:
                print("\nInterrupted.")
                sys.exit(0)
            except Exception as e:
                print(f"\nError at id {apartment_id}: {e}")
                time.sleep(1)

            apartment_id += 1


if __name__ == "__main__":
    clean_database()
