import os
import json
import requests

API_URL = os.getenv("API_URL", "http://100.68.189.53:8000").rstrip("/")

# filtry opcjonalne (ustaw przez env albo zostaw puste)
CITY = os.getenv("CITY")              # np. "Gdańsk"
PROFILE = os.getenv("PROFILE")        # student|single|dog_owner|family|universal
MAX_PRICE = os.getenv("MAX_PRICE")    # np. "600000"
MIN_FOOTAGE = os.getenv("MIN_FOOTAGE")# np. "35"
SKIP = int(os.getenv("SKIP", "0"))
LIMIT = int(os.getenv("LIMIT", "20"))

def main() -> int:
    params = {"skip": SKIP, "limit": LIMIT}
    if CITY:
        params["city"] = CITY
    if PROFILE:
        params["profile"] = PROFILE
    if MAX_PRICE:
        params["max_price"] = float(MAX_PRICE)
    if MIN_FOOTAGE:
        params["min_footage"] = float(MIN_FOOTAGE)

    r = requests.get(f"{API_URL}/apartments", params=params, timeout=15)
    r.raise_for_status()

    data = r.json()

    # backend może zwrócić listę albo obiekt; obsłuż oba
    rows = data if isinstance(data, list) else data.get("items") or data.get("data") or data.get("results") or data

    if isinstance(rows, list):
        for a in rows:
            print(json.dumps(a, ensure_ascii=False))
    else:
        # jakby backend zwrócił obiekt (np. z metadanymi)
        print(json.dumps(rows, ensure_ascii=False, indent=2))

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
