from pathlib import Path
import csv
import json
import pandas as pd


BASE_DIR = Path(r"C:\Users\pytek\Desktop\mierzalnosc")
DEBUG_JSONL = BASE_DIR / "offers_debug2.jsonl"

ADAPTERS = {
    "GRATKA": BASE_DIR / "gratka",
    "OTODOM": BASE_DIR / "otodom",
    "MORIZON": BASE_DIR / "morizon",
    "TROJMIASTO": BASE_DIR / "trojmiasto",
}

OFFER_COLUMNS = [
    "source",
    "url",
    "price_amount",
    "price_currency",
    "price_per_m2",
    "city",
    "lat",
    "lon",
    "area_m2",
    "rooms",
]


def read_urls(adapter_dir: Path) -> list[str]:
    path = adapter_dir / "urls.csv"
    if not path.exists():
        return []

    urls = []
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            u = row.get("offer_url") or row.get("url")
            if u:
                urls.append(u.strip())
    return urls


def read_offers(adapter_dir: Path) -> dict[str, dict]:
    """
    Mapuje: url -> cały wiersz z offers.csv
    """
    path = adapter_dir / "offers.csv"
    if not path.exists():
        return {}

    out = {}
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            u = row.get("url")
            if u:
                out[u.strip()] = row
    return out


def read_global_debug(path: Path) -> dict[str, str]:
    out = {}
    if not path.exists():
        return out

    with path.open(encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue

            url = obj.get("url")
            if not url:
                continue

            if "err" in obj:
                out[url] = obj["err"]
            elif "missing" in obj:
                out[url] = "missing: " + ",".join(obj["missing"])
            else:
                out[url] = "nieznany błąd"

    return out


def build_dataframe(adapter_dir: Path, debug_map: dict[str, str]) -> pd.DataFrame:
    urls = read_urls(adapter_dir)
    offers = read_offers(adapter_dir)

    rows = []
    for u in urls:
        row = {"url_oferty": u}

        if u in offers:
            offer = offers[u]
            for col in OFFER_COLUMNS:
                row[col] = offer.get(col, "")
            row["wynik"] = ""
        else:
            for col in OFFER_COLUMNS:
                row[col] = ""
            row["wynik"] = debug_map.get(u, "brak danych")

        rows.append(row)

    return pd.DataFrame(rows)


def main():
    debug_map = read_global_debug(DEBUG_JSONL)
    out_xlsx = BASE_DIR / "raport_mierzalnosc1.xlsx"

    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
        for name, path in ADAPTERS.items():
            df = build_dataframe(path, debug_map)
            df.to_excel(writer, sheet_name=name, index=False)

    print(f"Gotowe: {out_xlsx}")


if __name__ == "__main__":
    main()
