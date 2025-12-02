import requests
from typing import Optional, Dict, Any
from scrapper.core.log import get_logger

log = get_logger("backend")

class BackendClient:
    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip("/")

    def find_apartment_id(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Szuka ID mieszkania w bazie używając surowych danych ze scrappera.
        Zakłada, że dane są już poprawnymi typami (float/int).
        """
        url = f"{self.api_url}/apartments/duplicates/check"
        
        # Pobieramy dane wprost (ufamy walidacji Pydantic ze scrappera)
        lat = data.get("lat")
        lon = data.get("lon")
        price = data.get("price_amount")
        footage = data.get("area_m2")

        # Jeśli brakuje jakiejkolwiek kluczowej danej, nie szukamy
        if not all([lat, lon, price, footage]):
            return None

        payload = {
            "center": {"lat": lat, "lng": lon},
            "radius_m": 50,
            "price_min": price - 100,
            "price_max": price + 100,
            "footage_min": footage - 1.0,
            "footage_max": footage + 1.0,
            "limit": 1
        }

        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                results = resp.json()
                
                # Obsługa formatu: {'matches': [...]} lub [...]
                if isinstance(results, dict):
                    matches = results.get("matches", [])
                    if matches:
                        return int(matches[0]['id'])
                elif isinstance(results, list) and results:
                     return int(results[0]['id'])
        except Exception:
            pass
        
        return None

    def check_duplicate(self, data: Dict[str, Any]) -> bool:
        """
        Sprawdza czy oferta to duplikat.
        """
        url = f"{self.api_url}/apartments/duplicates/check"

        lat = data.get("lat")
        lon = data.get("lon")
        price = data.get("price_amount")
        footage = data.get("area_m2")

        if not all([lat, lon, price, footage]):
            return False

        payload = {
            "center": {"lat": lat, "lng": lon},
            "radius_m": 50,
            "price_min": price - 100,
            "price_max": price + 100,
            "footage_min": footage - 1.0,
            "footage_max": footage + 1.0,
            "limit": 1
        }

        try:
            resp = requests.post(url, json=payload, timeout=2)
            if resp.status_code == 200:
                results = resp.json()
                if isinstance(results, dict):
                    return results.get("has_matches", False)
                elif isinstance(results, list):
                    return len(results) > 0
        except Exception as e:
            log.warning("backend_dup_check_error", extra={"error": str(e)})
        
        return False

    def create_apartment(self, data: Dict[str, Any]) -> bool:
        """
        Wysyła dane 1:1 do API.
        """
        url = f"{self.api_url}/apartments"
        
        price = data.get("price_amount")
        
        # ZABEZPIECZENIE: Nawet jeśli scrapper przepuścił 0.0 (bo to nie None),
        # my tutaj blokujemy wysyłkę do bazy.
        if not price or price <= 0:
            log.warning("api_skip_zero_price", extra={"offer_id": data.get("offer_id")})
            return False

        # Przypisujemy dokładnie to, co przyszło ze scrappera
        payload = {
            "source_website": data.get("source", "unknown"),
            "source_id": str(data.get("offer_id", "")),
            "source_url": data.get("url", "") or data.get("offer_url", ""),
            
            "price": price,
            "currency": data.get("price_currency", "PLN"),
            
            "room_num": data.get("rooms", 1),
            "footage": data.get("area_m2"),
            "price_per_m2": data.get("price_per_m2"),
            
            "city": data.get("city", "Nieznane"),
            "geolocation": {
                "lat": data.get("lat"),
                "lng": data.get("lon")
            },
            
            "description": data.get("description", "")[:5000],

            # Pola domyślne (Backend ich wymaga, scrapper ich nie ma)
            "photo_attractiveness": 0,
            "student_attractiveness": 0,
            "family_attractiveness": 0,
            "single_attractiveness": 0,
            "dog_owner_attractiveness": 0,
            "universal_attractiveness": 0,
            "poi_desc": None,
            "price_desc": None,
            "size_desc": None
        }

        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code in (200, 201):
                #wyciągamy id nadane od bazy do mieszkania
                return int(resp.json().get("id"))
            else:
                # --- DEBUG START ---
                print(f"\n[DEBUG ERROR 422] Payload: {payload}")
                print(f"[DEBUG MSG] Odpowiedź: {resp.text}\n")
                # --- DEBUG END ---
                log.warning("backend_create_fail", extra={"status": resp.status_code, "msg": resp.text})
                return False
        except Exception as e:
            log.error("backend_conn_fail", extra={"error": str(e)})
            return False

    def upload_photo(self, apartment_id: int, photo_url: str) -> bool:
        """
        Wysyła LINK do zdjęcia.
        """
        url = f"{self.api_url}/photos"
        
        payload = {
            "apartment_id": apartment_id,
            "link": photo_url,
            "style": "other"
        }

        try:
            resp = requests.post(url, json=payload, timeout=5)
            
            if resp.status_code in (200, 201):
                return True
            else:
                # --- DEBUG START (Dodaj to!) ---
                print(f"\n[DEBUG ERROR] Kod: {resp.status_code}")
                print(f"[DEBUG RESP] Treść: {resp.text}")
                print(f"[DEBUG PAYLOAD] Wysyłane dane: {payload}\n")
                # --- DEBUG END ---
                
                log.warning("photo_send_fail", extra={"status": resp.status_code, "msg": resp.text})
                return False
        except Exception as e:
            log.error("photo_send_error", extra={"error": str(e)})
            return False