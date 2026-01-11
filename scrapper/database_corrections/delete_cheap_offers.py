import requests
import time
import sys

# --- KONFIGURACJA PRODUKCYJNA ---
API_BASE_URL = "https://api.matiko.ovh"
PRICE_THRESHOLD = 50000.0
START_ID = 1
MAX_EMPTY_STREAK = 200

def clean_database():
    current_id = START_ID
    deleted_count = 0
    consecutive_404 = 0
    
    print(f"🔥 ROZPOCZYNAM CZYSZCZENIE (TRYB BEZ LOGOWANIA)")
    print(f"👉 Baza: {API_BASE_URL}")
    print("-" * 60)

    with requests.Session() as session:
        # Nie ustawiamy żadnych nagłówków Authorization!
        
        while True:
            try:
                url = f"{API_BASE_URL}/apartments/{current_id}"
                
                # Próba pobrania (bez tokena)
                response = session.get(url, timeout=3)

                # --- DIAGNOSTYKA DOSTĘPU ---
                if response.status_code == 401:
                    print(f"\n⛔ BŁĄD 401: Jednak autoryzacja JEST wymagana.")
                    print("Matiko się pomylił albo API jest ukryte.")
                    break
                
                if response.status_code == 403:
                    print(f"\n⛔ BŁĄD 403: Dostęp zabroniony.")
                    break

                # --- OBSŁUGA DZIUR (404) ---
                if response.status_code == 404:
                    consecutive_404 += 1
                    if consecutive_404 % 20 == 0:
                        print(".", end="", flush=True)
                    
                    if consecutive_404 >= MAX_EMPTY_STREAK:
                        print(f"\n🛑 Koniec bazy. Ostatnie ID: {current_id}")
                        break
                    current_id += 1
                    continue
                
                # --- JEST OFERTA (200) ---
                if response.status_code == 200:
                    consecutive_404 = 0 
                    data = response.json()
                    
                    try:
                        price = float(data.get("price", 0))
                    except (ValueError, TypeError):
                        price = 0.0

                    if price < PRICE_THRESHOLD:
                        print(f"\n🗑️ ID {current_id} ({price} PLN) -> DELETE...", end="")
                        
                        # Próba usunięcia (bez tokena)
                        del_res = session.delete(url)
                        
                        if del_res.status_code == 204:
                            print(" ✅ USUNIĘTO")
                            deleted_count += 1
                        elif del_res.status_code == 401:
                            print(" ❌ BŁĄD 401 (Wymaga logowania przy usuwaniu!)")
                            break
                        else:
                            print(f" ❌ BŁĄD {del_res.status_code}")
                    
                    if current_id % 50 == 0:
                        print(f"\n... ID {current_id} ...", end="")

            except KeyboardInterrupt:
                print("\n🛑 Przerwano.")
                sys.exit(0)
            except Exception as e:
                print(f"\n❌ Błąd ID {current_id}: {e}")
                time.sleep(1)

            current_id += 1

if __name__ == "__main__":
    clean_database()