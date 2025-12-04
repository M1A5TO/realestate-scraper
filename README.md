 Uruchamianie w trybie Live (Strumieniowanie)
Scraper działa w trybie live, przetwarzając oferty i wysyłając je do bazy danych na bieżąco.

Ogólna składnia:

Bash

python -m scrapper.cli <NAZWA_STRONY> live --city "<MIASTO>" --limit <LICZBA>
--limit X: Pobiera X najnowszych ofert i kończy pracę. Usuń tę flagę, aby pobrać wszystkie dostępne oferty (do końca wyników).

--city: Określa miasto wyszukiwania. Jeśli pominiesz ten parametr, scraper przeszuka całą Polskę (dla serwisów ogólnopolskich).

 Otodom
Uwaga: Otodom wymaga podania lokalizacji w formacie wojewodztwo/miasto.

Bash

# Przykład: 5 ofert z Gdańska (woj. pomorskie)
python -m scrapper.cli otodom live --city "pomorskie/gdansk" --limit 5

# Pobranie wszystkich ofert z Gdańska
python -m scrapper.cli otodom live --city "pomorskie/gdansk"
 Morizon
Bash

# Przykład z limitem
python -m scrapper.cli morizon live --city "Gdańsk" --limit 5

# Pobranie wszystkich ofert (bez limitu)
python -m scrapper.cli morizon live --city "Gdańsk"
 Gratka
Bash

python -m scrapper.cli gratka live --city "Gdańsk" --limit 5
 Trójmiasto.pl
Bash

python -m scrapper.cli trojmiasto live --city "Gdańsk" --limit 5