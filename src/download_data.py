import requests
import os

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
OUTPUT_DIR = "data/bronze"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "yellow_tripdata_2023-01.parquet")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print(f"Téléchargement des données depuis {URL}...")
response = requests.get(URL)

with open(OUTPUT_FILE, "wb") as f:
    f.write(response.content) 

print(f"Données téléchargées et sauvegardées dans {OUTPUT_FILE}.")