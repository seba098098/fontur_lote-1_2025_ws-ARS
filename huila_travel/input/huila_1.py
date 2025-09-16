import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

base_url = "https://huila.travel/inventarios?page={}"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

data = []
max_pages = 30  # ajustar si sabes cuántas páginas hay

for page in range(1, max_pages + 1):
    url = base_url.format(page)
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Página {page} devolvió status {response.status_code}. Saltando.")
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        items = soup.find_all("a", class_="booking-item")
        if not items:
            print(f"No se encontraron items en la página {page}. Puede que ya no haya más.")
            break

        for item in items:
            nombre_tag = item.find("h5", class_="booking-item-title")
            ubicacion_tag = item.find("p", class_="booking-item-address")
            descripcion_tag = item.find("small")
            link_tag = item.get("href", "")

            nombre = nombre_tag.get_text(strip=True) if nombre_tag else ""
            ubicacion = ubicacion_tag.get_text(strip=True) if ubicacion_tag else ""
            descripcion = descripcion_tag.get_text(strip=True) if descripcion_tag else ""

            data.append({
                "Nombre": nombre,
                "Ubicación": ubicacion,
                "Descripción": descripcion,
                "Link": link_tag
            })

        print(f"Página {page} procesada correctamente.")
        time.sleep(1)  # esperar un poco entre requests para no sobrecargar el servidor

    except Exception as e:
        print(f"Error al procesar página {page}: {e}")
        # puedes agregar un retry aquí si quieres

# Guardar en Excel o CSV
df = pd.DataFrame(data)
df.to_csv("inventario_huila.csv", index=False, encoding="utf-8")
print("Datos extraídos y guardados.")
