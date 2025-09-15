import requests
import json
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# Tu clave de API de ScraperAPI
SCRAPERAPI_KEY = 'f2c0229f1f03a93a3664e09ef65c2315'  # Reemplázala con tu propia clave

# Configuración de Selenium para obtener cookies
chrome_options = Options()
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

# Configuración del WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# URL de la página de Tolima que deseas scrapear
url = "https://tolima.gov.co/tolima/informacion-general/turismo"

# Abrir la página en Selenium para obtener cookies
driver.get(url)

# Esperar a que la página cargue completamente
time.sleep(5)

# Obtener las cookies de la sesión
cookies = driver.get_cookies()
cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

# Headers del navegador
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.7258.155 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "es-ES,es;q=0.9",
    "Origin": "https://tolima.gov.co",
    "Referer": "https://tolima.gov.co/tolima/informacion-general/turismo?start=20",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "cross-site",
    "sec-fetch-storage-access": "active",
    "Content-Type": "application/json"
}

# Usamos ScraperAPI para evitar bloqueos
scraper_api_url = f'http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={url}'

# Realizamos la solicitud a ScraperAPI
response = requests.get(scraper_api_url, headers=headers, cookies=cookies_dict)

# Verificar el código de estado de la respuesta
if response.status_code == 200:
    print("Solicitud exitosa")

    # Verificar si la respuesta es válida JSON
    try:
        data = response.json()  # Intentar convertir la respuesta a JSON
        # Guardar los datos en un archivo JSON
        with open("datos_turismo_tolima.json", "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=2)

        # Guardar los datos en un archivo CSV
        with open("datos_turismo_tolima.csv", "w", newline='', encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            # Escribir los encabezados (ajusta según la estructura de los datos)
            writer.writerow(["Municipio", "Nombre", "Categoría", "Descripción", "Contacto"])
            # Escribir los datos
            for item in data.get("results", []):  # Cambia la clave según el formato de los datos
                writer.writerow([item.get("municipio", ""), item.get("nombre", ""), item.get("categoria", ""), item.get("descripcion", ""), item.get("contacto", "")])
    
    except json.decoder.JSONDecodeError:
        print("La respuesta no es JSON válida. Mostrando el contenido de la respuesta:")
        print(response.text)  # Imprimir la respuesta para diagnosticar el problema
else:
    print(f"Error en la solicitud. Código de estado: {response.status_code}")

# Cerrar el navegador
driver.quit()
