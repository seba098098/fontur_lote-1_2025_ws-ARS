import json
import csv
import os
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class DetailScraper:
    def __init__(self, input_file="turismo_huila_completo_20250901_164631.json"):
        with open(input_file, "r", encoding="utf-8") as f:
            self.data = json.load(f)

        self.results = []
        self.driver = webdriver.Chrome()

    def scrape_detail(self, entry):
        url = entry.get("enlace")
        if not url:
            print(f"⚠️ No hay enlace para {entry.get('titulo')}")
            entry["error"] = "Sin enlace"
            return entry

        try:
            print(f"🔎 Visitando: {url}")
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(random.uniform(2, 3))  # Pausa para cargar todo

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Capturar texto de varios posibles lugares
            descripcion_raw = soup.select("div.field-name-body p, article p, .content p")
            descripcion = " ".join([p.get_text(strip=True) for p in descripcion_raw]) if descripcion_raw else entry.get("descripcion")

            # Extra: Dirección, horarios, contacto
            direccion = soup.find(string=lambda t: "dirección" in t.lower()) if soup else None
            horario = soup.find(string=lambda t: "horario" in t.lower()) if soup else None
            correo = soup.find("a", href=lambda h: h and "mailto:" in h)
            telefono = soup.find("a", href=lambda h: h and "tel:" in h)

            # Redes sociales
            redes = [a["href"] for a in soup.find_all("a", href=True) if any(r in a["href"] for r in ["facebook", "instagram", "twitter", "youtube"])]

            # Precios (si aparecen en divs o spans)
            precios = soup.select("span.precio, div.price, .field-price")
            precio = ", ".join([p.get_text(strip=True) for p in precios]) if precios else entry.get("precio")

            # Secciones de detalle
            secciones = soup.select("section, div.detalles, div.info")
            detalles_texto = " ".join([s.get_text(strip=True) for s in secciones]) if secciones else entry.get("detalles")

            # Unir todo en un campo más largo
            descripcion_extendida = " ".join(filter(None, [
                descripcion,
                f"Dirección: {direccion}" if direccion else "",
                f"Horario: {horario}" if horario else "",
                f"Correo: {correo.get_text(strip=True)}" if correo else "",
                f"Teléfono: {telefono.get_text(strip=True)}" if telefono else "",
                f"Redes sociales: {', '.join(redes)}" if redes else "",
                f"Detalles adicionales: {detalles_texto}" if detalles_texto else ""
            ]))

            entry.update({
                "descripcion": descripcion_extendida,
                "telefono": telefono.get_text(strip=True) if telefono else entry.get("telefono"),
                "correo": correo.get_text(strip=True) if correo else "",
                "direccion": direccion if direccion else "",
                "horario": horario if horario else "",
                "redes": redes,
                "precio": precio,
                "detalles": detalles_texto,
                "fecha_detalle": datetime.now().isoformat()
            })

            return entry

        except Exception as e:
            print(f"❌ Error al scrapear {url}: {e}")
            entry["error"] = str(e)
            return entry

    def run(self):
        for entry in self.data:
            enriched_entry = self.scrape_detail(entry)
            if enriched_entry:
                self.results.append(enriched_entry)

        self.driver.quit()

    def save_results(self, output_json="turismo_huila_completo_detalles.json", output_csv="turismo_huila_completo_detalles.csv"):
        # Guardar JSON
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        # Guardar CSV
        if self.results:
            keys = sorted(set().union(*(d.keys() for d in self.results)))
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(self.results)

        print(f"✅ Resultados guardados en {output_json} y {output_csv}")


if __name__ == "__main__":
    scraper = DetailScraper(input_file="turismo_huila_completo_20250901_164631.json")
    scraper.run()
    scraper.save_results()
