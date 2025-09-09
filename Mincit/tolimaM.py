from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import random
import csv
import json
from datetime import datetime

class TolimaScraper:
    def __init__(self, headless=False):
        self.headless = headless
        self.driver = None
        self.all_results = []
        self.setup_driver()
    
    def setup_driver(self):
        """Configurar el navegador Chrome"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        self.driver = webdriver.Chrome(options=chrome_options)
        print("Navegador Chrome configurado correctamente")
    
    def navigate_to_url(self, url):
        """Navegar a una URL con manejo de errores"""
        try:
            print(f"🌐 Navegando a: {url}")
            self.driver.get(url)
            time.sleep(3)
            return True
        except Exception as e:
            print(f"❌ Error navegando a {url}: {e}")
            return False
    
    def accept_cookies(self):
        """Aceptar cookies si aparecen"""
        try:
            cookie_buttons = self.driver.find_elements(By.XPATH, 
                "//button[contains(text(), 'Aceptar') or contains(text(), 'Aceptar todas')]"
            )
            for button in cookie_buttons:
                try:
                    if button.is_displayed():
                        button.click()
                        print("✅ Cookies aceptadas")
                        time.sleep(2)
                        return True
                except:
                    continue
            return False
        except:
            return False

    def extract_search_page_data(self):
        """Extraer datos de la página de búsqueda actual"""
        html = self.driver.page_source
        results = []

        # Buscar elementos relevantes que contengan 'Tolima'
        elements = self.driver.find_elements(By.XPATH, "//a[contains(text(), 'Tolima')]")
        
        for element in elements:
            title = element.text.strip()  # Título del enlace
            link = element.get_attribute("href")  # Enlace asociado con el título
            
            # Asegurarse de que estamos extrayendo enlaces válidos
            if not link or link == "#":
                continue

            # Extraer detalles de la página de destino del enlace
            result_data = {
                'titulo': title,
                'enlace': link,
                'fecha_extraccion': datetime.now().isoformat()
            }

            # Guardar la información
            results.append(result_data)

        print(f"📊 Total de resultados extraídos de esta página: {len(results)}")
        return results

    def scroll_and_scrape(self, base_url, max_scrolls=10):
        """Realiza scroll hacia abajo y extrae resultados"""
        print(f"🔍 Iniciando scroll en: {base_url}")
        self.driver.get(base_url)
        self.accept_cookies()
        time.sleep(3)
        
        # Realizar scroll y extraer datos
        scroll_count = 0
        while scroll_count < max_scrolls:
            print(f"\n📄 Realizando scroll: {scroll_count + 1}")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2, 4))  # Esperar un poco para cargar nuevos resultados

            # Extraer los datos de la página después de hacer scroll
            page_results = self.extract_search_page_data()
            self.all_results.extend(page_results)
            
            print(f"✅ Encontrados {len(page_results)} resultados en esta página")
            print(f"📊 Total acumulado: {len(self.all_results)} resultados")
            
            scroll_count += 1

    def save_results(self):
        """Guardar todos los resultados"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if self.all_results:
            # Eliminar duplicados
            unique_results = []
            seen_links = set()
            for result in self.all_results:
                if result['enlace'] not in seen_links:
                    unique_results.append(result)
                    seen_links.add(result['enlace'])
            
            print(f"\n💾 Guardando {len(unique_results)} resultados únicos...")
            
            # JSON
            json_filename = f"turismo_tolima_completo_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(unique_results, f, indent=2, ensure_ascii=False)
            print(f"✅ JSON guardado: {json_filename}")
            
            # CSV
            csv_filename = f"turismo_tolima_completo_{timestamp}.csv"
            with open(csv_filename, 'w', encoding='utf-8', newline='') as f:
                if unique_results:
                    writer = csv.DictWriter(f, fieldnames=unique_results[0].keys())
                    writer.writeheader()
                    writer.writerows(unique_results)
            print(f"✅ CSV guardado: {csv_filename}")
            
        else:
            print("❌ No se encontraron resultados para guardar")

    def run_complete_scraping(self):
        """Ejecutar scraping completo"""
        print("🚀 INICIANDO SCRAPING COMPLETO DE TOLIMA")
        print("⏰ Esto puede tomar unos minutos...\n")
        
        start_time = time.time()
        
        # 1. Lista de palabras clave
        keywords = [
            "Tolima", "Ibagué", "Melgar", "Honda", "Mariquita",
            "Cultura Tolima", "Turismo Tolima", "Hoteles en Tolima",
            "Restaurantes Tolima", "Actividades aventura Tolima",
            "Naturaleza Tolima", "Eventos en Tolima", "Salud y bienestar Tolima"
        ]
        
        # 2. Realizar búsqueda para cada palabra clave
        for keyword in keywords:
            print(f"\n🔍 Buscando: {keyword}")
            base_url = f"https://www.mincit.gov.co/resultados.aspx?searchtext={keyword}&searchmode=allwords"
            self.scroll_and_scrape(base_url, max_scrolls=10)
        
        # 3. Guardar resultados
        self.save_results()
        
        end_time = time.time()
        print(f"\n⏱️ Tiempo total de ejecución: {round((end_time - start_time)/60, 1)} minutos")
        print("🎉 Scraping completado exitosamente!")

    def close(self):
        """Cerrar el navegador"""
        if self.driver:
            self.driver.quit()
            print("👋 Navegador cerrado")

# Función principal
def main():
    scraper = TolimaScraper(headless=False)
    
    try:
        scraper.run_complete_scraping()
    except Exception as e:
        print(f"❌ Error durante el scraping: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
