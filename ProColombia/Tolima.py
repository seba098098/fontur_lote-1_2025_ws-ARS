from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime
import time
import re
import random

class ComprehensiveTolimaScraper:
    def __init__(self, headless=False):
        self.headless = headless
        self.driver = None
        self.all_results = []
        self.visited_urls = set()
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
            if url in self.visited_urls:
                return False
                
            print(f"🌐 Navegando a: {url}")
            self.driver.get(url)
            WebDriverWait(self.driver, 25).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            self.visited_urls.add(url)
            return True
        except Exception as e:
            print(f"❌ Error navegando a {url}: {e}")
            return False
    
    def accept_cookies(self):
        """Aceptar cookies"""
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

    def scrape_search_results(self, base_url, max_pages=20):
        """Scraping profundo de resultados de búsqueda"""
        print(f"🔍 Iniciando scraping de búsqueda: {base_url}")
        
        if not self.navigate_to_url(base_url):
            return
        
        self.accept_cookies()
        page_count = 0
        
        while page_count < max_pages:
            current_url = self.driver.current_url
            print(f"\n📄 Procesando página {page_count + 1}: {current_url}")
            
            # Extraer datos de la página actual
            page_results = self.extract_search_page_data()
            self.all_results.extend(page_results)
            
            print(f"✅ Encontrados {len(page_results)} resultados en esta página")
            print(f"📊 Total acumulado: {len(self.all_results)} resultados")
            
            # Intentar navegar a la siguiente página
            if not self.go_to_next_search_page():
                print("⏹️ No hay más páginas de búsqueda")
                break
            
            page_count += 1
            time.sleep(random.uniform(2, 4))

    def explore_tolima_destinations(self):
        """Explorar destinos específicos de Tolima"""
        print("\n🏔️ Explorando destinos específicos de Tolima...")
        
        destinations_to_explore = [
            "https://colombia.travel/es/destinos/tolima",
            "https://colombia.travel/es/ibague",
            "https://colombia.travel/es/melgar",
            "https://colombia.travel/es/honda",
            "https://colombia.travel/es/mariquita",
            "https://colombia.travel/es/ambalema",
            "https://colombia.travel/es/fresno",
            "https://colombia.travel/es/libano",
            "https://colombia.travel/es/espinal"
        ]
        
        for destination_url in destinations_to_explore:
            if self.navigate_to_url(destination_url):
                print(f"📍 Explorando destino: {destination_url}")
                self.accept_cookies()
                
                # Extraer información del destino
                destination_data = self.extract_destination_data()
                if destination_data:
                    self.all_results.append(destination_data)
                
                # Buscar enlaces relacionados dentro del destino
                self.explore_related_links()
                
                time.sleep(random.uniform(2, 3))

    def explore_tourism_categories(self):
        """Explorar categorías turísticas relacionadas"""
        print("\n🎯 Explorando categorías turísticas...")
        
        categories = {
            "hoteles": "https://colombia.travel/es/alojamiento?destination=tolima",
            "restaurantes": "https://colombia.travel/es/gastronomia?destination=tolima",
            "aventura": "https://colombia.travel/es/aventura?destination=tolima",
            "naturaleza": "https://colombia.travel/es/naturaleza?destination=tolima",
            "cultura": "https://colombia.travel/es/cultura?destination=tolima",
            "eventos": "https://colombia.travel/es/eventos?destination=tolima"
        }
        
        for category_name, category_url in categories.items():
            if self.navigate_to_url(category_url):
                print(f"🏷️ Explorando categoría: {category_name}")
                self.accept_cookies()
                
                # Extraer resultados de la categoría
                category_results = self.extract_category_data(category_name)
                self.all_results.extend(category_results)
                
                print(f"✅ Encontrados {len(category_results)} resultados en {category_name}")
                
                time.sleep(random.uniform(2, 3))

    def extract_search_page_data(self):
        """Extraer datos de la página de búsqueda actual"""
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Múltiples selectores para resultados
        selectors = [
            'div.search-result', 'div.result-item', 'div.views-row',
            'article.node', 'div.card', 'div.item-list div',
            'div[class*="result"]', 'div[class*="item"]',
            'li.search-result', 'li.result-item'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                print(f"🔎 Encontrados {len(elements)} elementos con {selector}")
                for element in elements:
                    result_data = self.extract_result_data(element)
                    if result_data and result_data['enlace'] not in [r['enlace'] for r in self.all_results + results]:
                        results.append(result_data)
                break
        
        return results

    def extract_destination_data(self):
        """Extraer datos de página de destino"""
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extraer información principal del destino
        title = self.safe_extract(soup, ['h1', '.page-title', '.title'])
        description = self.safe_extract(soup, ['.field--name-body', '.description', 'article p'])
        
        if not title:
            return None
        
        return {
            'titulo': title,
            'categoria': 'destino_principal',
            'descripcion': description[:500] if description else "Destino turístico en Tolima",
            'enlace': self.driver.current_url,
            'imagen': self.extract_main_image(soup),
            'ubicacion': self.extract_location_from_text(title),
            'tipo': 'destino',
            'fecha_extraccion': datetime.now().isoformat(),
            'detalles': self.extract_destination_details(soup)
        }

    def extract_category_data(self, category_name):
        """Extraer datos de categorías turísticas"""
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Buscar elementos de la categoría
        elements = soup.select('div.card, article.node, div.views-row, div.result-item')
        
        for element in elements:
            result_data = self.extract_result_data(element)
            if result_data:
                result_data['categoria'] = category_name
                if result_data['enlace'] not in [r['enlace'] for r in self.all_results + results]:
                    results.append(result_data)
        
        return results

    def extract_result_data(self, element):
        """Extraer datos de un resultado individual"""
        try:
            title = self.safe_extract(element, ['h2', 'h3', 'h4', '.title', '.card-title', 'a'])
            if not title:
                return None
            
            link = self.extract_link(element)
            if not link or link == "#":
                return None
            
            return {
                'titulo': title,
                'categoria': self.determine_category(element, title),
                'descripcion': self.safe_extract(element, ['p', '.description', '.card-text']),
                'enlace': link,
                'imagen': self.extract_image(element),
                'ubicacion': self.extract_location_from_text(title),
                'tipo': 'resultado_busqueda',
                'fecha_extraccion': datetime.now().isoformat(),
                'precio': self.extract_price(element),
                'telefono': self.extract_phone(element)
            }
            
        except Exception as e:
            print(f"Error extrayendo resultado: {e}")
            return None

    def safe_extract(self, element, selectors):
        """Extraer texto de forma segura"""
        for selector in selectors:
            try:
                elem = element.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 2:
                        return text
            except:
                continue
        return None

    def extract_link(self, element):
        """Extraer enlace"""
        try:
            link_elem = element.select_one('a[href]')
            if link_elem and link_elem.has_attr('href'):
                href = link_elem['href']
                if href.startswith('/'):
                    return f"https://colombia.travel{href}"
                elif href.startswith('http'):
                    return href
        except:
            pass
        return "#"

    def extract_image(self, element):
        """Extraer imagen"""
        try:
            img_elem = element.select_one('img[src]')
            if img_elem:
                src = img_elem['src']
                if src.startswith('/'):
                    return f"https://colombia.travel{src}"
                return src
        except:
            pass
        return ""

    def extract_main_image(self, soup):
        """Extraer imagen principal"""
        try:
            img = soup.select_one('meta[property="og:image"]')
            if img and img.has_attr('content'):
                return img['content']
        except:
            pass
        return ""

    def determine_category(self, element, title):
        """Determinar categoría basado en contenido"""
        text = (element.get_text() + " " + title).lower()
        
        categories = {
            'hotel': ['hotel', 'alojamiento', 'hospedaje', 'posada'],
            'restaurante': ['restaurante', 'comida', 'gastronomía'],
            'evento': ['evento', 'festival', 'carnaval', 'feria'],
            'naturaleza': ['parque', 'reserva', 'natural', 'ecoturismo'],
            'aventura': ['aventura', 'deporte', 'rafting', 'caminata'],
            'cultural': ['museo', 'iglesia', 'cultural', 'historia'],
            'tour': ['tour', 'guía', 'excursión', 'paquete']
        }
        
        for cat, keywords in categories.items():
            if any(keyword in text for keyword in keywords):
                return cat
        
        return 'atraccion_turistica'

    def extract_location_from_text(self, text):
        """Extraer ubicación del texto"""
        tolima_locations = [
            'ibagué', 'melgar', 'honda', 'mariquita', 'ambalema',
            'fresno', 'libano', 'espinal', 'lérida', 'cajamarca',
            'venadillo', 'guamo', 'murillo', 'falan', 'herveo'
        ]
        
        text_lower = text.lower()
        for location in tolima_locations:
            if location in text_lower:
                return location.capitalize()
        
        return 'Tolima'

    def extract_price(self, element):
        """Extraer información de precio"""
        text = element.get_text()
        prices = re.findall(r'\$\s*\d+(?:\.\d+)?|\d+\s*(?:USD|COP|pesos)', text, re.IGNORECASE)
        return prices[0] if prices else "Consultar"

    def extract_phone(self, element):
        """Extraer teléfono"""
        text = element.get_text()
        phones = re.findall(r'(\+?\d{1,3}[\s-]?)?\(?\d{3}\)?[\s-]?\d{3}[\s-]?\d{4}', text)
        return phones[0] if phones else ""

    def extract_destination_details(self, soup):
        """Extraer detalles específicos de destino"""
        details = {}
        
        # Extraer características
        features = soup.select('.field--name-field-features .field__item, .characteristics li')
        if features:
            details['caracteristicas'] = [f.get_text(strip=True) for f in features[:5]]
        
        # Extraer actividades
        activities = soup.select('.field--name-field-activities .field__item, .activities li')
        if activities:
            details['actividades'] = [a.get_text(strip=True) for a in activities[:5]]
        
        return details

    def go_to_next_search_page(self):
        """Navegar a la siguiente página de búsqueda"""
        try:
            current_url = self.driver.current_url
            current_page = self.extract_page_number(current_url)
            next_page = current_page + 1
            
            # Construir URL de siguiente página
            if 'page=' in current_url:
                next_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
            else:
                separator = '&' if '?' in current_url else '?'
                next_url = f"{current_url}{separator}page={next_page}"
            
            return self.navigate_to_url(next_url)
            
        except Exception as e:
            print(f"Error yendo a página siguiente: {e}")
            return False

    def extract_page_number(self, url):
        """Extraer número de página de la URL"""
        match = re.search(r'page=(\d+)', url)
        return int(match.group(1)) if match else 0

    def explore_related_links(self):
        """Explorar enlaces relacionados dentro de una página"""
        try:
            # Buscar enlaces a otras páginas relevantes
            related_links = self.driver.find_elements(By.XPATH, 
                "//a[contains(@href, 'tolima') or contains(@href, 'ibague') or contains(@href, 'melgar') or contains(text(), 'Ver más') or contains(text(), 'Descubrir')]"
            )
            
            for link in related_links[:5]:  # Limitar a 5 enlaces para no saturar
                try:
                    href = link.get_attribute('href')
                    if href and 'colombia.travel' in href and href not in self.visited_urls:
                        print(f"🔗 Explorando enlace relacionado: {href}")
                        if self.navigate_to_url(href):
                            related_data = self.extract_destination_data()
                            if related_data:
                                self.all_results.append(related_data)
                            time.sleep(2)
                except:
                    continue
                    
        except Exception as e:
            print(f"Error explorando enlaces relacionados: {e}")

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
            
            # Generar reporte
            self.generate_report(unique_results)
            
        else:
            print("❌ No se encontraron resultados para guardar")

    def generate_report(self, results):
        """Generar reporte detallado"""
        print(f"\n📊 REPORTE COMPLETO DE TURISMO EN TOLIMA")
        print("=" * 60)
        
        # Estadísticas por categoría
        categories = {}
        for result in results:
            cat = result['categoria']
            categories[cat] = categories.get(cat, 0) + 1
        
        print("\n📈 DISTRIBUCIÓN POR CATEGORÍAS:")
        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"   {cat}: {count} resultados")
        
        # Estadísticas por ubicación
        locations = {}
        for result in results:
            loc = result['ubicacion']
            locations[loc] = locations.get(loc, 0) + 1
        
        print("\n🗺️ DISTRIBUCIÓN POR UBICACIÓN:")
        for loc, count in sorted(locations.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   {loc}: {count} resultados")
        
        print(f"\n⭐ TOTAL DE RESULTADOS ÚNICOS: {len(results)}")
        print("=" * 60)

    def run_complete_scraping(self):
        """Ejecutar scraping completo"""
        print("🚀 INICIANDO SCRAPING COMPLETO DE TOLIMA")
        print("⏰ Esto puede tomar 10-15 minutos...\n")
        
        start_time = time.time()
        
        # 1. Scraping de búsqueda principal
        self.scrape_search_results("https://colombia.travel/es/buscador?keys=tolima", max_pages=15)
        
        # 2. Explorar destinos específicos
        self.explore_tolima_destinations()
        
        # 3. Explorar categorías turísticas
        self.explore_tourism_categories()
        
        # 4. Guardar resultados
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
    scraper = ComprehensiveTolimaScraper(headless=False)
    
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