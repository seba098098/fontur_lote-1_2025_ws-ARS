from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
from collections import deque
import unicodedata
import json, csv, re, time, random
from datetime import datetime

def slugify_text(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

def canon_url(u: str) -> str:
    try:
        p = urlparse(u)
        # quitar fragmentos y normalizar query (ordenar)
        q = parse_qs(p.query, keep_blank_values=True)
        q_sorted = urlencode(sorted((k, v if isinstance(v, str) else v[0]) for k, v in q.items()))
        return urlunparse((p.scheme, p.netloc, p.path.rstrip('/'), '', q_sorted, ''))
    except:
        return u

class FonturDeptScraper:
    def __init__(self, departamento: str, headless=True,
                 max_pages=20, deep_crawl=True, max_depth=2, max_urls=800):
        self.departamento = departamento
        self.depto_norm = slugify_text(departamento)
        self.headless = headless
        self.max_pages = max_pages
        self.deep_crawl = deep_crawl
        self.max_depth = max_depth
        self.max_urls = max_urls

        self.base = "https://www.fontur.com.co"
        self.visited = set()
        self.results = []
        self.driver = None

        self.sections = [
            "/es/proyectos", "/es/convocatorias", "/es/noticias",
            "/es/destinos", "/es/programas"
        ]

        # Variantes por departamento (tildes, sin tildes y siglas/atractivos típicos)
        self.keyword_variants, self.municipios = self._variants_for_depto(departamento)

        self._setup_driver()

    def _setup_driver(self):
        o = Options()
        if self.headless: o.add_argument("--headless=new")
        o.add_argument("--no-sandbox"); o.add_argument("--disable-dev-shm-usage")
        o.add_argument("--disable-gpu"); o.add_argument("--window-size=1920,1080")
        o.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        o.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.driver = webdriver.Chrome(options=o)

    # ---------- Config ----------
    def _variants_for_depto(self, d):
        d_norm = slugify_text(d)
        variants = {d, d_norm}
        # extras por cada depto
        muni = []
        if d_norm == "tolima":
            muni = ["Ibagué","Ibague","Melgar","Honda","Mariquita","Ambalema","Líbano","Libano",
                    "Espinal","Lérida","Lerida","Murillo","Prado"]
        elif d_norm == "huila":
            muni = ["Neiva","Pitalito","San Agustín","San Agustin","Garzón","Garzon","La Plata",
                    "Villavieja","Desierto de la Tatacoa","Gigante","Campoalegre","Isnos","Paicol"]
        elif d_norm == "putumayo":
            muni = ["Mocoa","Puerto Asís","Puerto Asis","Sibundoy","Valle de Sibundoy","Orito",
                    "Villagarzón","Villagarzon","Leguízamo","Leguizamo","La Hormiga","Fin del Mundo","La Paya"]
        elif d_norm == "caqueta" or d_norm == "caquetá":
            variants |= {"caqueta","caquetá"}
            muni = ["Florencia","San Vicente del Caguán","San Vicente del Caguan","Cartagena del Chairá",
                    "Cartagena del Chaira","Belén de los Andaquíes","Belen de los Andaquies","La Montañita",
                    "Curillo","Morelia","Milán","Milan","Solano","Valparaíso","Valparaiso","Chiribiquete",
                    "Cueva de los Guácharos","Río Orteguaza","Rio Orteguaza"]
        # incluir variantes normalizadas de municipios
        muni_norm = list({m for m in muni} | {slugify_text(m) for m in muni})
        return list(variants), muni_norm

    # ---------- Utilidades ----------
    def _open(self, url, wait_css="body", wait_sec=25):
        try:
            cu = canon_url(url)
            if cu in self.visited: return False
            self.driver.get(url)
            WebDriverWait(self.driver, wait_sec).until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_css)))
            time.sleep(random.uniform(1.8, 3.2))
            self.visited.add(cu)
            return True
        except Exception as e:
            print(f"❌ Navegación fallida: {url} -> {e}")
            return False

    def _accept_cookies(self):
        xpaths = [
            "//button[contains(., 'Aceptar')]", "//button[contains(., 'Aceptar todas')]",
            "//a[contains(., 'Aceptar')]", "//div[contains(@class, 'cookie')]//button"
        ]
        for xp in xpaths:
            try:
                btn = WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, xp)))
                if btn.is_displayed():
                    btn.click(); time.sleep(1.2); return
            except: pass

    def _scroll_to_bottom(self):
        try:
            last_h = 0
            for _ in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.2)
                h = self.driver.execute_script("return document.body.scrollHeight;")
                if h == last_h: break
                last_h = h
        except: pass

    def _soup(self):
        return BeautifulSoup(self.driver.page_source, 'html.parser')

    def _textnorm(self, s):
        return slugify_text(s or "")

    def _match_interest(self, text):
        t = self._textnorm(text)
        if any(k in t for k in [self._textnorm(v) for v in self.keyword_variants]): return True
        if any(m in t for m in [self._textnorm(v) for v in self.municipios]): return True
        return False

    # ---------- Extracción ----------
    def _extract_items_from_listing(self, soup):
        containers = []
        selectors = [
            'div.search-result', 'div.view-content div.views-row',
            'article.node', 'li.search-result',
            '.search-results li', '.view-search-results .views-row',
            'div.views-row', 'article'
        ]
        for sel in selectors:
            found = soup.select(sel)
            if found: containers.extend(found)
        # fallback: tarjetas con <a> y titular
        if not containers:
            for a in soup.select('a[href]'):
                par = a.find_parent(['div','li','article','section'])
                if par: containers.append(par)
        # dedup por objeto
        uniq = []
        seen_ids = set()
        for c in containers:
            key = id(c)
            if key not in seen_ids:
                uniq.append(c); seen_ids.add(key)
        return uniq

    def _first(self, el, selectors):
        for sel in selectors:
            try:
                hit = el.select_one(sel)
                if hit: return hit
            except: pass
        return None

    def _extract_card(self, el):
        title_el = self._first(el, ['h1','h2','h3','h4','.title','a[rel*="bookmark"]','a'])
        title = (title_el.get_text(strip=True) if title_el else el.get_text(strip=True)[:120])
        link_el = self._first(el, ['a[href]'])
        href = link_el.get('href') if link_el else None
        if not href: return None
        href = urljoin(self.base, href)
        img_el = self._first(el, ['img','picture img'])
        img = urljoin(self.base, img_el.get('src')) if img_el and img_el.get('src') else ""
        desc_el = self._first(el, ['.summary','p','.teaser','.field-body'])
        desc = (desc_el.get_text(" ", strip=True) if desc_el else "")
        cat = self._guess_category(el.get_text(" ", strip=True) + " " + title)
        ubic = self._guess_location(title + " " + desc)
        return {
            'titulo': title[:300],
            'categoria': cat,
            'descripcion': desc[:300] if desc else "Información turística disponible en Fontur",
            'enlace': canon_url(href),
            'imagen': img,
            'ubicacion': ubic,
            'tipo': 'resultado_busqueda',
            'fecha_extraccion': datetime.now().isoformat(),
            'precio': 'Consultar',
            'telefono': '',
            'detalles': '',
            'fuente': 'Fontur'
        }

    def _guess_category(self, text):
        t = self._textnorm(text)
        cats = {
            'hotel': ['hotel','alojamiento','hospedaje','hostal','posada'],
            'restaurante': ['restaurante','gastronomia','comida','menú','menu'],
            'evento': ['evento','festival','feria','carnaval'],
            'naturaleza': ['parque','reserva','natural','cascada','ecoturismo','selva','amazonia','tatacoa'],
            'aventura': ['aventura','senderismo','caminata','rafting','kayak','canopy'],
            'cultural': ['museo','iglesia','cultural','patrimonio','arqueologico','indigena'],
            'tour': ['tour','guia','excursion','recorrido','paquete'],
            'proyecto': ['proyecto','programa','plan','estrategia','convocatoria','financiacion','subsidio'],
            'noticia': ['noticia','actualidad','novedad','comunicado']
        }
        for k, kws in cats.items():
            if any(kw in t for kw in kws): return k
        return 'informacion_turistica'

    def _guess_location(self, text):
        if self._match_interest(text): return self.departamento
        return self.departamento  # default

    # ---------- Flujo principal ----------
    def search_and_collect(self, search_url):
        if not self._open(search_url): return
        self._accept_cookies(); self._scroll_to_bottom()
        page = 0; seen_any = False
        while page < self.max_pages:
            soup = self._soup()
            cards = self._extract_items_from_listing(soup)
            collected = 0
            for c in cards:
                data = self._extract_card(c)
                if not data: continue
                if canon_url(data['enlace']) in {r['enlace'] for r in self.results}: continue
                self.results.append(data); collected += 1
            print(f"📄 Página {page+1}: +{collected} / total {len(self.results)}")
            seen_any = seen_any or (collected > 0)

            # intentar botón siguiente
            if self._click_next(): 
                page += 1; time.sleep(random.uniform(1.2, 2.2)); continue

            # intentar paginador numerado
            if self._click_next_numbered():
                page += 1; time.sleep(random.uniform(1.2, 2.2)); continue

            # construir ?page=N si la URL soporta
            try:
                base_no_page = re.sub(r'([?&])page=\d+.*', r'\1', search_url)
                next_url = base_no_page + (('&' if '?' in base_no_page else '?') + f"page={page+1}")
                if self._open(next_url):
                    page += 1; continue
            except: pass
            break

        if not seen_any:
            print("⚠️ No se detectaron resultados (puede que Fontur cambie HTML en algunas vistas).")

    def _click_next(self):
        xps = [
            "//a[contains(., 'Siguiente')]", "//a[contains(@title, 'Siguiente')]",
            "//li[contains(@class,'pager-next')]//a", "//a[@rel='next']"
        ]
        for xp in xps:
            try:
                els = self.driver.find_elements(By.XPATH, xp)
                for el in els:
                    if el.is_displayed() and el.is_enabled():
                        self.driver.execute_script("arguments[0].click();", el)
                        return True
            except: pass
        return False

    def _click_next_numbered(self):
        try:
            pag = self._soup().select_one(".pager, .pagination, ul.pager")
            if not pag: return False
            active = pag.select_one(".is-active, li.active")
            if not active: return False
            nxt = active.find_next("a")
            if nxt and nxt.has_attr("href"):
                self._open(urljoin(self.base, nxt['href']))
                return True
        except: pass
        return False

    def _deep_crawl_domain(self):
        if not self.deep_crawl: return
        q = deque()
        seed_links = [r['enlace'] for r in self.results][:120]  # semillas de resultados
        for link in seed_links:
            q.append((link, 0))
        crawled = 0
        while q and crawled < self.max_urls:
            url, depth = q.popleft()
            if depth > self.max_depth: continue
            if not self._open(url): continue
            self._scroll_to_bottom()
            soup = self._soup()
            # recoger enlaces internos
            for a in soup.select('a[href]'):
                href = a.get('href'); 
                if not href: continue
                full = urljoin(self.base, href)
                if urlparse(full).netloc != urlparse(self.base).netloc: continue
                cu = canon_url(full)
                # si coincide con intereses, o es claramente contenido de Fontur (nodo, noticia, proyecto)
                if self._match_interest(a.get_text(" ", strip=True)) or re.search(r'/es/(node|noticias|proyectos|convocatorias|destinos)/', cu):
                    # extraer tarjeta si parece contenido
                    par = a.find_parent(['article','div','li','section'])
                    data = None
                    if par: data = self._extract_card(par)
                    if not data:
                        # como fallback, crear mínima si es detalle
                        title = a.get_text(strip=True) or cu.split('/')[-1].replace('-', ' ')
                        data = {
                            'titulo': title[:300],
                            'categoria': self._guess_category(title),
                            'descripcion': "Contenido interno de Fontur",
                            'enlace': cu,
                            'imagen': "",
                            'ubicacion': self.departamento,
                            'tipo': 'detalle',
                            'fecha_extraccion': datetime.now().isoformat(),
                            'precio': 'Consultar',
                            'telefono': '',
                            'detalles': '',
                            'fuente': 'Fontur'
                        }
                    if data and data['enlace'] not in {r['enlace'] for r in self.results}:
                        self.results.append(data)
                    # expandir cola
                    if cu not in self.visited:
                        q.append((cu, depth+1))
            crawled += 1

    def _explore_sections(self):
        for s in self.sections:
            self.search_and_collect(urljoin(self.base, s))

    def _municipality_queries(self):
        # lanza búsquedas por cada municipio/atractivo
        for m in self.municipios[:40]:
            key = m if " " not in m else m.replace(" ", "+")
            url = f"{self.base}/es/search/node?keys={key}"
            self.search_and_collect(url)

    # ---------- Salida ----------
    def _unique(self):
        uniq, seen = [], set()
        for r in self.results:
            cu = canon_url(r['enlace'])
            if cu not in seen:
                seen.add(cu); uniq.append(r)
        return uniq

    def save(self, tag=None):
        data = self._unique()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"fontur_{slugify_text(self.departamento)}{('_'+tag if tag else '')}_{ts}"
        fields = ['titulo','categoria','descripcion','enlace','imagen','ubicacion','tipo','fecha_extraccion','precio','telefono','detalles','fuente']
        with open(f"{name}.json","w",encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        with open(f"{name}.csv","w",encoding="utf-8",newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(data)
        print(f"✅ Guardado: {name}.json / {name}.csv  (Total únicos: {len(data)})")

    def report(self):
        data = self._unique()
        print("\n📊 REPORTE")
        print("="*60)
        by_cat = {}
        for r in data: by_cat[r['categoria']] = by_cat.get(r['categoria'],0)+1
        for k,v in sorted(by_cat.items(), key=lambda x: x[1], reverse=True):
            print(f" - {k}: {v}")
        print(f"TOTAL: {len(data)}")
        print("="*60)

    # ---------- Pipeline ----------
    def run(self):
        print(f"🚀 FonturScraper — Departamento: {self.departamento}")
        # 1) búsqueda principal (palabra clave + variantes)
        seed_queries = [
            f"{self.base}/es/search/node?keys={self.departamento}",
        ] + [f"{self.base}/es/search/node?keys={v}" for v in self.keyword_variants]
        for q in seed_queries:
            self.search_and_collect(q)

        # 2) secciones
        self._explore_sections()

        # 3) municipios / atractivos
        self._municipality_queries()

        # 4) deep crawl (opcional)
        self._deep_crawl_domain()

        # 5) salida
        self.report()
        self.save()

        self.driver.quit()
        print("👋 Navegador cerrado")

# ----------------- Ejecutar -----------------
if __name__ == "__main__":
    # Cambia aquí el departamento objetivo:
    scraper = FonturDeptScraper(departamento="Caquetá", headless=False,
                                max_pages=25, deep_crawl=True, max_depth=2, max_urls=1200)
    scraper.run()
