# -*- coding: utf-8 -*-
"""
FonturDeptScraper v4.4 (Putumayo por defecto, filtro fecha >= 2019, salida formateada)
- Busca resultados en Fontur por palabra clave/URL (por defecto: Putumayo con tu URL)
- Recorre paginación con ?page=N
- Extrae campos del listado y enriquece desde la ficha.
- Filtra resultados por fecha de publicación/actualización (>= min-year; por defecto 2019)
- Exporta SOLO estas 11 claves en este orden:
  titulo, categoria, descripcion, enlace, imagen, ubicacion, tipo,
  fecha_extraccion, precio, telefono, detalles
"""

import re
import csv
import json
import time
import random
import argparse
from datetime import datetime
from urllib.parse import urlparse, urljoin, urlencode, parse_qs

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -------------------------
# Utilidades
# -------------------------
PHONE_RE = re.compile(r'(?:\+57\s?)?(?:\(?\d{1,3}\)?[\s\-.]?)?\d{3}[\s\-.]?\d{2,}|\+?\d[\d\s\-.]{6,}', re.UNICODE)

SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

def clean_text(s):
    if not s:
        return None
    return re.sub(r'\s+', ' ', s).strip()

def absolutize(base, href):
    if not href:
        return None
    return urljoin(base, href)

def domain_of(url):
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return None

def pick_category(container_text):
    if not container_text:
        return None
    hints = [
        ("aventura", "aventura"),
        ("gastronom", "gastronomía"),
        ("cultura", "cultura"),
        ("bienestar", "bienestar"),
        ("natur", "naturaleza"),
        ("evento", "evento"),
        ("alojamiento", "alojamiento"),
        ("transporte", "transporte"),
        ("turismo", "turismo"),
        ("promoci", "promoción"),
        ("convocatoria", "convocatoria"),
    ]
    t = (container_text or "").lower()
    for key, label in hints:
        if key in t:
            return label
    return None

def jitter(a=0.6, b=1.2):
    time.sleep(random.uniform(a, b))

# ---- Parseo de fechas (formatos comunes en sitios Drupal)
def parse_iso_like(s):
    try:
        s = s.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return datetime.fromisoformat(s + "T00:00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def parse_spanish_textual(s):
    # "12 de julio de 2021"
    m = re.search(r'(\d{1,2})\s+de\s+([A-Za-záéíóúñÁÉÍÓÚÑ]+)\s+de\s+(\d{4})', s, flags=re.IGNORECASE)
    if m:
        d = int(m.group(1))
        month_name = (m.group(2).lower()
                      .replace("á","a").replace("é","e")
                      .replace("í","i").replace("ó","o").replace("ú","u"))
        month = SPANISH_MONTHS.get(month_name)
        y = int(m.group(3))
        if month:
            try:
                return datetime(y, month, d)
            except Exception:
                return None
    return None

def parse_ddmmyyyy(s):
    m = re.search(r'\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b', s)
    if m:
        d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mth, d)
        except Exception:
            return None
    return None

def parse_yyyymmdd(s):
    m = re.search(r'\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b', s)
    if m:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mth, d)
        except Exception:
            return None
    return None

def extract_pub_date(soup):
    """
    Intenta obtener fecha de publicación/actualización:
    - meta[property='article:published_time'] / og:updated_time / dcterms
    - <time datetime="..."> o <time>12 de julio de 2021</time>
    - Texto libre: '12 de julio de 2021', '12/07/2021', '2021-07-12'
    """
    for sel in [
        "meta[property='article:published_time']",
        "meta[property='og:published_time']",
        "meta[name='article:published_time']",
        "meta[name='dcterms.date']",
        "meta[name='DC.date.issued']",
        "meta[property='og:updated_time']",
    ]:
        el = soup.select_one(sel)
        if el and el.get("content"):
            dt = parse_iso_like(el.get("content"))
            if dt:
                return dt

    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        dt = parse_iso_like(t.get("datetime"))
        if dt:
            return dt
    t2 = soup.select_one("time")
    if t2 and t2.get_text():
        dt = parse_spanish_textual(t2.get_text())
        if dt:
            return dt

    main = soup.select_one(".node__content, article, main, .region-content, .layout-content, body")
    text = main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True)

    for parser in (parse_spanish_textual, parse_ddmmyyyy, parse_yyyymmdd):
        dt = parser(text)
        if dt:
            return dt
    return None

# Claves objetivo y formateo
TARGET_FIELDS = [
    "titulo","categoria","descripcion","enlace","imagen","ubicacion","tipo",
    "fecha_extraccion","precio","telefono","detalles"
]

def format_output(raw, defaults):
    out = {}
    out["titulo"] = raw.get("titulo") or ""
    out["categoria"] = (raw.get("categoria") or "").strip() or None
    desc = raw.get("descripcion")
    out["descripcion"] = desc if (desc is None or isinstance(desc, str)) else clean_text(str(desc))
    out["enlace"] = raw.get("enlace") or ""
    out["imagen"] = raw.get("imagen") or None
    out["ubicacion"] = raw.get("ubicacion") or defaults.get("departamento") or ""
    out["tipo"] = raw.get("tipo") or "resultado_busqueda"
    out["fecha_extraccion"] = raw.get("fecha_extraccion") or datetime.utcnow().isoformat()
    price = raw.get("precio")
    out["precio"] = price if (price and str(price).strip()) else "Consultar"
    phone = raw.get("telefono")
    out["telefono"] = phone.strip() if isinstance(phone, str) else ""
    details = raw.get("detalles")
    out["detalles"] = details.strip() if isinstance(details, str) else ""
    return out

# -------------------------
# Scraper
# -------------------------
class FonturDeptScraper:
    def __init__(self, base_search, departamento="Putumayo", headless=True, max_pages=20, wait_sec=12,
                 min_year=2019, keep_undated=False):
        self.departamento = departamento
        self.base_domain = "https://www.fontur.com.co"
        self.base_search = self.normalize_search_url(base_search)
        self.max_pages = max_pages
        self.wait_sec = wait_sec
        self.min_year = int(min_year)
        self.keep_undated = bool(keep_undated)
        self.results = []
        self.visited = set()
        self._setup_driver(headless=headless)

    # ---- Navegador
    def _setup_driver(self, headless=True):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=es-ES")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                                    "Chrome/124.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(options=chrome_options)

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass

    # ---- URLs
    def normalize_search_url(self, base_search):
        if base_search.lower().startswith("http"):
            return base_search
        return f"{self.base_domain}/es/search/node?{urlencode({'keys': base_search})}"

    def page_url(self, page_idx):
        parsed = urlparse(self.base_search)
        q = parse_qs(parsed.query)
        q["page"] = [str(page_idx)]
        query = []
        for k, vals in q.items():
            for v in vals:
                query.append((k, v))
        new_q = urlencode(query)
        return parsed._replace(query=new_q).geturl()

    # ---- Helpers de carga
    def wait_for_page(self):
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )

    def try_accept_cookies(self):
        try:
            btn = self.driver.find_elements(By.ID, "onetrust-accept-btn-handler")
            if btn:
                btn[0].click()
                jitter(0.2, 0.5)
                return
            for xp in [
                "//button[contains(., 'Aceptar')]",
                "//button[contains(., 'ACEPTAR')]",
                "//button[contains(., 'Accept')]",
                "//a[contains(., 'Aceptar')]",
            ]:
                els = self.driver.find_elements(By.XPATH, xp)
                if els:
                    els[0].click()
                    jitter(0.2, 0.5)
                    return
        except Exception:
            pass

    # ---- Scrape principal (INDENTACIÓN CORREGIDA)
    def scrape(self):
        for page in range(self.max_pages):
            url = self.page_url(page)
            self.driver.get(url)
            self.wait_for_page()
            self.try_accept_cookies()
            jitter(0.8, 1.4)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            links = self._find_result_links(soup)
            if not links:
                if page == 0:
                    print("[INFO] No se hallaron resultados en la búsqueda (selectores vacíos).")
                break

            new_count = 0
            for link in links:
                item = self._parse_link_item(link, list_url=url)
                if not item or not item.get("enlace"):
                    continue
                if item["enlace"] in self.visited:
                    continue

                # Enriquecer y extraer fecha
                self._enrich_detail(item)
                pub_dt = item.get("_pub_dt")

                # Filtro por fecha
                if pub_dt is None and not self.keep_undated:
                    continue
                if pub_dt is not None and pub_dt.year < self.min_year:
                    continue

                item_fmt = format_output(item, defaults={"departamento": self.departamento})
                self.results.append(item_fmt)
                self.visited.add(item["enlace"])
                new_count += 1

                jitter(0.3, 0.8)

            if new_count == 0:
                break

        return self.results

    def _find_result_links(self, soup):
        selectors = [
            "main h3 a[href]",
            "#block-system-main h3 a[href]",
            ".search-results h3 a[href]",
            "h3 a[href]"
        ]
        for sel in selectors:
            found = soup.select(sel)
            if found:
                return found
        return []

    def _parse_link_item(self, a_tag, list_url):
        now_iso = datetime.utcnow().isoformat()
        titulo = clean_text(a_tag.get_text())
        enlace = absolutize(self.base_domain, a_tag.get("href"))

        descripcion = None
        cont = a_tag.find_parent()
        if cont:
            cont_txt = clean_text(cont.get_text(" "))
            if cont_txt and titulo and len(cont_txt) > len(titulo) + 10:
                descripcion = clean_text(cont_txt.replace(titulo, "", 1))[:300]

        item = {
            "titulo": titulo or "",
            "categoria": pick_category(descripcion or titulo or ""),
            "descripcion": descripcion,  # puede quedar como None
            "enlace": enlace or "",
            "imagen": None,
            "ubicacion": self.departamento,
            "tipo": "resultado_busqueda",
            "fecha_extraccion": now_iso,
            "precio": None,
            "telefono": None,
            "detalles": None,
            "_list_url": list_url,
        }
        return item

    def _enrich_detail(self, item):
        if not item.get("enlace"):
            return
        try:
            self.driver.get(item["enlace"])
            self.wait_for_page()
            jitter(0.6, 1.2)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            body_el = soup.select_one(".node__content, .region-content, article, main, .layout-content")
            body_txt = clean_text(body_el.get_text(" ")) if body_el else None

            if not item.get("imagen"):
                hero = soup.select_one(".field--name-field-image img, .media img, figure img, img")
                if hero and hero.get("src"):
                    item["imagen"] = absolutize(domain_of(item["enlace"]) or "https://www.fontur.com.co", hero.get("src"))

            tel = None
            scopes = [el.get_text(" ") for el in soup.select(
                ".field--name-field-telefono, .field--name-field-contacto, .contact, .field, .node__content, .region-content"
            )]
            if not scopes:
                scopes = [body_txt or ""]

            for scope_text in scopes:
                m = PHONE_RE.search(scope_text)
                if m:
                    tel = clean_text(m.group(0))
                    break

            precio = None
            for scope_text in scopes:
                if re.search(r'(?i)\bprecio[s]?\b', scope_text):
                    sent = re.search(r'([^.]*\bprecio[^.]*\.)', scope_text, flags=re.I)
                    if sent:
                        precio = clean_text(sent.group(1))
                        break

            if body_txt and (not item.get("descripcion") or len(item["descripcion"]) < 60):
                item["descripcion"] = (body_txt[:500] + "…") if len(body_txt) > 500 else body_txt

            # Fecha de publicación/actualización
            pub_dt = extract_pub_date(soup)
            item["_pub_dt"] = pub_dt

            item["telefono"] = tel
            item["precio"] = precio
            item["detalles"] = body_txt
            item["tipo"] = "ficha"

        except Exception:
            item["_pub_dt"] = None

# -------------------------
# Persistencia
# -------------------------
def save_json_array(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def save_csv(path, rows):
    if not rows:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TARGET_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in TARGET_FIELDS})

# -------------------------
# CLI
# -------------------------
def main():
    ap = argparse.ArgumentParser(description="Scraper de Fontur por departamento (Putumayo por defecto, filtro fecha >= 2019, formato fijo).")
    ap.add_argument("--url", default="https://www.fontur.com.co/es/search/node?keys=putumayo&page=0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0",
                    help="URL base de búsqueda (con keys=...) o solo la clave (ej. 'putumayo').")
    ap.add_argument("--departamento", default="Putumayo", help="Valor que irá en 'ubicacion'.")
    ap.add_argument("--headless", action="store_true", help="Ejecutar Chrome en modo headless.")
    ap.add_argument("--pages", type=int, default=20, help="Máximo de páginas (?page=N).")
    ap.add_argument("--out", default="fontur_putumayo_2019plus", help="Prefijo de salida (sin extensión).")
    ap.add_argument("--min-year", type=int, default=2019, help="Año mínimo de publicación/actualización (inclusive).")
    ap.add_argument("--keep-undated", action="store_true",
                    help="Si se especifica, conserva fichas sin fecha detectable (por defecto se descartan).")
    args = ap.parse_args()

    scraper = FonturDeptScraper(
        base_search=args.url,
        departamento=args.departamento,
        headless=args.headless,
        max_pages=args.pages,
        wait_sec=12,
        min_year=args.min_year,
        keep_undated=args.keep_undated
    )

    try:
        rows = scraper.scrape()

        # Deduplicar por enlace
        seen = set()
        normalized = []
        for r in rows:
            link = r.get("enlace")
            if link in seen:
                continue
            seen.add(link)
            normalized.append({k: r.get(k) for k in TARGET_FIELDS})

        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        json_path = f"{args.out}_{ts}.json"
        csv_path = f"{args.out}_{ts}.csv"

        save_json_array(json_path, normalized)
        save_csv(csv_path, normalized)

        print(f"[OK] Registros (>= {args.min_year}{' + sin fecha' if args.keep_undated else ''}): {len(normalized)}")
        print(f"[OK] JSON: {json_path}")
        print(f"[OK] CSV: {csv_path}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
