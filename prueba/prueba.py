# -*- coding: utf-8 -*-
"""
Scraper genérico (Drupal/Views y similares) con doble vía:
1) requests + BeautifulSoup (rápido)
2) Selenium (fallback si el HTML viene por JS)

Uso:
    python prueba.py "https://tu-dominio/ruta-del-listado?keys=huila&page=0"
o sin argumento (te pedirá la URL por input).

Requisitos:
    pip install requests beautifulsoup4 lxml pandas
    pip install selenium webdriver-manager
"""

import csv
import random
import re
import sys
import time
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ====== Config común ======
OUTPUT_CSV = "resultados.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"
}
PAUSE_MIN, PAUSE_MAX = 5, 10
REQ_TIMEOUT = 40
MAX_PAGES = 200  # tope de seguridad por si el paginador es infinito o cíclico
# ==========================


def clean_text(t: str) -> str:
    if not t:
        return ""
    t = t.replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t, flags=re.S).strip()
    return t


def validate_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        raise ValueError("Debes proporcionar una URL.")
    p = urlparse(u)
    if not p.scheme.startswith("http"):
        raise ValueError("La URL debe comenzar por http:// o https://")
    # evita placeholders comunes
    if any(x in u for x in ["TU-SITIO", "example.com", "midominio"]):
        raise ValueError("Esa URL parece un placeholder. Pega la URL real del listado.")
    return u


# ----------- Paginador / parse helpers -----------
def extract_max_page_from_soup(soup: BeautifulSoup):
    """
    Busca en el paginador todos los enlaces que tengan ?page=N y devuelve el mayor N (0-based).
    Devuelve None si no logra detectarlo.
    """
    max_idx = None
    for a in soup.select("ul.pager a[href], .pager a[href], .pager__items a[href], nav[role='navigation'] a[href]"):
        href = a.get("href", "")
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            n = int(m.group(1))
            if (max_idx is None) or (n > max_idx):
                max_idx = n
    return max_idx


def _find_pager_link(soup: BeautifulSoup, texts):
    for a in soup.select(".pager__item--next a, .pager-next a, ul.pager a, nav[role='navigation'] a"):
        txt = a.get_text(strip=True).lower()
        if a.has_attr("href") and (txt in texts):
            return a
    return None


def parse_listing_page(html: str, base_url: str):
    """
    Devuelve: (rows, next_url, max_page)
    rows = lista de dicts con titulo, url, region, contenido, imagen
    next_url = URL del botón Siguiente (si existe)
    max_page = índice máximo detectado en el paginador (?page=N) o None
    """
    soup = BeautifulSoup(html, "lxml")
    container = (
        soup.select_one(".search-results")
        or soup.select_one(".view-content")
        or soup.select_one(".views-element-container")
        or soup
    )

    # ítems típicos de Drupal/Views
    items = container.select(".views-row, .row.mb-2.views-row, .search-result")
    if not items:
        items = container.select("div:has(.field-content)")

    rows = []
    for item in items:
        row = {}

        title_el = (
            item.select_one(".titulo")
            or item.select_one(".views-field-title a")
            or item.select_one("h3 a, h2 a")
            or item.select_one("h3, h2, .views-field-title, .field-title")
            or item.select_one("span.field-content > a")
        )
        row["titulo"] = clean_text(title_el.get_text()) if title_el else ""

        link_el = title_el if (title_el and title_el.name == "a") else item.select_one("a[href]")
        row["url"] = urljoin(base_url, link_el["href"]) if link_el else ""

        region_el = item.select_one(".region-geografica, .views-field-field-region, .region")
        row["region_geografica"] = clean_text(region_el.get_text()) if region_el else ""

        content_el = (
            item.select_one(".contenido")
            or item.select_one(".views-field-body, .views-field-field-descripcion, .field-content")
        )
        row["contenido"] = clean_text(content_el.get_text()) if content_el else ""

        img_el = item.select_one("img[src]")
        row["imagen"] = urljoin(base_url, img_el["src"]) if img_el else ""

        rows.append(row)

    # Next por DOM
    next_link = (
        soup.select_one('a[rel="next"]')
        or _find_pager_link(soup, {"siguiente", "next", "›", "»"})
        or soup.select_one('a[aria-label="Next"], a[aria-label="Siguiente"]')
    )
    next_url = urljoin(base_url, next_link["href"]) if next_link and next_link.has_attr("href") else None

    # Máxima página visible en el paginador
    max_page = extract_max_page_from_soup(soup)

    return rows, next_url, max_page


def guess_next_page_by_param(cur_url: str, seen_urls: set, hard_max_page):
    """
    Si NO hay link 'Siguiente', intenta avanzar con ?page=N (respetando el máximo detectado).
    """
    p = urlparse(cur_url)
    q = parse_qs(p.query)
    cur_page = 0
    if "page" in q:
        try:
            cur_page = int(q["page"][0])
        except Exception:
            cur_page = 0
    next_page = cur_page + 1

    if hard_max_page is not None and next_page > hard_max_page:
        return None

    q["page"] = [str(next_page)]
    new_q = urlencode({k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in q.items()}, doseq=True)
    next_url = urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))
    if next_url in seen_urls:
        return None
    return next_url


# =========================
#   VÍA 1: requests + BS4
# =========================
def scrape_with_requests(start_url: str):
    s = requests.Session()
    s.headers.update(HEADERS)

    url = start_url
    all_rows, seen_keys, seen_urls = [], set(), {url}
    pages = 0
    last_container_fingerprint = None
    hard_max_page = None  # se setea desde el DOM cuando se detecte

    while url and pages < MAX_PAGES:
        print(f"[REQ] Página {pages+1}: {url}")
        r = s.get(url, timeout=REQ_TIMEOUT)
        r.raise_for_status()

        rows, next_url, max_page_dom = parse_listing_page(r.text, url)

        # Actualizar máximo detectado
        if max_page_dom is not None:
            hard_max_page = max_page_dom

        # Detección de página repetida (mismo HTML)
        container_fingerprint = hash(r.text)
        if last_container_fingerprint is not None and container_fingerprint == last_container_fingerprint:
            print("[REQ] Mismo HTML que la página anterior. Fin del pagineo.")
            break
        last_container_fingerprint = container_fingerprint

        # Registrar solo filas nuevas
        found_new = 0
        for rrow in rows:
            key = (rrow.get("titulo", ""), rrow.get("url", ""))
            if key not in seen_keys:
                seen_keys.add(key)
                all_rows.append(rrow)
                found_new += 1

        if found_new == 0:
            print("[REQ] 0 resultados nuevos en esta página. Fin del pagineo.")
            break

        # Guardado incremental
        if all_rows:
            pd.DataFrame(all_rows).to_csv(
                OUTPUT_CSV, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig"
            )

        # Determinar siguiente URL
        next_candidate = None
        if next_url and next_url not in seen_urls:
            next_candidate = next_url

        if not next_candidate:
            next_candidate = guess_next_page_by_param(url, seen_urls, hard_max_page)

        if not next_candidate:
            print("[REQ] No hay siguiente página. Fin.")
            break

        seen_urls.add(next_candidate)
        url = next_candidate
        pages += 1
        time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

    print(f"[REQ] Filas totales: {len(all_rows)}")
    return all_rows


# =========================
#   VÍA 2: Selenium (fallback)
# =========================
def scrape_with_selenium(start_url: str):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager

    def build_driver():
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--lang=es-ES,es")
        chrome_options.add_argument("--window-size=1400,1000")
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=chrome_options)

    driver = build_driver()
    wait = WebDriverWait(driver, 30)

    url = start_url
    all_rows, seen_keys, seen_urls = [], set(), {url}
    pages = 0
    last_container_fingerprint = None
    hard_max_page = None

    try:
        while url and pages < MAX_PAGES:
            print(f"[SEL] Página {pages+1}: {url}")
            driver.get(url)
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".search-results, .view-content, .views-element-container, .views-row")
                )
            )
            time.sleep(random.uniform(0.8, 1.6))
            html = driver.page_source
            rows, next_url, max_page_dom = parse_listing_page(html, url)

            if max_page_dom is not None:
                hard_max_page = max_page_dom

            container_fingerprint = hash(html)
            if last_container_fingerprint is not None and container_fingerprint == last_container_fingerprint:
                print("[SEL] Mismo HTML que la página anterior. Fin del pagineo.")
                break
            last_container_fingerprint = container_fingerprint

            found_new = 0
            for rrow in rows:
                key = (rrow.get("titulo", ""), rrow.get("url", ""))
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_rows.append(rrow)
                    found_new += 1

            if found_new == 0:
                print("[SEL] 0 resultados nuevos en esta página. Fin del pagineo.")
                break

            if all_rows:
                pd.DataFrame(all_rows).to_csv(
                    OUTPUT_CSV, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig"
                )

            next_candidate = None
            if next_url and next_url not in seen_urls:
                next_candidate = next_url

            if not next_candidate:
                next_candidate = guess_next_page_by_param(url, seen_urls, hard_max_page)

            if not next_candidate:
                print("[SEL] No hay siguiente página. Fin.")
                break

            seen_urls.add(next_candidate)
            url = next_candidate
            pages += 1
            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
    finally:
        driver.quit()

    print(f"[SEL] Filas totales: {len(all_rows)}")
    return all_rows


# =========================
#   Orquestador
# =========================
def main():
    url_arg = sys.argv[1] if len(sys.argv) > 1 else input("Pega la URL del listado a scrapear: ").strip()
    start_url = validate_url(url_arg)

    print("[INFO] Intentando requests + BeautifulSoup…")
    rows = scrape_with_requests(start_url)

    if len(rows) == 0:
        print("[WARN] No se extrajo nada con requests. Probando Selenium…")
        rows = scrape_with_selenium(start_url)

    if len(rows) == 0:
        print("[ERROR] No se encontraron elementos. Revisa los selectores en parse_listing_page().")
    else:
        print(f"[OK] Total filas: {len(rows)} | CSV -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
