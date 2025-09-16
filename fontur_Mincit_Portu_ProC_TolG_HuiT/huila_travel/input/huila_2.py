# -*- coding: utf-8 -*-
"""
Scraper robusto Huila (v3)
- Session con reintentos/backoff
- Limpieza de DOM (sin selectores inválidos)
- Selección del mejor contenedor de contenido y recorte de descripción
- Extracción estructurada (tablas/dl) + fallback por etiquetas (corte entre etiquetas)
- Normalización de teléfonos y porcentajes
- Guardado seguro con archivo temporal + reemplazo atómico
"""

import os
import re
import time
import unicodedata
from datetime import datetime
from typing import Dict, Optional, List

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== CONFIG ==================
INPUT_CSV  = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\turismo_huila\inventario_huila.csv"
OUTPUT_CSV = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\turismo_huila\inventario_huila_detallado.csv"

SLEEP_SECONDS = 0.8         # respeta el sitio
MAX_RETRIES_HTTP = 3
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Recorta la descripción cuando encuentre cualquiera de estos encabezados/secciones
STOP_DESC_TOKENS = [
    r"\bLocalizaci[oó]n\b",
    r"\bUbicaci[oó]n\b",
    r"\bDatos\s+generales\b",
    r"\bInformaci[oó]n\s+general\b",
    r"\bMunicipio\b\s*:",
    r"\bC[oó]digo\b\s*:",
    r"\bVereda\b\s*:",
    r"\bDirecci[oó]n\b\s*:",
    r"\bPropietario\b\s*:",
    r"\bResponsable\b\s*:",
    r"\bTel[eé]fono\b\s*:",
    r"\bEstado\s*de\s*Conservaci[oó]n\b\s*:",
    r"\bConstituci[oó]n\s*del\s*Bien\b\s*:",
    r"\bRepresentatividad\b",
    r"\bVolver a lista\b",
]
STOP_DESC_RE = re.compile("|".join(STOP_DESC_TOKENS), re.I)

# ================== UTILIDADES ==================
def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_ws(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = s.replace("–", "-").replace("—", "-")
    return s.strip()

def get_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=MAX_RETRIES_HTTP,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s

def safe_to_csv(df: pd.DataFrame, dest_path: str, encoding="utf-8-sig",
                max_retries: int = 3, sleep_base: float = 1.2) -> str:
    dest_dir = os.path.dirname(dest_path) or "."
    base, ext = os.path.splitext(os.path.basename(dest_path))
    os.makedirs(dest_dir, exist_ok=True)

    tmp_path = os.path.join(dest_dir, f".{base}.tmp.{os.getpid()}.{int(time.time())}{ext}")
    df.to_csv(tmp_path, index=False, encoding=encoding)

    for attempt in range(max_retries):
        try:
            os.replace(tmp_path, dest_path)
            print(f"✔ Guardado en: {dest_path}")
            return dest_path
        except PermissionError:
            wait = sleep_base * (attempt + 1)
            print(f"⚠ Archivo bloqueado: {dest_path}. Reintentando en {wait:.1f}s...")
            time.sleep(wait)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    alt_path = os.path.join(dest_dir, f"{base}_{ts}{ext}")
    os.replace(tmp_path, alt_path)
    print(f"⚠ No se pudo sobrescribir {dest_path}. Guardado como: {alt_path}")
    return alt_path

# ================== LIMPIEZA DEL DOM ==================
NOISE_SELECTORS = [
    "header", "nav", "footer",
    "script", "style", "noscript", "iframe", "svg", "canvas", "form",
    "[role='navigation']",
    "[aria-label*='breadcrumb']",
    ".breadcrumb", ".breadcrumbs",
    ".navbar", ".navbar-nav",
    ".menu", ".menu-item",
    ".sidebar",              # válido
    "[class^='sidebar-']",  # reemplaza .sidebar-* (no soportado)
    ".widget",
    ".footer", ".header", ".site-header", ".site-footer",
    "#google_translate_element", ".gtranslate_wrapper", ".goog-te-combo",
    ".language", ".languages", ".idioma", ".idiomas",
    ".language-selector", ".language-switcher",
    ".galeria", ".gallery", ".slider", ".carousel",
    ".share", ".social", ".rrss", ".redes",
    ".pagination", ".pager",
    ".cookie", ".cookies", "#onetrust-banner-sdk", "#onetrust-consent-sdk",
    ".breadcrumbs__wrapper", ".page-title", ".hero", ".masthead",
    ".related", ".recommendations"
]

def prune_noise(soup: BeautifulSoup) -> None:
    # Quita bloques ruidosos (si un selector falla, seguimos)
    for sel in NOISE_SELECTORS:
        try:
            for el in soup.select(sel):
                el.decompose()
        except Exception:
            continue

    # Listas de solo enlaces
    for ul in soup.find_all("ul"):
        links = ul.find_all("a")
        text = ul.get_text(" ", strip=True)
        if links and len(links) >= 5 and len(text) <= 400:
            ul.decompose()

    # Bloques con alta densidad de enlaces
    for div in soup.find_all(["div", "section", "aside"]):
        text = div.get_text(" ", strip=True)
        if not text:
            continue
        links = div.find_all("a")
        if links:
            link_text = " ".join(a.get_text(" ", strip=True) for a in links)
            ld = len(link_text) / max(1, len(text))
            if ld > 0.65 and len(links) >= 5:
                div.decompose()

def score_content_node(el: Tag) -> float:
    txt = el.get_text(" ", strip=True)
    if not txt:
        return -1.0
    p_count = len(el.find_all("p"))
    a_list = el.find_all("a")
    link_text = " ".join(a.get_text(" ", strip=True) for a in a_list) if a_list else ""
    link_density = len(link_text) / max(1, len(txt))
    score = len(txt) + 50 * p_count - 200 * link_density
    h_count = sum(len(el.find_all(h)) for h in ["h1", "h2", "h3"])
    score += 30 * h_count
    return score

def best_content_node(soup: BeautifulSoup) -> Optional[Tag]:
    candidates: List[Tag] = []
    for sel in ["main", "article",
                "div#content", "div#main", "section#content", "section#main",
                "div.content", "section.content", "div.main", "section.main",
                ".node__content", ".region-content", ".entry-content", ".post-content", ".page-content",
                ".field--name-body", ".field-item", ".field-item.even", ".view-content"]:
        candidates.extend(soup.select(sel))
    if not candidates and soup.body:
        candidates = [soup.body]

    best, best_score = None, -1e9
    for el in candidates:
        s = score_content_node(el)
        if s > best_score:
            best_score, best = s, el
    return best

def extract_clean_description(soup: BeautifulSoup) -> str:
    prune_noise(soup)
    node = best_content_node(soup)
    text = node.get_text(" ", strip=True) if node else soup.get_text(" ", strip=True)
    text = norm_ws(text)

    # Quitar restos de Google Translate y similares
    GT_PATTERNS = [
        r"\bCon la tecnolog[ií]a de Traductor de Google\b",
        r"\bTraductor de Google\b",
        r"\bEspa[nñ]ol\b(?:\s+\w+){0,5}\s+(Afrik[aá]ans|Alban[eé]s|Alem[aá]n|Franc[eé]s|Italiano|Ingl[eé]s)",
    ]
    for pat in GT_PATTERNS:
        text = re.sub(pat, " ", text, flags=re.I)

    # Recorta cuando empiece la ficha técnica
    m = STOP_DESC_RE.search(text)
    if m:
        text = text[:m.start()]

    return norm_ws(text)

# ================== EXTRACCIÓN: ESTRUCTURADO ==================
def extract_from_structured_blocks(soup: BeautifulSoup) -> Dict[str, str]:
    LABEL_PATTERNS: Dict[str, str] = {
        "Municipio": r"\bMunicipio\b",
        "Vereda": r"\bVereda\b",
        "Direccion": r"Direcci[oó]n",
        "Codigo": r"C[oó]digo",
        "Responsable": r"\bResponsable\b",
        "Telefono": r"Tel[eé]fono",
        "EstadoConservacion": r"Estado\s*de\s*Conservaci[oó]n",
        "ConstitucionBien": r"Constituci[oó]n\s*del\s*Bien",
        "Representatividad": r"Representatividad(?:\s*General)?",
    }

    found: Dict[str, str] = {}

    # Tablas
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = norm_ws(cells[0].get_text(" ", strip=True))
            val = norm_ws(cells[-1].get_text(" ", strip=True))
            if not key or not val:
                continue
            key_norm = strip_accents(key).lower()

            for canonical, pat in LABEL_PATTERNS.items():
                if re.search(pat, key, flags=re.I):
                    found[canonical] = val
                    break
                if canonical.lower() in key_norm:
                    found[canonical] = val
                    break

    # Listas de definiciones
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = norm_ws(dt.get_text(" ", strip=True))
            val = norm_ws(dd.get_text(" ", strip=True))
            if not key or not val:
                continue
            for canonical, pat in LABEL_PATTERNS.items():
                if re.search(pat, key, flags=re.I):
                    found[canonical] = val
                    break

    return found

# ================== EXTRACCIÓN: FALLBACK POR ETIQUETAS ==================
LABELS = [
    ("Municipio", r"Municipio"),
    ("Vereda", r"Vereda"),
    ("Direccion", r"Direcci[oó]n"),
    ("Codigo", r"C[oó]digo"),
    ("Responsable", r"Responsable"),
    ("Telefono", r"Tel[eé]fono"),
    ("EstadoConservacion", r"Estado\s*de\s*Conservaci[oó]n"),
    ("ConstitucionBien", r"Constituci[oó]n\s*del\s*Bien"),
    ("Representatividad", r"Representatividad(?:\s*General)?"),
]
_label_union = r"(?:" + "|".join(p for _, p in LABELS) + r")"
_label_re = re.compile(rf"(?P<label>{_label_union})\s*[:：\-]\s*", re.I)

_STOP_TOKENS = [
    r"\bVolver a lista\b",
    r"\b¿Tienes preguntas\?\b",
    r"\bCopyright\b",
    r"\bDesarrollado por\b",
    r"\bEncuentra las maravillas\b",
    r"\bDatos generales\b",
    r"\bPropietario\b",
    r"\bSignificado\b",
    r"\bCalidad\b",
    r"\bDistancia\b",
    r"\bTipo de acceso\b",
]
_stop_union = re.compile("|".join(_STOP_TOKENS), re.I)

def _slice_between_labels(text: str) -> dict:
    txt = norm_ws(text)
    out = {}
    matches = list(_label_re.finditer(txt))
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i+1].start() if i+1 < len(matches) else len(txt)
        chunk = txt[start:end]
        stop_m = _stop_union.search(chunk)
        if stop_m:
            chunk = chunk[:stop_m.start()]
        key_raw = m.group("label")
        out[key_raw] = norm_ws(chunk)

    if "Municipio" not in out and matches:
        pre = norm_ws(txt[:matches[0].start()])
        if 1 <= len(pre) <= 40 and re.match(r"^[A-ZÁÉÍÓÚÑ][\w\s\-\.'ÁÉÍÓÚÑ]+$", pre):
            out["Municipio"] = pre
    return out

def _postprocess_fields(d: dict) -> dict:
    out = {}
    key_map = {
        "Dirección": "Direccion",
        "Direccion": "Direccion",
        "Código": "Codigo",
        "Codigo": "Codigo",
        "Teléfono": "Telefono",
        "Telefono": "Telefono",
        "Estado De Conservación": "EstadoConservacion",
        "Estado de Conservación": "EstadoConservacion",
        "Constitución Del Bien": "ConstitucionBien",
        "Constitucion del Bien": "ConstitucionBien",
        "Representatividad General": "Representatividad",
        "Representatividad": "Representatividad",
        "Municipio": "Municipio",
        "Vereda": "Vereda",
        "Responsable": "Responsable",
    }
    for k, v in d.items():
        k2 = key_map.get(k, k)
        out[k2] = norm_ws(v)

    if "Telefono" in out and out["Telefono"]:
        m = re.search(r"(\+?\d[\d\-\s()]{5,})", out["Telefono"])
        if m:
            out["Telefono"] = norm_ws(m.group(1))

    for k in ["EstadoConservacion", "ConstitucionBien", "Representatividad"]:
        if k in out and out[k]:
            m = re.search(r"(\d{1,3})\s?%", out[k])
            if m:
                out[k] = m.group(1) + "%"

    return out

def extract_by_regex(text: str) -> Dict[str, str]:
    raw = _slice_between_labels(text)
    return _postprocess_fields(raw)

# ================== PROCESO PRINCIPAL ==================
def main():
    try:
        df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    except Exception as e:
        raise RuntimeError(f"No se pudo leer INPUT_CSV: {INPUT_CSV}. Error: {e}")

    if "Link" not in df.columns:
        raise ValueError("El CSV debe tener una columna 'Link' con las URLs.")
    if "Nombre" not in df.columns:
        df["Nombre"] = ""

    target_cols = [
        "DescripcionCompleta",
        "Municipio",
        "Vereda",
        "Direccion",
        "Codigo",
        "Responsable",
        "Telefono",
        "EstadoConservacion",
        "ConstitucionBien",
        "Representatividad",
    ]
    for col in target_cols:
        if col not in df.columns:
            df[col] = ""

    session = get_session()

    for i, row in df.iterrows():
        url = (row["Link"] or "").strip()
        name = row.get("Nombre", "")
        if not url:
            print(f"[{i+1}/{len(df)}] URL vacía para '{name}'. Saltando.")
            continue

        try:
            resp = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
            if resp.status_code != 200:
                print(f"[{i+1}/{len(df)}] No se pudo acceder ({resp.status_code}): {url}")
                continue

            resp.encoding = resp.apparent_encoding or "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            # (a) Descripción limpia y recortada
            clean_desc = extract_clean_description(soup)
            df.at[i, "DescripcionCompleta"] = clean_desc

            # (b) Intento con bloques estructurados
            data = extract_from_structured_blocks(soup)

            # (c) Completar con regex si faltan campos
            needed = ["Municipio","Vereda","Direccion","Codigo","Responsable",
                      "Telefono","EstadoConservacion","ConstitucionBien","Representatividad"]
            missing = [k for k in needed if k not in data or not data[k]]

            if missing:
                # Usa texto completo (sin limpiar demasiado) para no perder etiquetas
                full_text = soup.get_text(" ", strip=True)
                rx = extract_by_regex(full_text)
                for k in missing:
                    if k in rx and rx[k]:
                        data.setdefault(k, rx[k])

            # (d) Asignar (vacío si no hay evidencia)
            for k in needed:
                df.at[i, k] = norm_ws(data.get(k, ""))

            print(f"[{i+1}/{len(df)}] OK: {name or url}")
            time.sleep(SLEEP_SECONDS)

        except KeyboardInterrupt:
            print("⛔ Interrumpido por el usuario.")
            break
        except requests.exceptions.RequestException as e:
            print(f"[{i+1}/{len(df)}] Error de red con {url}: {e}")
        except Exception as e:
            print(f"[{i+1}/{len(df)}] Error con {url}: {e}")

    safe_to_csv(df, OUTPUT_CSV)

if __name__ == "__main__":
    main()
