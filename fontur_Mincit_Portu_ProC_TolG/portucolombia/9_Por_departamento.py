# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import unicodedata
import re
from collections import Counter

# === Ruta del FACT unificado de salida (tu archivo actual) ===
IN_FILE = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\portucolombia\output\unido_fact.csv"

# === Carpetas de salida ===
BASE_OUT = Path(IN_FILE).parent
OUT_FILE_NORMALIZADO = BASE_OUT / "unido_fact_normalizado.csv"
OUT_DIR_SPLIT = BASE_OUT / "por_departamento"
OUT_DIR_SPLIT.mkdir(parents=True, exist_ok=True)

def strip_accents(s: str) -> str:
    """Quita tildes/diacríticos."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def normalize_whitespace(s: str) -> str:
    """Reemplaza NBSP y colapsa espacios."""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

# Mapa de alias (clave SIN tildes y en MAYÚSCULAS) -> nombre canónico (también sin tildes y MAYÚSCULAS)
ALIAS_MAP = {
    # Casos comunes
    "CAQUETA": "CAQUETA",
    "CAQUETA ": "CAQUETA",
    "CAQUETÁ": "CAQUETA",  # por si llega así desde otra fuente
    "CAQUETA-": "CAQUETA",
    "BOGOTA": "BOGOTA D.C.",
    "BOGOTA D.C": "BOGOTA D.C.",
    "BOGOTA DC": "BOGOTA D.C.",
    "SANTAFE DE BOGOTA": "BOGOTA D.C.",
    "ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y SANTA CATALINA": "SAN ANDRES Y PROVIDENCIA",
    "SAN ANDRES Y PROVIDENCIA": "SAN ANDRES Y PROVIDENCIA",
    "VALLE": "VALLE DEL CAUCA",
    # Puedes agregar más si detectas variantes locales
}

def canonical_departamento(raw) -> str:
    """
    Normaliza y devuelve el nombre canónico del departamento:
    - Limpia espacios, quita tildes, convierte a MAYÚSCULAS
    - Aplica ALIAS_MAP
    - Por defecto devuelve la versión SIN tildes y MAYÚSCULAS
    """
    if raw is None:
        return ""
    s = normalize_whitespace(str(raw))
    if s == "":
        return ""
    s_noacc = strip_accents(s).upper()
    # Normalizaciones menores típicas
    s_noacc = s_noacc.replace("-", " ").replace(".", " ")
    s_noacc = re.sub(r"\s+", " ", s_noacc).strip()

    # Alias (buscamos coincidencia exacta primero)
    if s_noacc in ALIAS_MAP:
        return ALIAS_MAP[s_noacc]

    # Alias por aproximación sencilla: quitar/poner "DEPARTAMENTO DEL/DE/LA"
    s_key = re.sub(r"^(DEPARTAMENTO\s+DEL?\s+|DEPARTAMENTO\s+DE\s+|DEPTO\s+DEL?\s+)", "", s_noacc).strip()
    if s_key in ALIAS_MAP:
        return ALIAS_MAP[s_key]

    # Valor por defecto: sin tildes y MAYÚSCULAS
    return s_key if s_key else s_noacc

def slugify(nombre: str) -> str:
    """Convierte el nombre canónico en un slug apto para archivo."""
    s = re.sub(r"[^A-Z0-9]+", "_", nombre.upper())
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def main():
    print(f"[INFO] Leyendo: {IN_FILE}")
    df = pd.read_csv(IN_FILE, dtype=str, encoding="utf-8-sig")

    if "departamento" not in df.columns:
        raise SystemExit("[ERROR] La columna 'departamento' no existe en el CSV.")

    # === Normalizar TODAS las filas de 'departamento' ===
    orig = df["departamento"].fillna("").astype(str)
    df["departamento"] = orig.apply(canonical_departamento)

    # Reporte de mapeos (útil para verificar qué cambió)
    pares = list(zip(orig.tolist(), df["departamento"].tolist()))
    cambios = [(o, n) for (o, n) in pares if normalize_whitespace(strip_accents(o)).upper() != n]
    if cambios:
        print("\n[INFO] Mapeos de normalizacion (muestra de 20):")
        for o, n in cambios[:20]:
            print(f"  '{o}'  ->  '{n}'")
    else:
        print("\n[INFO] No hubo cambios tras normalizar departamentos.")

    # Quitar filas sin departamento tras normalizacion
    antes = len(df)
    df = df[df["departamento"].astype(str).str.strip() != ""].copy()
    despues = len(df)
    if despues < antes:
        print(f"[WARN] Se eliminaron {antes - despues} filas sin 'departamento' valido tras normalizar.")

    # Guardar FACT normalizado completo
    df.to_csv(OUT_FILE_NORMALIZADO, index=False, encoding="utf-8-sig")
    print(f"[OK] FACT normalizado guardado en: {OUT_FILE_NORMALIZADO}")

    # === Resumen de conteos por departamento (ya normalizados) ===
    conteo = Counter(df["departamento"])
    print("\n[INFO] Distribucion por departamento:")
    for dep, cnt in sorted(conteo.items()):
        print(f"  {dep}: {cnt} filas")

    # === Split por departamento normalizado ===
    print("\n[INFO] Creando archivos por departamento...")
    for dep in sorted(df["departamento"].unique()):
        sub = df[df["departamento"] == dep].copy()
        out_path = OUT_DIR_SPLIT / f"unido_fact_{slugify(dep)}.csv"
        sub.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] {dep} -> {out_path} ({len(sub)} filas)")

    print(f"\n[DONE] Archivos creados en: {OUT_DIR_SPLIT}")

if __name__ == "__main__":
    main()
