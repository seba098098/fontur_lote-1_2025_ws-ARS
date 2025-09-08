# -*- coding: utf-8 -*-
import csv
import re
from pathlib import Path
import pandas as pd

# =========================
# Descubrimiento de rutas
# =========================
BASE_DIR = Path.cwd()
IN_DIR_CANDIDATES = [BASE_DIR / "input", BASE_DIR]  # busca primero en ./input/, luego en la carpeta actual

def log_info(msg):  print(f"[INFO] {msg}")
def log_ok(lbl, path): print(f"[OK] {lbl}: {path}")
def log_warn(msg): print(f"[WARN] {msg}")
def log_err(msg):  print(f"[ERROR] {msg}")

# Patrones (regex) para encontrar archivos (insensible a mayúsculas, _ o -)
PATTERNS = {
    "1_estadistica_territorial": [
        r"^1[_-]estadistica[_-]territorial.*2018.*2022.*\.csv$",
    ],
    "2_vnr": [
        r"^2[_-]visitantes[_-]?no[_-]residentes.*2018.*2025.*\.csv$",
    ],
    "3_parques": [
        r"^3[_-]parques[_-]mensual.*2015.*2024.*\.csv$",
    ],
    "4_rnt_cat": [
        r"^4[_-]rnt[_-]activos[_-]por[_-]categoria.*\.csv$",
    ],
    "5_colegios": [
        r"^5[_-]colegios.*\.csv$",
    ],
    "6_checkin": [
        r"^6[_-].*check.*bioseguridad.*\.csv$",
    ],
}

def find_one(pattern_list):
    comps = [re.compile(p, re.IGNORECASE) for p in pattern_list]
    candidates = []
    for d in IN_DIR_CANDIDATES:
        if not d.exists():
            continue
        # Buscar solo en nivel actual (rápido). Si prefieres recursivo: rglob("*.csv")
        for p in d.glob("*.csv"):
            name = p.name
            if any(c.search(name) for c in comps):
                candidates.append(p)
    if not candidates:
        return None
    # Determinístico por nombre alfabético
    candidates = sorted(candidates, key=lambda x: x.name.lower())
    return candidates[0]

# =========================
# Utilidades
# =========================
def std_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas (minúsculas, sin tildes, con guion bajo)."""
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace("á","a",regex=False)
                  .str.replace("é","e",regex=False)
                  .str.replace("í","i",regex=False)
                  .str.replace("ó","o",regex=False)
                  .str.replace("ú","u",regex=False)
                  .str.replace("ñ","n",regex=False)
                  .str.replace(" ", "_", regex=False)
    )
    return df

MONTH_MAP = {
    "enero":1, "febrero":2, "marzo":3, "abril":4, "mayo":5, "junio":6,
    "julio":7, "agosto":8, "septiembre":9, "setiembre":9, "octubre":10,
    "noviembre":11, "diciembre":12
}

def month_to_int(x):
    if pd.isna(x): return pd.NA
    s = str(x).strip().lower()
    if s.isdigit():
        n = int(s); return n if 1 <= n <= 12 else pd.NA
    return MONTH_MAP.get(s, pd.NA)

def read_csv_flexible(path: Path) -> pd.DataFrame:
    """Lectura robusta con Sniffer + fallback de separadores/encodings."""
    # 1) Sniffer
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
            sample = f.read(4096)
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig", sep=dialect.delimiter)
    except Exception:
        pass
    # 2) Fallback separadores
    for sep in [",",";","\t","|"]:
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", sep=sep)
            if df.shape[1] > 1:
                return df
        except Exception:
            continue
    # 3) Fallback encoding
    for enc in ["utf-8", "latin-1"]:
        try:
            df = pd.read_csv(path, dtype=str, encoding=enc)
            if df.shape[1] > 1:
                return df
        except Exception:
            continue
    raise RuntimeError(f"No fue posible leer: {path}")

def letters_ratio(series: pd.Series) -> float:
    """% de filas con letras (para detectar columnas textuales)."""
    s = series.dropna().astype(str).str.strip()
    if s.empty: return 0.0
    return (s.str.contains(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", regex=True)).mean()

def numeric_like_ratio(series: pd.Series) -> float:
    """% de filas que parecen numéricas (incluye notación científica)."""
    s = series.dropna().astype(str).str.strip().str.replace(" ", "", regex=False).str.replace(",", "", regex=False)
    if s.empty: return 0.0
    return (s.str.match(r"^[0-9]+(\.[0-9]+)?([eE][\+\-]?[0-9]+)?$")).mean()

def read_hoteles_bioseguridad(path: Path) -> pd.DataFrame:
    """
    Lector tolerante para '6_hoteles_Check-in_de_bioseguridad.csv'.
    Si una fila tiene más de 5 celdas (comas en dirección), las extra se pegan en 'direccion'.
    """
    rows = []
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        # Saltar encabezado si existe
        _ = next(reader, None)
        for parts in reader:
            if not parts:
                continue
            if len(parts) < 4:
                continue
            if len(parts) == 4:
                dep, mun, rnt, razon = parts
                direccion = ""
            else:
                dep, mun, rnt, razon = parts[0], parts[1], parts[2], parts[3]
                direccion = ",".join(parts[4:])
            rows.append([dep, mun, rnt, razon, direccion])
    df = pd.DataFrame(rows, columns=["departamento","municipio","rnt","razon_social","direccion"])
    return df

def _norm_indicador_checkin(x: str):
    """Unifica variantes al rótulo final 'Prestadores con Check-in de bioseguridad'."""
    if not isinstance(x, str):
        return x
    key = re.sub(r"\s+", " ", x.lower().strip().replace("-", " "))
    aliases = {
        "check in": "Prestadores con Check-in de bioseguridad",
        "checkin": "Prestadores con Check-in de bioseguridad",
        "check in de bioseguridad": "Prestadores con Check-in de bioseguridad",
        "check in bioseguridad": "Prestadores con Check-in de bioseguridad",
        "sello check in": "Prestadores con Check-in de bioseguridad",
        "prestadores con check in de bioseguridad": "Prestadores con Check-in de bioseguridad",
        "prestadores con check in de bioseguridad ": "Prestadores con Check-in de bioseguridad",
        "prestadores con check-in de bioseguridad": "Prestadores con Check-in de bioseguridad",
    }
    return aliases.get(key, x)

# =========================
# Main
# =========================
def main():
    log_info(f"Carpeta de ejecución: {BASE_DIR}")
    in_root = None
    for d in IN_DIR_CANDIDATES:
        if d.exists():
            in_root = d
            break
    if in_root is None:
        raise SystemExit("[ERROR] No existe carpeta de entrada (ni ./input ni la carpeta actual).")

    # Buscar archivos
    p1 = find_one(PATTERNS["1_estadistica_territorial"])
    if p1: log_ok("1_estadistica_territorial", p1)
    p2 = find_one(PATTERNS["2_vnr"])
    if p2: log_ok("2_visitantes_no_residentes", p2)
    p3 = find_one(PATTERNS["3_parques"])
    if p3: log_ok("3_parques_mensual", p3)
    p4 = find_one(PATTERNS["4_rnt_cat"])
    if p4: log_ok("4_rnt_activos_por_categoria", p4)
    p5 = find_one(PATTERNS["5_colegios"])
    if p5: log_ok("5_colegios", p5)
    p6 = find_one(PATTERNS["6_checkin"])
    if p6: log_ok("6_hoteles_Check-in_de_bioseguridad", p6)

    missing = [lbl for lbl, p in [
        ("1_estadistica_territorial", p1),
        ("2_visitantes_no_residentes", p2),
        ("3_parques_mensual", p3),
        ("4_rnt_activos_por_categoria", p4),
        ("5_colegios", p5),
        ("6_hoteles_Check-in_de_bioseguridad", p6),
    ] if p is None]

    if missing:
        log_err(f"Faltan archivos de entrada: {missing}")
        raise SystemExit("Revisa los nombres o coloca los CSV en ./input o en la carpeta actual.")

    # ---- Cargar y normalizar nombres
    df1 = std_cols(read_csv_flexible(p1))
    df2 = std_cols(read_csv_flexible(p2))
    df3 = std_cols(read_csv_flexible(p3))
    df4 = std_cols(read_csv_flexible(p4))
    df5 = std_cols(read_csv_flexible(p5))
    df6 = std_cols(read_hoteles_bioseguridad(p6))

    # ---- df1 (estadística territorial)
    df1 = df1.rename(columns={"año":"anio","ano":"anio"})
    if "anio" in df1.columns:
        df1["anio"] = pd.to_numeric(df1["anio"], errors="coerce").astype("Int64")
    if "mes" in df1.columns:
        df1["mes"] = df1["mes"].apply(month_to_int).astype("Int64")
    need1 = ["departamento","municipio","anio","mes","indicador","valor"]
    miss1 = [c for c in need1 if c not in df1.columns]
    if miss1: raise SystemExit(f"[ERROR] En {p1.name} faltan columnas: {miss1}")

    # ---- df2 (VNR por origen + total departamento)
    df2 = df2.rename(columns={"año":"anio","ano":"anio"})
    need2 = ["anio","mes","departamento","pais_origen","vnr_origen","vnr_total_depto"]
    miss2 = [c for c in need2 if c not in df2.columns]
    if miss2: raise SystemExit(f"[ERROR] En {p2.name} faltan columnas: {miss2}")
    df2["anio"] = pd.to_numeric(df2["anio"], errors="coerce").astype("Int64")
    df2["mes"]  = df2["mes"].apply(month_to_int).astype("Int64")

    # ---- df3 (Parques por atractivo)
    df3 = df3.rename(columns={"año":"anio","ano":"anio","total_entr":"valor"})
    need3 = ["anio","mes","departamento","municipio","atractivo","valor"]
    miss3 = [c for c in need3 if c not in df3.columns]
    if miss3: raise SystemExit(f"[ERROR] En {p3.name} faltan columnas: {miss3}")
    df3["anio"] = pd.to_numeric(df3["anio"], errors="coerce").astype("Int64")
    df3["mes"]  = df3["mes"].apply(month_to_int).astype("Int64")

    # ---- df4 (RNT por categoría) SIN fecha en fuente
    df4 = df4.rename(columns={"categoria_grupo":"categoria","rnt_count":"valor"})
    need4 = ["departamento","municipio","categoria","valor"]
    miss4 = [c for c in need4 if c not in df4.columns]
    if miss4: raise SystemExit(f"[ERROR] En {p4.name} faltan columnas: {miss4}")
    df4["anio"] = pd.Series(pd.NA, index=df4.index, dtype="Int64")
    df4["mes"]  = pd.Series(pd.NA, index=df4.index, dtype="Int64")

    # ---- df5 (Colegios) SIN fecha y SIN 'dane' en el FACT final
    df5 = df5.applymap(lambda x: re.sub(r"\s+", " ", x.strip()) if isinstance(x, str) else x)

    # Usar 'colegio_turisticos' si viene con texto
    if "colegio_turisticos" in df5.columns and letters_ratio(df5["colegio_turisticos"]) >= 0.3:
        df5["colegio"] = df5["colegio_turisticos"]

    # Reparar caso 'colegio' numérico y 'dane' textual (encabezado corrido)
    if "colegio" in df5.columns and "dane" in df5.columns:
        if numeric_like_ratio(df5["colegio"]) >= 0.6 and letters_ratio(df5["dane"]) >= 0.3:
            df5["colegio"], df5["dane"] = df5["dane"], df5["colegio"]

    # Si 'colegio' aún no luce textual, buscar mejor candidata
    if "colegio" not in df5.columns or letters_ratio(df5["colegio"]) < 0.3:
        excl = {"departamento","municipio","dane","anio","ano","año","mes","valor","categoria","colegio_turisticos"}
        candidates = [c for c in df5.columns if c not in excl]
        if candidates:
            best = max(candidates, key=lambda c: letters_ratio(df5[c]))
            if letters_ratio(df5[best]) >= 0.3:
                df5["colegio"] = df5[best]

    need5 = ["departamento","municipio","colegio"]
    miss5 = [c for c in need5 if c not in df5.columns]
    if miss5: raise SystemExit(f"[ERROR] En {p5.name} faltan columnas: {miss5}")
    df5 = df5.drop(columns=["dane","colegio_turisticos"], errors="ignore")
    df5["anio"] = pd.Series(pd.NA, index=df5.index, dtype="Int64")
    df5["mes"]  = pd.Series(pd.NA, index=df5.index, dtype="Int64")

    # ---- df6 (Prestadores con Check-in) SIN fecha en fuente
    need6 = ["departamento","municipio","rnt","razon_social","direccion"]
    miss6 = [c for c in need6 if c not in df6.columns]
    if miss6: raise SystemExit(f"[ERROR] En {p6.name} faltan columnas: {miss6}")

    for c in ["razon_social","direccion","municipio","departamento"]:
        if c in df6.columns:
            df6[c] = df6[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    df6.loc[df6["rnt"].astype(str).str.strip().isin(["", "nan", "None"]), "rnt"] = pd.NA
    df6["anio"] = pd.Series(pd.NA, index=df6.index, dtype="Int64")
    df6["mes"]  = pd.Series(pd.NA, index=df6.index, dtype="Int64")
    df6 = df6.rename(columns={
        "razon_social": "prestador",
        "rnt": "registro_nacional_de_turismo"
    })

    # =========================
    # Esquema unificado (con 'registro_nacional_de_turismo')
    # =========================
    cols_out = [
        "anio","mes","departamento","municipio","indicador",
        "atractivo","categoria","colegio","prestador","registro_nacional_de_turismo",
        "direccion","pais_origen","valor"
    ]

    # A) df1 -> indicadores territoriales
    df1_u = (
        df1.assign(atractivo=pd.NA, categoria=pd.NA, colegio=pd.NA,
                   prestador=pd.NA, registro_nacional_de_turismo=pd.NA, direccion=pd.NA, pais_origen=pd.NA)
           [cols_out]
    )

    # B1) df2 -> VNR por origen
    df2_origen = (
        df2.rename(columns={"vnr_origen":"valor"})
           .assign(municipio=pd.NA, atractivo=pd.NA, categoria=pd.NA, colegio=pd.NA,
                   prestador=pd.NA, registro_nacional_de_turismo=pd.NA, direccion=pd.NA,
                   indicador="Visitantes No Residentes por origen")
           [cols_out]
    )

    # B2) df2 -> VNR total por departamento
    df2_total = (
        df2[["anio","mes","departamento","vnr_total_depto"]]
          .drop_duplicates()
          .rename(columns={"vnr_total_depto":"valor"})
          .assign(municipio=pd.NA, atractivo=pd.NA, categoria=pd.NA, colegio=pd.NA,
                  prestador=pd.NA, registro_nacional_de_turismo=pd.NA, direccion=pd.NA,
                  pais_origen=pd.NA, indicador="Visitantes No Residentes total por departamento")
          [cols_out]
    )

    # C) df3 -> Parques por atractivo
    df3_u = (
        df3.assign(indicador="Visitantes por atractivo",
                   categoria=pd.NA, colegio=pd.NA, prestador=pd.NA, registro_nacional_de_turismo=pd.NA, direccion=pd.NA,
                   pais_origen=pd.NA)
           [cols_out]
    )

    # D) df4 -> Prestadores (RNT) por categoría
    df4_u = (
        df4.assign(indicador="Prestadores de Servicios Turísticos activos por categoria",
                   atractivo=pd.NA, colegio=pd.NA, prestador=pd.NA, registro_nacional_de_turismo=pd.NA, direccion=pd.NA,
                   pais_origen=pd.NA)
           [cols_out]
    )

    # E) df5 -> Colegios (una fila por colegio, valor=1)
    df5_u = (
        df5.assign(indicador="Colegios Amigos del Turismo",
                   atractivo=pd.NA, categoria=pd.NA, prestador=pd.NA, registro_nacional_de_turismo=pd.NA, direccion=pd.NA,
                   pais_origen=pd.NA, valor=1)
           [cols_out]
    )

    # F) df6 -> Prestadores con Check-in de bioseguridad (unificado)
    df6_u = (
        df6.assign(indicador="Prestadores con Check-in de bioseguridad",
                   atractivo=pd.NA, categoria=pd.NA, colegio=pd.NA, pais_origen=pd.NA, valor=1)
           [cols_out]
    )

    # ---- Unión
    final = pd.concat([df1_u, df2_origen, df2_total, df3_u, df4_u, df5_u, df6_u], ignore_index=True)

    # ---- Normalización de indicador a la etiqueta final para cualquier variante
    final["indicador"] = final["indicador"].apply(_norm_indicador_checkin)

    final = final.sort_values(
        ["anio","mes","departamento","municipio","indicador","atractivo","categoria",
         "colegio","prestador","registro_nacional_de_turismo","pais_origen"],
        na_position="last"
    ).reset_index(drop=True)

    # ---- Salida en ./output/unido_fact.csv
    OUT_DIR = BASE_DIR / "output"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE = OUT_DIR / "unido_fact.csv"
    final.to_csv(OUT_FILE, index=False, encoding="utf-8")
    log_ok("Archivo final", OUT_FILE)

    # Chequeos rápidos
    try:
        sample_checkin = final.loc[final["indicador"]=="Prestadores con Check-in de bioseguridad",
                                   ["departamento","municipio","prestador","registro_nacional_de_turismo","direccion"]].head(8)
        print("\n[CHECK] Muestra 'Prestadores con Check-in de bioseguridad':")
        print(sample_checkin.to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()
