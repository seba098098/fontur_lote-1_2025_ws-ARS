# -*- coding: utf-8 -*-
"""
Unificador de CSVs (Caquetá, Huila, Putumayo, Tolima)
- Lee todos los CSV de una carpeta
- Homologa columnas (dos esquemas distintos)
- Asigna departamento usando diccionario interno de municipios
- Clasifica 'fuente' en {fontur, procolombia, desconocida}
- Exporta CSV y Excel (consolidado + por departamento)
"""

import os
import glob
import unicodedata
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd

# ==========================
# CONFIGURACIÓN
# ==========================
CARPETA_ENTRADA = r"C:\Users\carlo\OneDrive\Escritorio\TrabajoURL\fontur_lote-1_2025_ws-ARS\fontur_Mincit_Portu_ProC_TolG\urlFP\input"   # <-- CAMBIA esta ruta
CARPETA_SALIDA  = os.path.join(CARPETA_ENTRADA, "salida")
DEPTOS_OBJETIVO = ["Caquetá", "Huila", "Putumayo", "Tolima"]

# ==========================
# DICCIONARIO MUNICIPIOS
# ==========================
DEPTOS_MUNICIPIOS = {
    "Huila": [
        "Neiva","Acevedo","Agrado","Aipe","Algeciras","Altamira","Baraya","Campoalegre","Colombia","Elías",
        "Garzón","Gigante","Guadalupe","Hobo","Íquira","Isnos","La Argentina","La Plata","Nátaga","Oporapa",
        "Paicol","Palermo","Palestina","Pital","Pitalito","Rivera","Saladoblanco","San Agustín",
        "Santa María","Suaza","Tarqui","Tello","Teruel","Tesalia","Timaná","Villavieja","Yaguará"
    ],
    "Tolima": [
        "Ibagué","Alpujarra","Alvarado","Ambalema","Anzoátegui","Armero","Ataco","Cajamarca","Carmen de Apicalá",
        "Casabianca","Chaparral","Coello","Coyaima","Cunday","Dolores","Espinal","Falan","Flandes","Fresno",
        "Guamo","Herveo","Honda","Icononzo","Lérida","Líbano","Mariquita","Melgar","Murillo","Natagaima",
        "Ortega","Palocabildo","Piedras","Planadas","Prado","Purificación","Rioblanco","Roncesvalles",
        "Rovira","Saldaña","San Antonio","San Luis","Santa Isabel","Suárez","Valle de San Juan",
        "Venadillo","Villahermosa","Villarrica"
    ],
    "Putumayo": [
        "Mocoa","Colón","Orito","Puerto Asís","Puerto Caicedo","Puerto Guzmán","Puerto Leguízamo",
        "San Francisco","San Miguel","Santiago","Sibundoy","Valle del Guamuez","Villagarzón"
    ],
    "Caquetá": [
        "Florencia","Albania","Belén de los Andaquíes","Cartagena del Chairá","Curillo","El Doncello",
        "El Paujíl","La Montañita","Milán","Morelia","Puerto Rico","San José del Fragua",
        "San Vicente del Caguán","Solano","Solita","Valparaíso"
    ]
}

# ==========================
# UTILIDADES
# ==========================
def sin_tildes_mayus(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return " ".join(s.split()).upper().strip()

def estandarizar_departamento(valor: str) -> str:
    v = sin_tildes_mayus(valor)
    mapping = {
        "CAQUETA": "Caquetá",
        "HUILA": "Huila",
        "PUTUMAYO": "Putumayo",
        "TOLIMA": "Tolima",
    }
    return mapping.get(v, "")

# Mapa inverso MUNICIPIO_NORMALIZADO -> DEPARTAMENTO_CANON
MUNI2DEPTO = {}
for depto, municipios in DEPTOS_MUNICIPIOS.items():
    for m in municipios:
        MUNI2DEPTO[sin_tildes_mayus(m)] = depto

def pick_category(texto: str) -> str:
    t = (texto or "").lower()
    if any(x in t for x in ["festival","feria","evento","congreso","vitrina"]):
        return "evento"
    if any(x in t for x in ["hotel","hospedaje","hostal","alojamiento","glamping"]):
        return "alojamiento"
    if any(x in t for x in ["parque","reserva","pnn","sendero","natural","avistamiento"]):
        return "naturaleza"
    if any(x in t for x in ["gastronom","restaurante","plaza mercado","cocina"]):
        return "gastronomía"
    if any(x in t for x in ["museo","centro cultural","arte","patrimonio"]):
        return "cultura"
    return "otro"

def parse_fecha_iso(valor) -> str:
    if valor is None or str(valor).strip() == "":
        return datetime.now().isoformat()
    s = str(valor).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).isoformat()
        except ValueError:
            continue
    try:
        return pd.to_datetime(s, errors="coerce").to_pydatetime().isoformat()
    except Exception:
        return datetime.now().isoformat()

def asegurar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def inferir_depto_desde_ubicacion(ubicacion: str) -> str:
    if not ubicacion or str(ubicacion).strip() == "":
        return ""
    # ¿ya es dpto?
    d = estandarizar_departamento(ubicacion)
    if d:
        return d
    # tokens por separadores comunes
    raw = str(ubicacion)
    tokens = []
    for sep in [",", " - ", "-", "|", "—", "/"]:
        if sep in raw:
            tokens = [t.strip() for t in raw.split(sep) if t and t.strip()]
            break
    if not tokens:
        tokens = [raw]
    # probar como dpto
    for t in tokens:
        d = estandarizar_departamento(t)
        if d:
            return d
    # probar como municipio
    for t in tokens:
        m = sin_tildes_mayus(t)
        if m in MUNI2DEPTO:
            return MUNI2DEPTO[m]
    # último intento: todo el texto como municipio
    m = sin_tildes_mayus(raw)
    return MUNI2DEPTO.get(m, "")

def inferir_depto_por_municipio(municipio: str) -> str:
    if not municipio or str(municipio).strip() == "":
        return ""
    return MUNI2DEPTO.get(sin_tildes_mayus(municipio), "")

def clasificar_fuente(origen_archivo: str, enlace: str) -> str:
    """
    Heurística:
    - Si nombre de archivo o dominio contiene 'fontur' -> 'fontur'
    - Si contiene 'procolombia' o 'colombia.travel' -> 'procolombia'
    - En otro caso -> 'desconocida'
    """
    texto = f"{origen_archivo or ''} {enlace or ''}".lower()
    dominio = ""
    try:
        dominio = urlparse(enlace or "").netloc.lower()
    except Exception:
        pass

    hay_fontur = any(k in texto for k in ["fontur"]) or any(k in (dominio or "") for k in ["fontur.com.co"])
    hay_proco  = any(k in texto for k in ["procolombia","colombia.travel"]) or any(k in (dominio or "") for k in ["procolombia.co","colombia.travel"])

    if hay_fontur:
        return "fontur"
    if hay_proco:
        return "procolombia"
    return "desconocida"

# ==========================
# HOMOLOGACIÓN DE ESQUEMAS
# ==========================
COLUMNAS_SALIDA = [
    "titulo","categoria","descripcion","enlace","imagen","ubicacion",
    "tipo","fecha_extraccion","precio","telefono","detalles","_list_url",
    "origen_archivo","fuente"
]

def a_esquema_unico(df: pd.DataFrame) -> pd.DataFrame:
    df = asegurar_columnas(df)

    # columnas candidatas
    col_titulo  = "titulo" if "titulo" in df.columns else ("title" if "title" in df.columns else None)
    col_desc    = "descripcion" if "descripcion" in df.columns else ("description" if "description" in df.columns else None)
    col_enlace  = "enlace" if "enlace" in df.columns else ("link" if "link" in df.columns else ("url" if "url" in df.columns else None))
    col_imagen  = "imagen" if "imagen" in df.columns else ("image" if "image" in df.columns else None)
    col_ubic    = "ubicacion" if "ubicacion" in df.columns else ("location" if "location" in df.columns else None)
    col_tipo    = "tipo" if "tipo" in df.columns else None
    col_fecha   = "fecha_extraccion" if "fecha_extraccion" in df.columns else ("fecha" if "fecha" in df.columns else None)
    col_precio  = "precio" if "precio" in df.columns else None
    col_tel     = "telefono" if "telefono" in df.columns else ("tel" if "tel" in df.columns else None)
    col_det     = "detalles" if "detalles" in df.columns else ("detail" if "detail" in df.columns else None)
    col_listurl = "_list_url" if "_list_url" in df.columns else None
    col_muni    = "municipio" if "municipio" in df.columns else ("ciudad" if "ciudad" in df.columns else None)

    def default_categoria(row):
        if "categoria" in df.columns and pd.notna(row.get("categoria")) and str(row.get("categoria")).strip():
            return row["categoria"]
        txt = ""
        if col_desc and pd.notna(row.get(col_desc)):
            txt = str(row.get(col_desc))
        elif col_titulo and pd.notna(row.get(col_titulo)):
            txt = str(row.get(col_titulo))
        return pick_category(txt)

    def default_descripcion(row):
        if col_desc and pd.notna(row.get(col_desc)) and str(row.get(col_desc)).strip():
            return str(row.get(col_desc))
        return "Destino turístico"

    def default_enlace(row):
        if col_enlace and pd.notna(row.get(col_enlace)):
            return str(row.get(col_enlace))
        return ""

    def default_imagen(row):
        if col_imagen and pd.notna(row.get(col_imagen)):
            return str(row.get(col_imagen))
        return None

    def default_tipo(row):
        if col_tipo and pd.notna(row.get(col_tipo)) and str(row.get(col_tipo)).strip():
            return str(row.get(col_tipo))
        return "resultado_busqueda" if col_listurl else "destino"

    def default_fecha(row):
        if col_fecha and pd.notna(row.get(col_fecha)):
            return parse_fecha_iso(row.get(col_fecha))
        return datetime.now().isoformat()

    def default_precio(row):
        if col_precio and pd.notna(row.get(col_precico := col_precio)) and str(row.get(col_precico)).strip():
            return str(row.get(col_precico))
        return "Consultar" if default_tipo(row) == "destino" else None

    def default_telefono(row):
        if col_tel and pd.notna(row.get(col_tel)) and str(row.get(col_tel)).strip():
            return str(row.get(col_tel))
        return ""

    def default_detalles(row):
        if col_det and pd.notna(row.get(col_det)) and str(row.get(col_det)).strip():
            return str(row.get(col_det))
        return None

    def default_list_url(row):
        if col_listurl and pd.notna(row.get(col_listurl)):
            return str(row.get(col_listurl))
        return None

    registros = []
    for _, row in df.iterrows():
        titulo = str(row.get(col_titulo, "")).strip() if col_titulo else ""
        categoria = default_categoria(row)
        descripcion = default_descripcion(row)
        enlace = default_enlace(row)
        imagen = default_imagen(row)
        tipo = default_tipo(row)
        fecha_extraccion = default_fecha(row)
        precio = default_precio(row)
        telefono = default_telefono(row)
        detalles = default_detalles(row)
        list_url = default_list_url(row)

        # UBICACIÓN -> DEPARTAMENTO
        depto = ""
        if col_ubic and pd.notna(row.get(col_ubic)) and str(row.get(col_ubic)).strip():
            depto = inferir_depto_desde_ubicacion(str(row.get(col_ubic)))
        if not depto and col_muni and pd.notna(row.get(col_muni)) and str(row.get(col_muni)).strip():
            depto = inferir_depto_por_municipio(str(row.get(col_muni)))
        if not depto:
            for d in DEPTOS_OBJETIVO:
                if d.lower() in titulo.lower():
                    depto = d
                    break

        registros.append({
            "titulo": titulo,
            "categoria": categoria,
            "descripcion": descripcion,
            "enlace": enlace,
            "imagen": imagen,
            "ubicacion": depto,  # DEPARTAMENTO canon si se pudo
            "tipo": tipo,
            "fecha_extraccion": fecha_extraccion,
            "precio": precio,
            "telefono": telefono,
            "detalles": detalles,
            "_list_url": list_url,
            # 'origen_archivo' y 'fuente' se completan afuera
            "origen_archivo": "",
            "fuente": ""
        })

    out = pd.DataFrame(registros, columns=COLUMNAS_SALIDA)
    return out

# ==========================
# EJECUCIÓN
# ==========================
def main():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)

    archivos = sorted(glob.glob(os.path.join(CARPETA_ENTRADA, "*.csv")))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron CSV en: {CARPETA_ENTRADA}")

    frames = []
    for path in archivos:
        try:
            df = pd.read_csv(path)
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="latin-1")
        df_u = a_esquema_unico(df)
        df_u["origen_archivo"] = os.path.basename(path)
        # clasificar fuente
        df_u["fuente"] = df_u.apply(lambda r: clasificar_fuente(r.get("origen_archivo"), r.get("enlace")), axis=1)
        frames.append(df_u)

    base = pd.concat(frames, ignore_index=True)

    # Clasificación objetivo
    base["ubicacion"] = base["ubicacion"].fillna("")
    es_objetivo = base["ubicacion"].isin(DEPTOS_OBJETIVO)
    base_obj = base[es_objetivo].copy()
    base_no = base[~es_objetivo | (base["ubicacion"] == "")].copy()

    # ----------------
    # EXPORTAR CSV
    # ----------------
    path_consol_csv = os.path.join(CARPETA_SALIDA, "consolidado.csv")
    base.to_csv(path_consol_csv, index=False, encoding="utf-8-sig")

    for d in DEPTOS_OBJETIVO:
        sub = base_obj[base_obj["ubicacion"] == d]
        sub.to_csv(os.path.join(CARPETA_SALIDA, f"{d}.csv"), index=False, encoding="utf-8-sig")

    base_no.to_csv(os.path.join(CARPETA_SALIDA, "reporte_no_clasificados.csv"),
                   index=False, encoding="utf-8-sig")

    # ----------------
    # EXPORTAR EXCEL
    # ----------------
    # 1) Consolidado con hojas por dpto
    path_consol_xlsx = os.path.join(CARPETA_SALIDA, "consolidado.xlsx")
    with pd.ExcelWriter(path_consol_xlsx, engine="openpyxl") as xw:
        base.to_excel(xw, sheet_name="Consolidado", index=False)
        for d in DEPTOS_OBJETIVO:
            sub = base_obj[base_obj["ubicacion"] == d]
            # Evitar hojas vacías si no hay registros
            if len(sub) > 0:
                sub.to_excel(xw, sheet_name=d[:31], index=False)  # límite nombre hoja Excel

    # 2) Un Excel por departamento
    for d in DEPTOS_OBJETIVO:
        sub = base_obj[base_obj["ubicacion"] == d]
        if len(sub) > 0:
            sub.to_excel(os.path.join(CARPETA_SALIDA, f"{d}.xlsx"), index=False)

    # Resumen
    print("=== RESUMEN ===")
    print("Consolidado CSV:", path_consol_csv)
    print("Consolidado XLSX:", path_consol_xlsx)
    for d in DEPTOS_OBJETIVO:
        print(f"{d}: {len(base_obj[base_obj['ubicacion']==d])} filas")
    print("No clasificados:", len(base_no))

if __name__ == "__main__":
    main()
