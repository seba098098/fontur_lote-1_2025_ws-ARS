# -*- coding: utf-8 -*-
"""
RNT – Extracción por API de Datos Abiertos (Socrata/SoQL), SIN Power BI.
Hace: DISTINCT COUNT(RNT) por Departamento/Municipio/Categoría (grupos),
apoyándose en el esquema real de la tabla para ubicar columnas.

Salida: rnt_activos_por_categoria.csv
"""

import csv, json, os, time, random, requests, unicodedata
from typing import Dict, List, Optional

# ---- CONFIG ----
# Datasets candidatos (todos de datos.gov.co con API SoQL):
CANDIDATE_DATASETS = [
    # Registro Nacional de Turismo - RNT
    "thwd-ivmp",  # https://www.datos.gov.co/Comercio-Industria-y-Turismo/Registro-Nacional-de-Turismo-RNT/thwd-ivmp
    # Histórico RNT
    "jqjy-rhzv",  # https://www.datos.gov.co/Comercio-Industria-y-Turismo/Hist-rico-Registro-Nacional-de-Turismo-RNT/jqjy-rhzv
    # Prestadores RNT
    "npkw-6rke",  # https://www.datos.gov.co/Comercio-Industria-y-Turismo/Prestadores-Registro-Nacional-de-Turismo/npkw-6rke
]

BASE = "https://www.datos.gov.co"
TIMEOUT = 60
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")  # opcional, pero recomendado si harás muchas peticiones

# Tus categorías objetivo (las mismas que estabas usando)
CATEGORIAS_OBJ = [
    "VIVIENDAS TURÍSTICAS",
    "ESTABLECIMIENTOS DE ALOJAMIENTO TURÍSTICO",
    "AGENCIAS DE VIAJES",
    "GUIAS DE TURISMO",
    "ESTABLECIMIENTOS DE GASTRONOMÍA Y SIMILARES",
    "OTROS PRESTADORES",
    "OFICINAS DE REPRESENTACION TURÍSTICA",
    "OPERADORES DE PLATAFORMAS ELECTRÓNICAS O DIGITALES DE SERVICIOS TURÍSTICOS",
    "ORGANIZADORES DE BODA DESTINO",
    "EMPRESAS DE TIEMPO COMPARTIDO Y MULTIPROPIEDAD",
    "USUARIOS INDUSTRIALES OPERADORES O DESARROLLADORES DE SERVICIOS TURISTICOS DE LAS ZONAS FRANCAS",
    "COMPAÑÍAS DE INTERCAMBIO VACACIONAL",
    "OTROS TIPOS DE HOSPEDAJE TURÍSTICOS NO PERMANENTES",
    "EMPRESAS DE TRANSPORTE TERRESTRE AUTOMOTOR",
]

# Municipios (tu lista)
MUNICIPIOS: Dict[str, List[str]] = {
    "HUILA": ["ACEVEDO","AGRADO","AIPE","ALGECIRAS","ALTAMIRA","BARAYA","CAMPOALEGRE","COLOMBIA","ELÍAS","GARZÓN",
              "GIGANTE","GUADALUPE","HOBO","ÍQUIRA","ISNOS","LA ARGENTINA","LA PLATA","NÁTAGA","NEIVA","OPORAPA",
              "PAICOL","PALERMO","PITALITO","RIVERA","SALADOBLANCO","SAN AGUSTÍN","SANTA MARÍA","SUAZA","TARQUI",
              "TELLO","TERUEL","TESALIA","TIMANÁ","VILLAVIEJA","YAGUARÁ"],
    "TOLIMA": ["IBAGUÉ","ALPUJARRA","ALVARADO","AMBALEMA","ANZOÁTEGUI","ARMERO","ATACO","CAJAMARCA","CARMEN DE APICALÁ","CASABIANCA",
               "CHAPARRAL","COELLO","COYAIMA","CUNDAY","DOLORES","ESPINAL","FALAN","FLANDES","FRESNO","GUAMO",
               "HERVEO","HONDA","ICONONZO","LÉRIDA","LÍBANO","MARIQUITA","MELGAR","MURILLO","NATAGAIMA","ORTEGA",
               "PALOCABILDO","PIEDRAS","PLANADAS","PRADO","PURIFICACIÓN","RIOBLANCO","RONCESVALLES","ROVIRA",
               "SALDAÑA","SAN ANTONIO","SAN LUIS","SANTA ISABEL","SUÁREZ","VALLE DE SAN JUAN","VENADILLO","VILLAHERMOSA","VILLARRICA"],
    "PUTUMAYO": ["MOCOA","ORITO","PUERTO ASÍS","PUERTO CAICEDO","PUERTO GUZMÁN","PUERTO LEGUÍZAMO",
                 "SANTIAGO","SIBUNDOY","SAN FRANCISCO","COLÓN","VALLE DEL GUAMUEZ","SAN MIGUEL","VILLAGARZÓN"],
    "CAQUETÁ": ["FLORENCIA","ALBANIA","BELÉN DE LOS ANDAQUÍES","CARTAGENA DEL CHAIRÁ","CURILLO","EL DONCELLO","EL PAUJIL",
                "LA MONTAÑITA","MILÁN","MORELIA","PUERTO RICO","SAN JOSÉ DEL FRAGUA","SAN VICENTE DEL CAGUÁN",
                "SOLANO","SOLITA","VALPARAÍSO"]
}

# ---- Helpers ----
def H(s: str) -> str:
    return unicodedata.normalize("NFC", s.strip().upper())

def headers():
    h = {"Accept": "application/json"}
    if APP_TOKEN:
        h["X-App-Token"] = APP_TOKEN
    return h

def sleep():
    time.sleep(random.uniform(0.3, 0.8))  # SODA tolera más QPS, pero no abuses sin token

# ---- Socrata metadata & query ----
def get_metadata(dataset_id: str) -> Optional[dict]:
    url = f"{BASE}/api/views/{dataset_id}.json"
    r = requests.get(url, headers=headers(), timeout=TIMEOUT)
    if r.ok:
        return r.json()
    return None

def find_columns(meta: dict) -> Optional[dict]:
    """
    Busca columnas candidatas en el dataset:
    - rnt: algo que contenga 'rnt' (numero_rnt, rnt, nro_rnt, etc.)
    - departamento: 'departamento'
    - municipio: 'municipio'
    - categoria: preferible 'categoría' con 'grupo' o 'categoria' a secas
    Devuelve nombres de campos **tal cual** los usa la API (fieldName).
    """
    cols = meta.get("columns", [])
    def fieldnames():
        for c in cols:
            name = (c.get("name") or "").lower()
            fn = (c.get("fieldName") or c.get("name") or "").strip()
            yield name, fn

    rnt_fn = depto_fn = muni_fn = cat_fn = None
    # prioridad inteligible
    for name, fn in fieldnames():
        if rnt_fn is None and "rnt" in name:
            rnt_fn = fn
        if depto_fn is None and "depart" in name:
            depto_fn = fn
        if muni_fn is None and ("munic" in name or "municipio" in name):
            muni_fn = fn
        # categorias puede venir como "categoría (grupos)" / "categoria (grupos)" / "categoria"
        if cat_fn is None and "categor" in name:
            cat_fn = fn

    if all([rnt_fn, depto_fn, muni_fn, cat_fn]):
        return {"rnt": rnt_fn, "depto": depto_fn, "muni": muni_fn, "cat": cat_fn}
    return None

def soql_count_by_cat(dataset_id: str, cols: dict, dpto: str, muni: str) -> List[dict]:
    """
    Usa SoQL:
      $select = {cat} as categoria, count(distinct {rnt}) as rnt_count
      $where  = upper({depto})='{DPTO}' AND upper({muni})='{MUNI}'
      $group  = categoria
      $order  = categoria
    """
    endpoint = f"{BASE}/resource/{dataset_id}.json"
    select = f"{cols['cat']} as categoria, count(distinct {cols['rnt']}) as rnt_count"
    where = f"upper({cols['depto']})='{dpto}' AND upper({cols['muni']})='{muni}'"
    params = {
        "$select": select,
        "$where": where,
        "$group": "categoria",
        "$order": "categoria",
        "$limit": 50000
    }
    r = requests.get(endpoint, params=params, headers=headers(), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def choose_dataset() -> Optional[tuple]:
    """Elige el primer dataset que tenga las columnas necesarias."""
    for ds in CANDIDATE_DATASETS:
        meta = get_metadata(ds)
        if not meta:
            continue
        cols = find_columns(meta)
        if cols:
            return ds, cols
    return None

# ---- MAIN ----
def main():
    chosen = choose_dataset()
    if not chosen:
        print("[ERROR] No pude encontrar un dataset de RNT con columnas compatibles en datos.gov.co")
        return
    dataset_id, cols = chosen
    print(f"[INFO] Usando dataset: {dataset_id} con columnas {cols}")

    out = "rnt_activos_por_categoria.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["departamento","municipio","categoria_grupo","rnt_count"])
        w.writeheader()

        target_norm = {unicodedata.normalize('NFD', c).encode('ascii','ignore').decode('ascii').lower(): c
                       for c in CATEGORIAS_OBJ}

        for depto, lista in MUNICIPIOS.items():
            dpto = H(depto)
            for muni in lista:
                muni_up = H(muni)
                try:
                    sleep()
                    rows = soql_count_by_cat(dataset_id, cols, dpto, muni_up)
                except requests.HTTPError as e:
                    print(f"[HTTP] {e.response.status_code} en {dpto}/{muni_up}")
                    continue
                except Exception as ex:
                    print(f"[ERR] {dpto}/{muni_up}: {ex}")
                    continue

                if not rows:
                    print(f"[WARN] Sin data para {dpto}/{muni_up}")
                    continue

                # filtrar a tus categorías objetivo (normalizando acentos/case)
                for r in rows:
                    cat = (r.get("categoria") or "").strip()
                    rnt = int(float(r.get("rnt_count", 0)))
                    key = unicodedata.normalize('NFD', cat).encode('ascii','ignore').decode('ascii').lower()
                    if key in target_norm:
                        w.writerow({
                            "departamento": dpto,
                            "municipio": muni_up,
                            "categoria_grupo": target_norm[key],  # el nombre tal como tú lo quieres
                            "rnt_count": rnt
                        })
                print(f"{dpto} | {muni_up} -> {len(rows)} filas (filtradas a tus categorías)")

    print("\nCSV listo: rnt_activos_por_categoria.csv")

if __name__ == "__main__":
    main()
