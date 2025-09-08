"""
primer scrapeo de la pagina de fontur 
https://portucolombia.mincit.gov.co/tematicas/estadisticas-territoriales/estadisticas-territoriales-de-turismo-1
"""

# -*- coding: utf-8 -*-
import json, csv, time, random, sys, os, requests
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== Config PBI =====
PBI_URL    = "https://wabi-paas-1-scus-api.analysis.windows.net/public/reports/querydata?synchronous=true"
DATASET_ID = "c4dba708-20f5-453c-a1a4-311026c16c80"
REPORT_ID  = "202e35bc-f334-49ed-84ff-1c4fe4d9a328"
MODEL_ID   = 2463006
VISUAL_ID_SERIE = "bf773346ca29b8ed8415"
RESOURCE_KEY = "c6cd032f-335b-479f-9da0-be90c7baab26"

# Solo hasta 2022
ANIOS  = list(range(2018, 2023))
MESES  = ["enero","febrero","marzo","abril","mayo","junio",
          "julio","agosto","septiembre","octubre","noviembre","diciembre"]

# Municipios manuales
MUNICIPIOS = {
    "HUILA": ["ACEVEDO","AGRADO","AIPE","ALGECIRAS","ALTAMIRA","BARAYA","CAMPOALEGRE","COLOMBIA","ELÍAS","GARZÓN",
              "GIGANTE","GUADALUPE","HOBO","ÍQUIRA","ISNOS","LA ARGENTINA","LA PLATA","NÁTAGA","NEIVA","OPORAPA",
              "PAICOL","PALERMO","PITALITO","RIVERA","SALADOBLANCO","SAN AGUSTÍN","SANTA MARÍA","SUAZA","TARQUI",
              "TELLO","TERUEL","TESALIA","TIMANÁ","VILLAVIEJA","YAGUARÁ"],
    "TOLIMA": ["IBAGUÉ","ALPUJARRA","ALVARADO","AMBALEMA","ANZOÁTEGUI","ARMERO","ATACO","CAJAMARCA","CARMEN DE APICALÁ","CASABIANCA",
               "CHAPARRAL","COELLO","COYAIMA","CUNDAY","DOLORES","ESPINAL","FALAN","FLANDES","FRESNO","GUAMO",
               "HERVEO","HONDA","ICONONZO","LÉRIDA","LÍBANO","MARIQUITA","MELGAR","MURILLO","NATAGAIMA","ORTEGA",
               "PALOCABILDO","PIEDRAS","PLANADAS","PRADO","PURIFICACIÓN","RIOBLANCO","RONCESVALLES","ROVIRA",
               "SALDAÑA","SAN ANTONIO","SAN LUIS","SANTA ISABEL","SUÁREZ","VALLE DE SAN JUAN","VENADILLO","VILLAHERMOSA","VILLARRICA"],
    "PUTUMAYO": ["MOCOA","OROITO","PUERTO ASÍS","PUERTO CAICEDO","PUERTO GUZMÁN","PUERTO LEGUÍZAMO",
                 "SANTIAGO","SIBUNDOY","SAN FRANCISCO","COLÓN","VALLE DEL GUAMUEZ","SAN MIGUEL","VILLAGARZÓN"],
    "CAQUETÁ": ["FLORENCIA","ALBANIA","BELÉN DE LOS ANDAQUÍES","CARTAGENA DEL CHAIRÁ","CURILLO","EL DONCELLO","EL PAUJIL",
                "LA MONTAÑITA","MILÁN","MORELIA","PUERTO RICO","SAN JOSÉ DEL FRAGUA","SAN VICENTE DEL CAGUÁN",
                "SOLANO","SOLITA","VALPARAÍSO"]
}

# Orden de ejecución deseado
ORDEN_DEPTOS = ["TOLIMA", "PUTUMAYO", "CAQUETÁ","HUILA"]

# ===== HTTP Session =====
SESSION = requests.Session()
retry = Retry(total=4, connect=4, read=4, status=4,
              backoff_factor=1.5,
              status_forcelist=[429, 500, 502, 503, 504],
              allowed_methods=frozenset(["POST"]))
adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
SESSION.mount("https://", adapter); SESSION.mount("http://", adapter)

def _headers():
    return {
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
        "x-powerbi-resourcekey": RESOURCE_KEY,
        "User-Agent": "Mozilla/5.0"
    }

def _sleep_rate():
    time.sleep(random.uniform(3, 8))  # 5–10 segundos entre requests

def _post(payload: dict) -> dict:
    _sleep_rate()
    r = SESSION.post(PBI_URL, headers=_headers(), data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    return r.json()

def _extract_rows(dsr_json: dict) -> List[List]:
    rows = []
    data = dsr_json.get("results", [{}])[0].get("result", {}).get("data", {})
    dsr  = data.get("dsr")
    if not dsr or "DS" not in dsr: return rows
    ds   = dsr["DS"][0]
    dicts = ds.get("ValueDicts", {})
    ph   = ds.get("PH", [])
    if not ph: return rows
    for r in ph[0].get("DM0", []):
        if "C" in r:
            vals = []
            for i, val in enumerate(r["C"]):
                if isinstance(val, int):
                    dk = f"D{i}"
                    vals.append(dicts.get(dk, [val])[val] if 0 <= val < len(dicts.get(dk, [])) else val)
                else:
                    vals.append(val)
            rows.append(vals)
    return rows

def consultar_indicadores_municipio_mes(departamento: str, municipio: str, anio: int, mes: str, top_count: int = 500) -> List[Dict]:
    payload = {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [
                            {"Name": "b", "Entity": "BASE REGIONALIZADA TOTAL", "Type": 0},
                            {"Name": "c", "Entity": "CORRELATIVA", "Type": 0},
                            {"Name": "l", "Entity": "LocalDateTable_abaea962-05bb-4971-9e99-867d8075366f", "Type": 0}
                        ],
                        "Select": [
                            {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "indicador"},
                             "Name": "BASE REGIONALIZADA TOTAL.indicador"},
                            {"Measure": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "Valor.actual.VF"},
                             "Name": "CORRELATIVA.visitantes.actual.VF"}
                        ],
                        "Where": [
                            {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "MUNICIPIO"}}],
                                            "Values": [[{"Literal": {"Value": f"'{municipio}'"}}]]}}},
                            {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "DEPARTAMENTO"}}],
                                            "Values": [[{"Literal": {"Value": f"'{departamento}'"}}]]}}},
                            {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "l"}}, "Property": "Año"}}],
                                            "Values": [[{"Literal": {"Value": f"{anio}L"}}]]}}},
                            {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "l"}}, "Property": "Mes"}}],
                                            "Values": [[{"Literal": {"Value": f"'{mes}'"}}]]}}}
                        ],
                        "OrderBy": [{
                            "Direction": 2,
                            "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "Valor.actual.VF"}}
                        }]
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0, 1]}]},
                        "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": top_count}}},
                        "Version": 1
                    },
                    "ExecutionMetricsKind": 1
                }
            }]},
            "ApplicationContext": {"DatasetId": DATASET_ID, "Sources": [{"ReportId": REPORT_ID, "VisualId": VISUAL_ID_SERIE}]}
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID
    }

    data = _post(payload)
    rows = _extract_rows(data)
    out: List[Dict] = []
    for r in rows:
        if not r: continue
        indicador = r[0] if len(r) > 0 else None
        valor     = r[1] if len(r) > 1 else None
        try:
            if isinstance(valor, str):
                valor = float(valor.replace(",", "").strip())
        except Exception:
            pass
        out.append({
            "departamento": departamento,
            "municipio": municipio,
            "anio": anio,
            "mes": mes,
            "indicador": indicador,
            "valor": valor
        })
    return out

# ===== MAIN =====
if __name__ == "__main__":
    salida_csv = "1_estadistica_territorial_2018_2022.csv"
    fieldnames = ["departamento","municipio","anio","mes","indicador","valor"]

    file_exists = os.path.isfile(salida_csv)
    with open(salida_csv, "a", newline="", encoding="utf-8") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for dpto in ORDEN_DEPTOS:
            lista_mpios = MUNICIPIOS[dpto]
            print(f"\n=== {dpto} ==="); sys.stdout.flush()
            for i, mpio in enumerate(lista_mpios, 1):
                print(f"  [{i:02d}/{len(lista_mpios):02d}] {mpio}"); sys.stdout.flush()
                for anio in ANIOS:
                    for mes in MESES:
                        try:
                            filas = consultar_indicadores_municipio_mes(dpto, mpio, anio, mes)
                            if filas:
                                writer.writerows(filas)
                                fcsv.flush(); os.fsync(fcsv.fileno())  # asegurar escritura inmediata
                                print(f"    - ({anio}-{mes}) ok ({len(filas)} filas)"); sys.stdout.flush()
                            else:
                                print(f"    - ({anio}-{mes}) sin datos"); sys.stdout.flush()
                        except Exception as e:
                            print(f"    ! ({anio}-{mes}) Error: {e}"); sys.stdout.flush()

    print(f"\nCSV guardado: {salida_csv}")
