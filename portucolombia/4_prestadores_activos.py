# -*- coding: utf-8 -*-
"""
RNT – Prestadores activos por Departamento/Municipio/Categoría (grupos)
- SIN validación previa de categorías (va directo a contar)
- DISTINCTCOUNT(Activos.RNT)
- Filtros de Departamento/Municipio en MUNICIPIOS_2022 (como los slicers del panel)
Salida: rnt_activos_por_categoria.csv
"""

import json, csv, time, random, requests, unicodedata, os
from typing import Dict, List, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======= Config =======
PBI_URL   = "https://wabi-paas-1-scus-api.analysis.windows.net/public/reports/querydata?synchronous=true"
DATASET_ID = "9853cab7-41aa-4853-93eb-e3605e0e0cd4"
REPORT_ID  = "737e95d9-c887-4b37-944b-1a3e530f7c7d"
MODEL_ID   = 2326806
VISUAL_ID  = "bd6dadf17cde8bcf453a"        # barras por categoría
RESOURCE_KEY = "b3996b70-fdb0-4d6d-aba8-9ba88a70d243"

# Si quieres inspeccionar un response JSON, pon True (guarda 'debug_validation.json' y 'debug_count.json')
DEBUG_DUMP = True

# Categorías EXACTAS (con tildes) del payload que enviaste
CATEGORIAS = [
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

# ---------------- HTTP ----------------
SESSION = requests.Session()
retry = Retry(total=5, connect=5, read=5, status=5, backoff_factor=1.5,
              status_forcelist=[429,500,502,503,504], allowed_methods=frozenset(["POST"]))
SESSION.mount("https://", HTTPAdapter(max_retries=retry))

def headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
        "x-powerbi-resourcekey": RESOURCE_KEY,
        "User-Agent": "Mozilla/5.0"
    }

def sleep(): time.sleep(random.uniform(5,10))
def NFCU(s:str)->str: return unicodedata.normalize("NFC", s.strip().upper())

# ---------------- Payload: DISTINCTCOUNT por municipio + categoría ----------------
def payload_count(depto: str, muni: str, categoria: str) -> Dict[str, Any]:
    # 6 = DistinctCount, 5 = Count
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {
                "Commands": [{
                    "SemanticQueryDataShapeCommand": {
                        "Query": {
                            "Version": 2,
                            "From": [
                                {"Name": "a", "Entity": "Activos",         "Type": 0},
                                {"Name": "m", "Entity": "MUNICIPIOS_2022", "Type": 0}
                            ],
                            "Select": [{
                                "Aggregation": {
                                    "Expression": {
                                        "Column": {
                                            "Expression": {"SourceRef": {"Source": "a"}},
                                            "Property": "RNT"
                                        }
                                    },
                                    "Function": 6
                                },
                                "Name": "DistinctCount(Activos.RNT)"
                            }],
                            "Where": [
                                {"Condition": {"In": {
                                    "Expressions": [{
                                        "Column": {
                                            "Expression": {"SourceRef": {"Source": "m"}},
                                            "Property": "NOMBRE_DEPARTAMENTO"
                                        }
                                    }],
                                    "Values": [[{"Literal": {"Value": f"'{depto}'"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{
                                        "Column": {
                                            "Expression": {"SourceRef": {"Source": "m"}},
                                            "Property": "NOMBRE_MUNICIPIO"
                                        }
                                    }],
                                    "Values": [[{"Literal": {"Value": f"'{muni}'"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{
                                        "Column": {
                                            "Expression": {"SourceRef": {"Source": "a"}},
                                            "Property": "CATEGORIA (grupos)"
                                        }
                                    }],
                                    "Values": [[{"Literal": {"Value": f"'{categoria}'"}}]]
                                }}}
                            ]
                        },
                        "Binding": {
                            "Primary": {"Groupings": [{"Projections": [0]}]},
                            "DataReduction": {"DataVolume": 3, "Primary": {"Top": {"Count": 1}}},
                            "Version": 1
                        },
                        "ExecutionMetricsKind": 1
                    }
                }]
            },
            "ApplicationContext": {
                "DatasetId": DATASET_ID,
                "Sources": [{"ReportId": REPORT_ID, "VisualId": VISUAL_ID}]
            }
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID
    }

# ---------------- Parser M ----------------
def parse_m_value(resp_json: Dict[str, Any]) -> Optional[float]:
    try:
        data = resp_json["results"][0]["result"]["data"]["dsr"]["DS"]
        for ds in data:
            for ph in ds.get("PH", []):
                for r in ph.get("DM0", []):
                    if isinstance(r.get("M"), list) and r["M"]:
                        return float(r["M"][0])
                    for k in r:
                        if isinstance(k, str) and k.startswith("M") and r[k] is not None:
                            return float(r[k])
    except Exception:
        pass
    return None

# ---------------- Cliente ----------------
def contar(depto: str, muni: str, cat: str, dump_once: bool=False) -> int:
    payload = payload_count(depto, muni, cat)
    sleep()
    r = SESSION.post(PBI_URL, headers=headers(), data=json.dumps(payload), timeout=90)
    r.raise_for_status()
    if DEBUG_DUMP and dump_once:
        with open("debug_count.json","w",encoding="utf-8") as f:
            f.write(r.text)
    v = parse_m_value(r.json())
    return int(v) if v is not None else 0

# ---------------- Main ----------------
def main():
    out = "rnt_activos_por_categoria.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["departamento","municipio","categoria_grupo","rnt_count"])
        w.writeheader()

        first_dump_done = False

        for depto, lista in MUNICIPIOS.items():
            dpto = NFCU(depto)
            for m in lista:
                muni = NFCU(m)
                for cat in CATEGORIAS:
                    catu = NFCU(cat)  # las etiquetas que enviaste ya vienen con tildes; reforzamos NFC+upper
                    try:
                        cnt = contar(dpto, muni, catu, dump_once=(not first_dump_done))
                        first_dump_done = True
                        w.writerow({
                            "departamento": dpto,
                            "municipio": muni,
                            "categoria_grupo": catu,
                            "rnt_count": cnt
                        })
                        f.flush()
                        print(f"{dpto} | {muni} | {catu} -> {cnt}")
                    except requests.HTTPError as e:
                        code = e.response.status_code if e.response is not None else "NA"
                        print(f"HTTP {code} en {dpto}/{muni}/{catu}")
                        w.writerow({
                            "departamento": dpto, "municipio": muni,
                            "categoria_grupo": catu, "rnt_count": -1
                        })
                    except Exception as ex:
                        print(f"Error {dpto}/{muni}/{catu}: {ex}")
                        w.writerow({
                            "departamento": dpto, "municipio": muni,
                            "categoria_grupo": catu, "rnt_count": -1
                        })

    print(f"\nCSV guardado: {out}")

if __name__ == "__main__":
    main()
