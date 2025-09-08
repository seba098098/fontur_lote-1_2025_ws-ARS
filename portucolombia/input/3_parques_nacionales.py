"""
https://portucolombia.mincit.gov.co/tematicas/flujo-de-turistas-y-pasajeros/parques-nacionales
"""



# -*- coding: utf-8 -*-
"""
Extractor Power BI público – Parques Nacionales (Total_entr)
Modo: SOLO MENSUAL
Casos:
  - Huila / Acevedo / "PNN Cueva de los Guacharos"
  - Tolima / Ibagué / "PNN Nevados"
Salida:
  - parques_mensual_2015_2024.csv
"""

import json, csv, time, random, os, sys, requests
from typing import Dict, List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= Endpoint Power BI público =========
PBI_URL = "https://wabi-paas-1-scus-api.analysis.windows.net/public/reports/querydata?synchronous=true"

# ========= IDs del reporte (ajusta si cambian en el informe) =========
DATASET_ID = "0ed7dcb4-f978-4240-8cb7-ad8b80285b8a"
REPORT_ID  = "0be8c28d-78e7-4b4e-a22f-a3e90403ecd9"
MODEL_ID   = 2075223
VISUAL_ID  = "c8e90010b5e95081a330"   # visual con la medida Total_entr

# ========= ResourceKey del visual público (importante) =========
RESOURCE_KEY = "3d89052a-81c9-4a5f-a5b1-6d0d45a38cdf"

# ========= Entidades / columnas (según el modelo del reporte) =========
ENTITY_Q   = "q_Parques_Nacionales"
MEASURE    = "Total_entr"
COL_YEAR   = "annio"
COL_MONTH  = "mes"
COL_DPTO   = "nombre_dpto"
COL_MUNI   = "nombre_muni"
COL_ATTR   = "nomb_atractivo"

# ========= Parámetros =========
ANIOS  = list(range(2015, 2025))  # 2015..2024
MESES  = [
    "enero","febrero","marzo","abril","mayo","junio",
    "julio","agosto","septiembre","octubre","noviembre","diciembre"
]

TARGETS = [
    ("Huila",  "Acevedo", "PNN Cueva de los Guacharos"),
    ("Tolima", "Ibagué",  "PNN Nevados"),
]

# ========= Sesión HTTP con reintentos =========
SESSION = requests.Session()
retry = Retry(
    total=5, connect=5, read=5, status=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["POST"])
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json",
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
        "x-powerbi-resourcekey": RESOURCE_KEY,
        "User-Agent": "Mozilla/5.0",
    }

def _sleep_rate():
    # Para no saturar: pausa aleatoria entre 5 y 10 s
    time.sleep(random.uniform(5, 10))

def _post(payload: dict, timeout: float = 90.0) -> dict:
    _sleep_rate()
    r = SESSION.post(PBI_URL, headers=_headers(), data=json.dumps(payload), timeout=timeout)
    r.raise_for_status()
    return r.json()

# ========= Parser DSR =========
def _extract_rows(dsr_json: dict) -> List[List]:
    rows: List[List] = []
    data = dsr_json.get("results", [{}])[0].get("result", {}).get("data", {})
    dsr  = data.get("dsr")
    if not dsr or "DS" not in dsr:
        return rows

    for ds in dsr.get("DS", []):
        dicts = ds.get("ValueDicts", {}) or {}
        for ph in ds.get("PH", []):
            for r in ph.get("DM0", []):
                if not isinstance(r, dict):
                    continue
                row_vals: List[object] = []

                if "C" in r:
                    for i, val in enumerate(r["C"]):
                        if isinstance(val, int):
                            dk = f"D{i}"
                            if dk in dicts and 0 <= val < len(dicts[dk]):
                                row_vals.append(dicts[dk][val])
                            else:
                                row_vals.append(val)
                        else:
                            row_vals.append(val)

                if "M" in r and isinstance(r["M"], list):
                    row_vals.extend(r["M"])
                else:
                    keys = sorted([k for k in r if isinstance(k, str) and k.startswith("M")])
                    for k in keys:
                        row_vals.append(r.get(k))

                if row_vals:
                    rows.append(row_vals)
    return rows

def _to_float(x) -> Optional[float]:
    if isinstance(x, (int, float)): return float(x)
    if isinstance(x, str):
        try:
            return float(x.replace(",", "").replace("%","").strip())
        except Exception:
            return None
    return None

# ========= Payload con filtros (CORREGIDO) =========
def _payload_total_entr(anio: int, mes: str, dpto: str, muni: str, atractivo: str) -> dict:
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {
                "Commands": [{
                    "SemanticQueryDataShapeCommand": {
                        "Query": {
                            "Version": 2,
                            "From": [
                                {"Name": "q", "Entity": ENTITY_Q, "Type": 0}
                            ],
                            "Select": [
                                {"Aggregation": {
                                    "Expression": {"Column": {
                                        "Expression": {"SourceRef": {"Source": "q"}},
                                        "Property": MEASURE
                                    }},
                                    "Function": 0
                                }, "Name": f"Sum({ENTITY_Q}.{MEASURE})"}
                            ],
                            "Where": [
                                {
                                    "Condition": {"In": {
                                        "Expressions": [{
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "q"}},
                                                "Property": COL_YEAR
                                            }
                                        }],
                                        "Values": [[{"Literal": {"Value": f"'{anio}'"}}]]
                                    }}
                                },
                                {
                                    "Condition": {"In": {
                                        "Expressions": [{
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "q"}},
                                                "Property": COL_MONTH
                                            }
                                        }],
                                        "Values": [[{"Literal": {"Value": f"'{mes}'"}}]]
                                    }}
                                },
                                {
                                    "Condition": {"In": {
                                        "Expressions": [{
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "q"}},
                                                "Property": COL_DPTO
                                            }
                                        }],
                                        "Values": [[{"Literal": {"Value": f"'{dpto}'"}}]]
                                    }}
                                },
                                {
                                    "Condition": {"In": {
                                        "Expressions": [{
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "q"}},
                                                "Property": COL_MUNI
                                            }
                                        }],
                                        "Values": [[{"Literal": {"Value": f"'{muni}'"}}]]
                                    }}
                                },
                                {
                                    "Condition": {"In": {
                                        "Expressions": [{
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "q"}},
                                                "Property": COL_ATTR
                                            }
                                        }],
                                        "Values": [[{"Literal": {"Value": f"'{atractivo}'"}}]]
                                    }}
                                }
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

def consultar_total_entr(anio: int, mes: str, dpto: str, muni: str, atractivo: str) -> Optional[float]:
    data = _post(_payload_total_entr(anio, mes, dpto, muni, atractivo))
    rows = _extract_rows(data)
    for row in rows:
        for v in row:
            fv = _to_float(v)
            if fv is not None:
                return fv
    return None

# ========= Main (solo mensual) =========
def main():
    csv_mes = "3_parques_nacionales_2015_2024.csv"

    with open(csv_mes, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["anio","mes","departamento","municipio","atractivo","total_entr"])
        w.writeheader()

        for dpto, muni, atr in TARGETS:
            print(f"\n== {dpto} / {muni} / {atr} =="); sys.stdout.flush()
            for anio in ANIOS:
                for mes in MESES:
                    try:
                        val = consultar_total_entr(anio, mes, dpto, muni, atr)
                        if val is None:
                            print(f"  {anio}-{mes[:3]}: sin dato")
                            continue
                        w.writerow({
                            "anio": anio, "mes": mes,
                            "departamento": dpto, "municipio": muni,
                            "atractivo": atr, "total_entr": val
                        })
                        f.flush(); os.fsync(f.fileno())
                        print(f"  {anio}-{mes[:3]}: {val:.0f}")
                    except requests.HTTPError as e:
                        code = e.response.status_code if e.response is not None else "NA"
                        print(f"  {anio}-{mes[:3]}: HTTP {code}")
                    except Exception as e:
                        print(f"  {anio}-{mes[:3]}: Error {e}")

    print(f"\nCSV guardado: {csv_mes}")

if __name__ == "__main__":
    main()
