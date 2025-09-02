"""
segundo scrapeo de la pagina de portu
https://portucolombia.mincit.gov.co/tematicas/flujo-de-turistas-y-pasajeros/visitantes-no-residentes

pais_origen= pais de origen desde donde visitaron

vnr_origen = s el número de visitantes no residentes procedentes de un país específico que llegaron a un departamento en un mes determinado.

vnr_total_depto = Es el total de visitantes no residentes (todos los países de origen) que llegaron al departamento en ese mes.
"""# -*- coding: utf-8 -*-
import json, csv, time, random, sys, os, requests
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= Config Power BI público =========
PBI_URL      = "https://wabi-paas-1-scus-api.analysis.windows.net/public/reports/querydata?synchronous=true"
DATASET_ID   = "bc19d162-dcc7-4353-9c6a-403546f541c1"
REPORT_ID    = "3f8a664a-4b59-4144-9b83-e45e396b4925"
MODEL_ID     = 2173467
RESOURCE_KEY = "53947d13-78dc-4874-aed1-8581f1073da2"

# Visuales (ajusta si tu gráfico/tabla tiene otros IDs)
VISUAL_ID_ORIGEN   = "e4a90a8309872330bea9"  # países de origen
VISUAL_ID_DESTINOS = "e4a90a8309872330bea9"  # destinos (usar el correcto si es distinto)

# Entidades/columnas (según tus payloads)
DEPTO_ENTITY  = "Tabla_divipola_depto"
DEPTO_COLUMN  = "DEPTO "  # OJO: con espacio al final en el modelo
MEAS_ENTITY   = "TablaVNR_OE"
MEAS_NAME     = "VNR"
PAIS_ENTITY   = "MC_PAISES_OMT_RESIDENCIA"
PAIS_COLUMN   = "PaisOEEResidencia"

# ========= Parámetros =========
ANIOS  = list(range(2018, 2026))                 # 2018..2025
MESES  = list(range(1, 13))                      # 1..12
DEPTOS = ["TOLIMA", "PUTUMAYO", "CAQUETÁ", "HUILA"]  # orden solicitado

# ========= HTTP session con reintentos =========
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
        "User-Agent": "Mozilla/5.0"
    }

def _sleep_rate():
    # Rate limit: 4–8 s entre requests
    time.sleep(random.uniform(4, 8))

def _post(payload: dict, timeout: float = 60.0) -> dict:
    _sleep_rate()
    r = SESSION.post(PBI_URL, headers=_headers(), data=json.dumps(payload), timeout=timeout)
    r.raise_for_status()
    return r.json()

# ========= Parser DSR ROBUSTO =========
def _extract_rows(dsr_json: dict) -> List[List]:
    """
    Devuelve filas combinando:
      - Categóricas en 'C' (resueltas con ValueDicts)
      - Medidas en la MISMA fila:
          * si vienen como 'M' (lista), o
          * 'M0','M1',... (con 'S') o por orden de claves
    """
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

                # 1) Categóricas (C)
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

                # 2) Medidas
                if "M" in r and isinstance(r["M"], list):
                    for mv in r["M"]:
                        row_vals.append(mv)
                else:
                    measure_keys = []
                    if "S" in r and isinstance(r["S"], list):
                        for s in r["S"]:
                            mk = s.get("N")
                            if mk and mk in r:
                                measure_keys.append(mk)
                    else:
                        measure_keys = sorted([k for k in r.keys() if isinstance(k, str) and k.startswith("M")])
                    for mk in measure_keys:
                        row_vals.append(r.get(mk))

                if row_vals:
                    rows.append(row_vals)

    return rows

def _to_float(x: Optional[object]) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.replace("%", "").replace(",", "").strip())
        except Exception:
            return None
    return None

def _norm_pct(p: Optional[float]) -> Optional[float]:
    """Normaliza %: 23.5 -> 0.235 ; ya 0.235 -> 0.235."""
    if p is None:
        return None
    if p > 1.0 and p <= 100.0:
        return p / 100.0
    return p

# ========= Payloads =========
def _payload_origen(anio: int, mes: int, depto: str, count: int = 5000) -> dict:
    # País, VNR, % sobre total del visual
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {
                "Commands": [{
                    "SemanticQueryDataShapeCommand": {
                        "Query": {
                            "Version": 2,
                            "From": [
                                {"Name": "m",  "Entity": PAIS_ENTITY,  "Type": 0},
                                {"Name": "t1", "Entity": MEAS_ENTITY,  "Type": 0},
                                {"Name": "t",  "Entity": DEPTO_ENTITY, "Type": 0}
                            ],
                            "Select": [
                                {"Column":  {"Expression": {"SourceRef": {"Source": "m"}}, "Property": PAIS_COLUMN},
                                 "Name": f"{PAIS_ENTITY}.{PAIS_COLUMN}"},
                                {"Measure": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": MEAS_NAME},
                                 "Name": f"{MEAS_ENTITY}.{MEAS_NAME}"},
                                {"Arithmetic": {
                                    "Left":  {"Measure": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": MEAS_NAME}},
                                    "Right": {"ScopedEval": {"Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": MEAS_NAME}}, "Scope": []}},
                                    "Operator": 3
                                 },
                                 "Name": f"Divide({MEAS_ENTITY}.{MEAS_NAME}, ScopedEval({MEAS_ENTITY}.{MEAS_NAME}, []))"}
                            ],
                            "Where": [
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": "ANNIO"}}],
                                    "Values": [[{"Literal": {"Value": f"{anio}L"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": "MES"}}],
                                    "Values": [[{"Literal": {"Value": f"{mes}L"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": DEPTO_COLUMN}}],
                                    "Values": [[{"Literal": {"Value": f"'{depto}'"}}]]
                                }}}
                            ],
                            "OrderBy": [{
                                "Direction": 2,
                                "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": MEAS_NAME}}
                            }]
                        },
                        "Binding": {
                            "Primary": {"Groupings": [{"Projections": [0, 1, 2]}]},
                            "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": count}}},
                            "Version": 1
                        },
                        "ExecutionMetricsKind": 1
                    }
                }]
            },
            "ApplicationContext": {
                "DatasetId": DATASET_ID,
                "Sources": [{"ReportId": REPORT_ID, "VisualId": VISUAL_ID_ORIGEN}]
            }
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID
    }

def _payload_destinos_total(anio: int, mes: int, depto: str, count: int = 500) -> dict:
    # Departamento + VNR (sin porcentaje), para leer el total del destino
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {
                "Commands": [{
                    "SemanticQueryDataShapeCommand": {
                        "Query": {
                            "Version": 2,
                            "From": [
                                {"Name": "t",  "Entity": DEPTO_ENTITY, "Type": 0},
                                {"Name": "t1", "Entity": MEAS_ENTITY,  "Type": 0}
                            ],
                            "Select": [
                                {"Column":  {"Expression": {"SourceRef": {"Source": "t"}},  "Property": DEPTO_COLUMN},
                                 "Name": f"{DEPTO_ENTITY}.{DEPTO_COLUMN}"},
                                {"Measure": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": MEAS_NAME},
                                 "Name": f"{MEAS_ENTITY}.{MEAS_NAME}"}
                            ],
                            "Where": [
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": "ANNIO"}}],
                                    "Values": [[{"Literal": {"Value": f"{anio}L"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": "MES"}}],
                                    "Values": [[{"Literal": {"Value": f"{mes}L"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": DEPTO_COLUMN}}],
                                    "Values": [[{"Literal": {"Value": f"'{depto}'"}}]]
                                }}}
                            ],
                            "OrderBy": [{
                                "Direction": 2,
                                "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": MEAS_NAME}}
                            }]
                        },
                        "Binding": {
                            "Primary": {"Groupings": [{"Projections": [0, 1]}]},
                            "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": count}}},
                            "Version": 1
                        },
                        "ExecutionMetricsKind": 1
                    }
                }]
            },
            "ApplicationContext": {
                "DatasetId": DATASET_ID,
                "Sources": [{"ReportId": REPORT_ID, "VisualId": VISUAL_ID_DESTINOS}]
            }
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID
    }

def _payload_origen_por_pais(anio: int, mes: int, depto: str, pais: str) -> dict:
    """
    Payload minimalista para VNR por país específico (sin % en Select).
    Filtra ANNIO, MES, DEPTO y PAIS.
    """
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {
                "Commands": [{
                    "SemanticQueryDataShapeCommand": {
                        "Query": {
                            "Version": 2,
                            "From": [
                                {"Name": "m",  "Entity": PAIS_ENTITY,  "Type": 0},
                                {"Name": "t1", "Entity": MEAS_ENTITY,  "Type": 0},
                                {"Name": "t",  "Entity": DEPTO_ENTITY, "Type": 0}
                            ],
                            "Select": [
                                {"Measure": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": MEAS_NAME},
                                 "Name": f"{MEAS_ENTITY}.{MEAS_NAME}"}
                            ],
                            "Where": [
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": "ANNIO"}}],
                                    "Values": [[{"Literal": {"Value": f"{anio}L"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t1"}}, "Property": "MES"}}],
                                    "Values": [[{"Literal": {"Value": f"{mes}L"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": DEPTO_COLUMN}}],
                                    "Values": [[{"Literal": {"Value": f"'{depto}'"}}]]
                                }}},
                                {"Condition": {"In": {
                                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "m"}}, "Property": PAIS_COLUMN}}],
                                    "Values": [[{"Literal": {"Value": f"'{pais}'"}}]]
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
                "Sources": [{"ReportId": REPORT_ID, "VisualId": VISUAL_ID_ORIGEN}]
            }
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID
    }

# ========= Consultas =========
def consultar_origen_lista(anio: int, mes: int, depto: str) -> List[Dict]:
    data = _post(_payload_origen(anio, mes, depto))
    rows = _extract_rows(data)
    out: List[Dict] = []
    for row in rows:
        if not row:
            continue
        pais = row[0] if len(row) > 0 else None
        vnr  = _to_float(row[1] if len(row) > 1 else None)
        pct  = _norm_pct(_to_float(row[2] if len(row) > 2 else None))
        out.append({"pais_origen": pais, "vnr_origen": vnr, "pct_visual": pct})
    return out

def consultar_destino_total(anio: int, mes: int, depto: str) -> Optional[float]:
    data = _post(_payload_destinos_total(anio, mes, depto))
    rows = _extract_rows(data)
    # Esperado: [depto, vnr_total]
    for row in rows:
        if len(row) >= 2:
            v = _to_float(row[1])
            if v is not None:
                return v
    return None

def consultar_origen_por_pais(anio: int, mes: int, depto: str, pais: str) -> Optional[float]:
    """
    Devuelve el VNR (float) para un país específico. Si no hay dato, None.
    """
    payload = _payload_origen_por_pais(anio, mes, depto, pais)
    data = _post(payload)
    rows = _extract_rows(data)
    # Buscamos el primer valor numérico en la(s) fila(s)
    for row in rows:
        for val in row:
            fv = _to_float(val)
            if fv is not None:
                return fv
    return None

# ========= MAIN: vista larga =========
if __name__ == "__main__":
    salida_csv = "2_visitantes-no-residentes_2018_2025.csv"
    fieldnames = ["anio","mes","departamento","pais_origen","vnr_origen",
                  "vnr_total_depto"]

    file_exists = os.path.isfile(salida_csv)
    with open(salida_csv, "a", newline="", encoding="utf-8") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for d in DEPTOS:
            print(f"\n=== {d} ==="); sys.stdout.flush()
            for anio in ANIOS:
                for mes in MESES:
                    try:
                        # 1) Total del destino (VNR)
                        total_depto = consultar_destino_total(anio, mes, d)
                        if total_depto is None or total_depto <= 0:
                            print(f"  ({anio}-{mes:02d}) sin total destino"); sys.stdout.flush()
                            continue

                        # 2) Países (lo que muestra el visual)
                        origen_raw = consultar_origen_lista(anio, mes, d)

                        # 3) Completar VNR si viene vacío: fetch por país -> % * total
                        filas = []
                        completados_directos = 0
                        completados_fetch = 0
                        completados_por_pct = 0

                        for x in origen_raw:
                            pais = (x["pais_origen"] or "").strip()
                            if not pais:
                                continue
                            vnr  = x["vnr_origen"]
                            pct  = _norm_pct(x["pct_visual"])

                            if vnr is None:
                                # Intento 1: consulta puntual por país
                                vnr = consultar_origen_por_pais(anio, mes, d, pais)
                                if vnr is not None:
                                    completados_fetch += 1
                                else:
                                    # Intento 2: si hay %, estimar con % × total
                                    if pct is not None:
                                        vnr = pct * total_depto
                                        completados_por_pct += 1

                            if vnr is None and pct is None:
                                # No hay forma de inferir; descartar
                                continue

                            # Recalcular % final con nuestro total (consistencia)
                            pct_final = (vnr / total_depto) if (vnr is not None and total_depto) else pct
                            if x["vnr_origen"] is not None:
                                completados_directos += 1

                            filas.append({
                                "anio": anio,
                                "mes": mes,
                                "departamento": d,
                                "pais_origen": pais,
                                "vnr_origen": vnr,
                                "vnr_total_depto": total_depto
                            })

                        # 4) Escribir y log
                        if filas:
                            writer.writerows(filas)
                            fcsv.flush(); os.fsync(fcsv.fileno())
                            print(f"  ({anio}-{mes:02d}) paises_visual={len(origen_raw)} directos={completados_directos} fetch={completados_fetch} pct={completados_por_pct} escritos={len(filas)} total={total_depto}")
                        else:
                            print(f"  ({anio}-{mes:02d}) sin países con dato")

                    except requests.HTTPError as e:
                        code = e.response.status_code if e.response is not None else "NA"
                        print(f"  ({anio}-{mes:02d}) HTTP {code}"); sys.stdout.flush()
                    except Exception as e:
                        print(f"  ({anio}-{mes:02d}) Error: {e}"); sys.stdout.flush()

    print(f"\nCSV guardado: {salida_csv}")
