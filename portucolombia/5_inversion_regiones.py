# -*- coding: utf-8 -*-
"""
Power BI (público) -> ÚNICO CSV consolidado
Cortes incluidos en el MISMO archivo:
  - departamento
  - fecha_excl_2019
  - linea
  - impacto

Prioriza: HUILA, TOLIMA, PUTUMAYO, CAQUETA
"""

import json, time, random, csv, os
from typing import Dict, List, Optional, Iterable, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ====== Endpoint y credenciales públicas (ajusta resource key si cambia) ======
PBI_URL = "https://wabi-paas-1-scus-api.analysis.windows.net/public/reports/querydata?synchronous=true"
RESOURCE_KEY = "a04a4e5d-fdb7-4243-b18d-1b00284261d2"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"

# ====== IDs del reporte/visual/model (según tu payload que funcionó) ======
DATASET_ID = "524368d3-2b8b-499d-811c-c4a2ba94dd4f"
REPORT_ID  = "fd490a55-21f5-40cc-a6d5-3cb786db54d9"
VISUAL_ID  = "402b3517bbfee1b432d1"
MODEL_ID   = 2168271

# ====== Parámetros de segmentación (EDITABLES) ======
DEPARTAMENTOS = ["HUILA", "TOLIMA", "PUTUMAYO", "CAQUETA"]

# Usa literales EXACTOS del panel para línea e impacto
LINEAS = [
    "APOYO_A_LA_CADENA_DE_VALOR_DEL_SECTOR_TURISMO_EN_SITUACIONES_DE_EMERGENCIA"
]
IMPACTOS = ["NACIONAL"]

# ====== Sesión HTTP robusta ======
SESSION = requests.Session()
_retry = Retry(
    total=5, connect=5, read=5, status=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["POST"]),
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=20, pool_maxsize=20)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

def _headers() -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
        "User-Agent": UA,
        "x-powerbi-resourcekey": RESOURCE_KEY,
    }

def _sleep():
    time.sleep(random.uniform(5, 10))  # ventana 5–10 s

def _post(payload: dict, timeout: float = 90.0) -> dict:
    _sleep()
    r = SESSION.post(PBI_URL, headers=_headers(), data=json.dumps(payload), timeout=timeout)
    r.raise_for_status()
    return r.json()

# ====== Parser DSR genérico ======
def parse_dsr_rows(dsr_json: dict) -> List[List]:
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
                row: List[object] = []
                # columnas categóricas
                if "C" in r:
                    for i, v in enumerate(r["C"]):
                        if isinstance(v, int):
                            dk = f"D{i}"
                            row.append(dicts.get(dk, [v])[v] if 0 <= v < len(dicts.get(dk, [])) else v)
                        else:
                            row.append(v)
                # medidas
                if "M" in r and isinstance(r["M"], list):
                    row.extend(r["M"])
                else:
                    for k in sorted([k for k in r if isinstance(k, str) and k.startswith("M")]):
                        row.append(r.get(k))
                if row:
                    rows.append(row)
    return rows

# ====== Plantillas de payload ======
def _base_app_context():
    return {"DatasetId": DATASET_ID, "Sources": [{"ReportId": REPORT_ID, "VisualId": VISUAL_ID}]}

def _wrap(query_obj: dict) -> dict:
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{"SemanticQueryDataShapeCommand": query_obj}]},
            "QueryId": "",
            "ApplicationContext": _base_app_context(),
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID,
    }

def _select_block():
    # Orden esperada => mapeo por posición
    return [
        {"Column": {"Expression": {"SourceRef": {"Source": "g"}}, "Property": "LATITUD "}, "Name": "lat"},
        {"Column": {"Expression": {"SourceRef": {"Source": "g"}}, "Property": "LONGITUD "}, "Name": "lon"},
        {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "g"}}, "Property": "Departamento arreglado "}}, "Function": 3}, "Name": "departamento"},
        {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Sumatoria_total_proyectos"}, "Name": "total_proyectos"},
        {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Sumatoria_total_solicitando"}, "Name": "total_solicitado_fnt"},
        {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "%Solicitando"}, "Name": "pct_solicitado"},
        {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Sumatoria_total_contrapartida"}, "Name": "total_contrapartida"},
        {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "%contrapartidaproyecto"}, "Name": "pct_contrapartida"},
        {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Conteo_proyectos"}, "Name": "num_proyectos"},
    ]

def payload_departamento(dep: str) -> dict:
    query = {
        "Query": {
            "Version": 2,
            "From": [
                {"Name": "g", "Entity": "Geo", "Type": 0},
                {"Name": "f", "Entity": "Fecha de actualizacion", "Type": 0},
                {"Name": "s", "Entity": "Sheet 1", "Type": 0},
            ],
            "Select": _select_block(),
            "Where": [{
                "Condition": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Departamento.Regionalizado"}}],
                    "Values": [[{"Literal": {"Value": f"'{dep}'"}}]]
                }}
            }],
            "OrderBy": [{
                "Direction": 2,
                "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Sumatoria_total_proyectos"}}
            }],
        },
        "Binding": {"Primary": {"Groupings": [{"Projections": list(range(9))}]}, "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}}, "Version": 1},
        "ExecutionMetricsKind": 1
    }
    return _wrap(query)

def payload_fecha_excluye_2019(dep: str) -> dict:
    query = {
        "Query": {
            "Version": 2,
            "From": [
                {"Name": "g", "Entity": "Geo", "Type": 0},
                {"Name": "f", "Entity": "Fecha de actualizacion", "Type": 0},
                {"Name": "l", "Entity": "LocalDateTable_ae23acf8-68e2-44c2-bd15-f11c73152d80", "Type": 0},
                {"Name": "s", "Entity": "Sheet 1", "Type": 0},
            ],
            "Select": _select_block(),
            "Where": [
                {"Condition": {"Not": {"Expression": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "l"}}, "Property": "Año"}}],
                    "Values": [[{"Literal": {"Value": "2019L"}}]]
                }}}}},
                {"Condition": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Departamento.Regionalizado"}}],
                    "Values": [[{"Literal": {"Value": f"'{dep}'"}}]]
                }}},
            ],
            "OrderBy": [{
                "Direction": 2,
                "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Sumatoria_total_proyectos"}}
            }],
        },
        "Binding": {"Primary": {"Groupings": [{"Projections": list(range(9))}]}, "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}}, "Version": 1},
        "ExecutionMetricsKind": 1
    }
    return _wrap(query)

def payload_linea(dep: str, linea_literal: str) -> dict:
    query = {
        "Query": {
            "Version": 2,
            "From": [
                {"Name": "g", "Entity": "Geo", "Type": 0},
                {"Name": "f", "Entity": "Fecha de actualizacion", "Type": 0},
                {"Name": "s", "Entity": "Sheet 1", "Type": 0},
            ],
            "Select": _select_block(),
            "Where": [
                {"Condition": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Línea.Estratégica.a.la.que.aplica"}}],
                    "Values": [[{"Literal": {"Value": f"'{linea_literal}'"}}]]
                }}},
                {"Condition": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Departamento.Regionalizado"}}],
                    "Values": [[{"Literal": {"Value": f"'{dep}'"}}]]
                }}},
            ],
            "OrderBy": [{
                "Direction": 2,
                "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Sumatoria_total_proyectos"}}
            }],
        },
        "Binding": {"Primary": {"Groupings": [{"Projections": list(range(9))}]}, "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}}, "Version": 1},
        "ExecutionMetricsKind": 1
    }
    return _wrap(query)

def payload_impacto(dep: str, impacto_literal: str, linea_literal: str) -> dict:
    query = {
        "Query": {
            "Version": 2,
            "From": [
                {"Name": "g", "Entity": "Geo", "Type": 0},
                {"Name": "f", "Entity": "Fecha de actualizacion", "Type": 0},
                {"Name": "s", "Entity": "Sheet 1", "Type": 0},
            ],
            "Select": _select_block(),
            "Where": [
                {"Condition": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Línea.Estratégica.a.la.que.aplica"}}],
                    "Values": [[{"Literal": {"Value": f"'{linea_literal}'"}}]]
                }}},
                {"Condition": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Regionalizado"}}],
                    "Values": [[{"Literal": {"Value": f"'{impacto_literal}'"}}]]
                }}},
                {"Condition": {"In": {
                    "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Departamento.Regionalizado"}}],
                    "Values": [[{"Literal": {"Value": f"'{dep}'"}}]]
                }}},
            ],
            "OrderBy": [{
                "Direction": 2,
                "Expression": {"Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Sumatoria_total_proyectos"}}
            }],
        },
        "Binding": {"Primary": {"Groupings": [{"Projections": list(range(9))}]}, "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}}, "Version": 1},
        "ExecutionMetricsKind": 1
    }
    return _wrap(query)

# ====== Utilidad: normalizar una fila al esquema del CSV ======
CSV_HEADER = [
    "corte", "departamento_filtro", "linea_filtro", "impacto_filtro",
    "lat", "lon", "departamento",
    "total_proyectos", "total_solicitado_fnt", "pct_solicitado",
    "total_contrapartida", "pct_contrapartida", "num_proyectos"
]

def fila_csv(
    corte: str, dep_sel: str, linea_sel: Optional[str], imp_sel: Optional[str], row: List
) -> List:
    # Mapeo por posición conforme al bloque Select:
    # 0: lat, 1: lon, 2: departamento (agg), 3..8 medidas
    lat, lon, dep_agregado = row[0], row[1], row[2]
    m = [row[i] if i < len(row) else None for i in range(3, 9)]
    return [
        corte, dep_sel, (linea_sel or ""), (imp_sel or ""),
        lat, lon, dep_agregado,
        m[0], m[1], m[2], m[3], m[4], m[5]
    ]

# ====== Runner consolidado ======
def main():
    out_csv = "pbi_resumen_departamentos.csv"
    wrote_header = False

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        for dep in DEPARTAMENTOS:
            # 1) Corte: departamento
            data = _post(payload_departamento(dep))
            for row in parse_dsr_rows(data):
                if not wrote_header:
                    w.writerow(CSV_HEADER); wrote_header = True
                w.writerow(fila_csv("departamento", dep, None, None, row))

            # 2) Corte: fecha_excl_2019
            data = _post(payload_fecha_excluye_2019(dep))
            for row in parse_dsr_rows(data):
                w.writerow(fila_csv("fecha_excl_2019", dep, None, None, row))

            # 3) Corte: línea
            for linea in LINEAS:
                data = _post(payload_linea(dep, linea))
                for row in parse_dsr_rows(data):
                    w.writerow(fila_csv("linea", dep, linea, None, row))

            # 4) Corte: impacto (con línea)
            for linea in LINEAS:
                for imp in IMPACTOS:
                    data = _post(payload_impacto(dep, imp, linea))
                    for row in parse_dsr_rows(data):
                        w.writerow(fila_csv("impacto", dep, linea, imp, row))

    print(f"[OK] CSV consolidado -> {out_csv}")

if __name__ == "__main__":
    main()
