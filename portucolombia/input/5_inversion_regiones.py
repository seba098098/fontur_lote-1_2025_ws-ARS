# -*- coding: utf-8 -*-
import os, sys, json, csv, time, random, copy, re
import requests
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== CONFIG ==================
PBI_URL            = "https://wabi-paas-1-scus-api.analysis.windows.net/public/reports/querydata?synchronous=true"
RESOURCE_KEY       = "577008ec-a209-4227-8193-0eff9bbe47ce"   # tu x-powerbi-resourcekey
BASE_PAYLOAD_FILE  = "payload_base.json"                      # payload del visual (copiado desde F12)
OUT_CSV            = "colegios_cat_filtrado.csv"

# Listas solicitadas
OBJETIVO = {
    "HUILA":   ["GARZON", "NEIVA", "PAICOL", "SAN AUSTIN", "SAN AGUSTIN", "TESALIA"],
    "TOLIMA":  ["FALAN", "HONDA", "IBAGUE", "IBAGUÉ", "LIBANO", "LÍBANO", "MELGAR"],
    "CAQUETÁ": ["FLORENCIA"],
    "PUTUMAYO":["COLON", "COLÓN", "MOCOA", "ORINTO", "ORITO"]
}

# Nombres esperados en tu payload base (ajústalos si en tu Select son otros)
ENTITY_BASE          = "Base CAT"      # entidad que tiene los campos
PROP_DEPARTAMENTO    = "Departamento"  # columna de departamento
PROP_MUNICIPIO       = "Municipio"     # columna de municipio

# ================== HTTP SESSION ==================
SESSION = requests.Session()
retry = Retry(
    total=5, connect=5, read=5, status=5,
    backoff_factor=1.4,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["POST"])
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
SESSION.mount("https://", adapter); SESSION.mount("http://", adapter)

def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
        "x-powerbi-resourcekey": RESOURCE_KEY,
        "User-Agent": "Mozilla/5.0"
    }

def _sleep():
    time.sleep(random.uniform(2.0, 5.0))

def _post(payload: dict) -> dict:
    _sleep()
    r = SESSION.post(PBI_URL, headers=_headers(), data=json.dumps(payload), timeout=90)
    r.raise_for_status()
    return r.json()

# ================== PAYLOAD UTILS ==================
def load_base_payload(path: str) -> dict:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"No existe {path}. Guarda ahí tu payload copiado desde F12.")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _set_in_condition(where_list: list, entity: str, prop: str, value_literal: str, replace_only: bool=False) -> bool:
    """
    Busca en where_list una condición In para (entity.prop) y pone el valor (literal).
    Si no existe y replace_only=False, crea la condición al final.
    Devuelve True si reemplazó o creó, False si no pudo.
    """
    found = False
    for cond in where_list:
        try:
            inside = cond["Condition"]["In"]
            exprs  = inside.get("Expressions", [])
            if not exprs: continue
            col = exprs[0].get("Column", {})
            prop_name   = col.get("Property", "")
            src_expr    = col.get("Expression", {})
            src_ref     = src_expr.get("SourceRef", {})
            src_name    = src_ref.get("Source", "")
            if src_name == entity and prop_name == prop:
                inside["Values"] = [[{"Literal": {"Value": f"'{value_literal}'"}}]]
                found = True
        except Exception:
            continue
    if found:
        return True

    if replace_only:
        return False

    # crear al final
    where_list.append({
        "Condition": {
            "In": {
                "Expressions": [{
                    "Column": {
                        "Expression": {"SourceRef": {"Source": entity}},
                        "Property": prop
                    }
                }],
                "Values": [[{"Literal": {"Value": f"'{value_literal}'"}}]]
            }
        }
    })
    return True

def prepare_payload(base_payload: dict, departamento: str, municipio: str) -> dict:
    """
    Clona el payload y establece filtros de Departamento y Municipio.
    Si no existen en Where, los agrega.
    """
    p = copy.deepcopy(base_payload)
    queries = p.get("queries", [])

    for q in queries:
        commands = q.get("Query", {}).get("Commands", [])
        for cmd in commands:
            if "SemanticQueryDataShapeCommand" not in cmd:
                continue
            query_obj = cmd["SemanticQueryDataShapeCommand"].get("Query", {})
            where_list = query_obj.setdefault("Where", [])

            # 1) Departamento
            ok_dep = _set_in_condition(where_list, ENTITY_BASE, PROP_DEPARTAMENTO, departamento, replace_only=False)
            # 2) Municipio
            ok_mun = _set_in_condition(where_list, ENTITY_BASE, PROP_MUNICIPIO, municipio, replace_only=False)

            # Aumentar ventana de filas si existe Binding
            binding = cmd["SemanticQueryDataShapeCommand"].get("Binding", {})
            dr = binding.get("DataReduction", {})
            pr = dr.get("Primary", {})
            if "Top" in pr:
                pr["Top"]["Count"] = max(50000, pr["Top"].get("Count", 0))  # por si estaba bajo

    return p

# ================== PARSER (DM0 / RW) ==================
def descriptor_names(data: dict) -> List[str]:
    sel = data.get("descriptor", {}).get("Select", [])
    return [s.get("Name", f"col{i}") for i, s in enumerate(sel)]

def get_ds(data: dict) -> dict:
    return data["dsr"]["DS"][0]

def try_dm0_rows(ds: dict, names: List[str]) -> Optional[List[List[str]]]:
    ph = ds.get("PH", [])
    if not ph: return None
    dm0 = ph[0].get("DM0", [])
    if not dm0: return None

    rows = []
    # Caso tarjeta: M0..Mn
    if "M0" in dm0[0]:
        row = []
        for i, _ in enumerate(names):
            row.append("" if dm0[0].get(f"M{i}") is None else str(dm0[0].get(f"M{i}")))
        rows.append(row)
        return rows

    # Caso C[]
    if "C" in dm0[0]:
        value_dicts = ds.get("ValueDicts", {})
        dict_keys = sorted(value_dicts.keys(), key=lambda k: int(k[1:])) if value_dicts else []
        c = dm0[0]["C"]
        out, idx = [], 0
        for cell in c:
            val = cell
            if isinstance(cell, int) and idx < len(dict_keys):
                dk = dict_keys[idx]
                vals = value_dicts.get(dk, [])
                if 0 <= cell < len(vals):
                    val = vals[cell]
                idx += 1
            out.append("" if val is None else str(val))
        if len(out) < len(names):
            out += [""] * (len(names) - len(out))
        rows.append(out[:len(names)])
        return rows
    return None

def collect_rw(ds: dict) -> List[dict]:
    found = []
    def walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "RW" and isinstance(v, list):
                    found.extend(v)
                else:
                    walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)
    walk(ds)
    return found

def decode_rw_rows(ds: dict, names: List[str]) -> List[List[str]]:
    value_dicts = ds.get("ValueDicts", {})
    dict_keys = sorted(value_dicts.keys(), key=lambda k: int(k[1:])) if value_dicts else []
    rows = []
    for r in collect_rw(ds):
        if "C" not in r or not isinstance(r["C"], list):
            continue
        out, idx = [], 0
        for cell in r["C"]:
            val = cell
            if isinstance(cell, int) and idx < len(dict_keys):
                dk = dict_keys[idx]
                vals = value_dicts.get(dk, [])
                if 0 <= cell < len(vals):
                    val = vals[cell]
                idx += 1
            out.append("" if val is None else str(val))
        if len(out) < len(names):
            out += [""] * (len(names) - len(out))
        rows.append(out[:len(names)])
    return rows

# ================== MAPEOS / PROYECCIÓN ==================
FINAL_HEADERS = ["departamento","municipio","colegio","dane","latitud","longitud"]

PATTERNS = {
    "departamento": [r"depart"],
    "municipio":    [r"munic"],
    "colegio":      [r"coleg", r"instituci", r"plantel", r"nombre"],
    "dane":         [r"dane", r"identificaci", r"codi.*dane"],
    "latitud":      [r"lat"],
    "longitud":     [r"lon", r"long"]
}

def build_index(names: List[str]) -> Dict[str, int]:
    idx_map: Dict[str, int] = {}
    lowered = [n.lower() for n in names]
    for key, pats in PATTERNS.items():
        chosen = -1
        for i, nm in enumerate(lowered):
            for p in pats:
                if re.search(p, nm):
                    chosen = i; break
            if chosen >= 0: break
        if chosen < 0:
            # fallback exacto por si ya están con estos alias
            for i, nm in enumerate(lowered):
                if nm.strip() == key:
                    chosen = i; break
        idx_map[key] = chosen
    return idx_map

def project_row(row: List[str], names: List[str], idx_map: Dict[str, int]) -> List[str]:
    out = []
    for h in FINAL_HEADERS:
        i = idx_map.get(h, -1)
        out.append(row[i] if 0 <= i < len(row) else "")
    return out

# ================== NORMALIZADOR DE TOPÓNIMOS ==================
NORM = {
    "GARZON": "GARZÓN",
    "SAN AUSTIN": "SAN AGUSTÍN",
    "SAN AGUSTIN": "SAN AGUSTÍN",
    "IBAGUE": "IBAGUÉ",
    "LIBANO": "LÍBANO",
    "COLON": "COLÓN",
    "ORINTO": "ORITO"
}
def norm_name(x: str) -> str:
    x = (x or "").strip().upper()
    return NORM.get(x, x)

# ================== MAIN ==================
def main():
    base_payload = load_base_payload(BASE_PAYLOAD_FILE)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(FINAL_HEADERS)

        for dpto, mpios_raw in OBJETIVO.items():
            dpto_norm = norm_name(dpto)
            # deduplicar municipios tras normalizar
            mpios = []
            for m in mpios_raw:
                nm = norm_name(m)
                if nm not in mpios:
                    mpios.append(nm)

            for muni in mpios:
                try:
                    payload = prepare_payload(base_payload, dpto_norm, muni)
                    res = _post(payload)
                    data = res["results"][0]["result"]["data"]
                    names = descriptor_names(data)
                    ds = get_ds(data)

                    rows = try_dm0_rows(ds, names)
                    if rows is None:
                        rows = decode_rw_rows(ds, names)

                    if not rows:
                        print(f"[WARN] {dpto_norm} / {muni}: sin filas (revisa payload_base y el visual).")
                        continue

                    idx_map = build_index(names)
                    count = 0
                    for r in rows:
                        out_row = project_row(r, names, idx_map)
                        # rellenar dpto/muni si no vinieron en la respuesta
                        if not out_row[0]:
                            out_row[0] = dpto_norm
                        if not out_row[1]:
                            out_row[1] = muni
                        w.writerow(out_row)
                        count += 1
                    print(f"[OK] {dpto_norm} / {muni}: {count} fila(s)")

                except Exception as e:
                    print(f"[ERROR] {dpto_norm} / {muni}: {e}")
                    # dump rápido para depurar
                    try:
                        with open(f"dump_{dpto_norm}_{muni}.json", "w", encoding="utf-8") as fd:
                            json.dump(res, fd, ensure_ascii=False, indent=2)
                            print(f"  -> dump_{dpto_norm}_{muni}.json guardado")
                    except Exception:
                        pass

    print(f"\nCSV generado: {OUT_CSV}")

if __name__ == "__main__":
    main()
