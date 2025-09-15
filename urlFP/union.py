from pathlib import Path
import pandas as pd
import unicodedata

# ==== CONFIGURA AQUÍ ====
INPUT_DIR   = r"C:\Users\carlo\OneDrive\Escritorio\TrabajoURL\fontur_lote-1_2025_ws-ARS\urlFP\input"
OUTPUT_CSV  = r"C:\Users\carlo\OneDrive\Escritorio\TrabajoURL\fontur_lote-1_2025_ws-ARS\urlFP\output\unificado_por_departamento.csv"
OUTPUT_XLSX = r"C:\Users\carlo\OneDrive\Escritorio\TrabajoURL\fontur_lote-1_2025_ws-ARS\urlFP\output\unificado_por_departamento.xlsx"

# nombres posibles para la columna de departamento
POSSIBLE_DEPT_COLS = ["departamento", "depto", "dpto", "Departamento", "DEPARTAMENTO"]

# si una fila sigue sin departamento, así la marcamos para ordenar/hojas
SIN_DEP_LABEL = "Sin departamento"
# ========================

def _norm(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()

def find_dept_col(columns):
    low = {c.lower(): c for c in columns}
    for name in POSSIBLE_DEPT_COLS:
        if name.lower() in low:
            return low[name.lower()]
    for c in columns:
        if "depart" in c.lower() or c.lower() in ("dep","dept","dpt"):
            return c
    return None

def read_csv_safe(path: Path) -> pd.DataFrame:
    # Lectura robusta (autodetecta separador). NO se deduplica ni filtra.
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig", sep=None, engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding="latin-1", sep=None, engine="python")

def main():
    in_dir = Path(INPUT_DIR)
    files = sorted([p for p in in_dir.glob("*.csv") if p.is_file()])
    if not files:
        raise SystemExit(f"No se encontraron CSV en: {INPUT_DIR}")

    dfs, all_cols = [], set()
    diag = []  # diagnóstico por archivo

    for f in files:
        df = read_csv_safe(f)
        diag.append({"archivo": f.name, "filas_leidas": len(df)})

        if df.empty:
            continue

        # Garante la columna 'departamento' (no descartamos nada)
        col_dep = find_dept_col(df.columns)
        if col_dep and col_dep != "departamento":
            df = df.rename(columns={col_dep: "departamento"})
        if "departamento" not in df.columns:
            df["departamento"] = None  # no perder filas: se marcarán como "Sin departamento"

        # Trazabilidad
        if "archivo_origen" not in df.columns:
            df["archivo_origen"] = f.name

        all_cols.update(df.columns)
        dfs.append(df)

    if not dfs:
        raise SystemExit("No hay data para unificar.")

    # Unimos sin perder columnas: superconjunto de todas
    ordered_cols = ["departamento"] + sorted([c for c in all_cols if c != "departamento"])
    aligned = [d.reindex(columns=ordered_cols) for d in dfs]
    master = pd.concat(aligned, ignore_index=True)

    # Organizar por departamento:
    # - crear columna auxiliar para ordenar y para hojas de Excel
    master["departamento_aux"] = master["departamento"].fillna("").astype(str).str.strip()
    master.loc[master["departamento_aux"] == "", "departamento_aux"] = SIN_DEP_LABEL

    # Orden estable por departamento (y opcionalmente por archivo y título si existen)
    sort_keys = ["departamento_aux"]
    for k in ["archivo_origen", "titulo"]:
        if k in master.columns:
            sort_keys.append(k)
    master.sort_values(by=sort_keys, kind="stable", inplace=True)

    # Exportar CSV (sin tocar datos; valores faltantes se quedan vacíos)
    Path(OUTPUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    master.drop(columns=["departamento_aux"], errors="ignore").to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    # Exportar Excel: MASTER + una hoja por departamento (incluye “Sin departamento” si aplica)
    Path(OUTPUT_XLSX).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as xw:
        master.drop(columns=["departamento_aux"], errors="ignore").to_excel(xw, index=False, sheet_name="MASTER")
        for dpto in master["departamento_aux"].dropna().unique():
            df_d = master[master["departamento_aux"] == dpto].drop(columns=["departamento_aux"], errors="ignore")
            df_d.to_excel(xw, index=False, sheet_name=str(dpto)[:31])

    # Diagnóstico en consola
    total_fuentes = sum(x["filas_leidas"] for x in diag)
    print("=== DIAGNÓSTICO DE FILAS ===")
    for x in diag:
        print(f"{x['archivo']}: {x['filas_leidas']} filas")
    print(f"TOTAL LEÍDO (suma de todos los CSV): {total_fuentes}")
    print(f"TOTAL EN CONSOLIDADO: {len(master)}")  # debe coincidir con total_fuentes
    if len(master) != total_fuentes:
        print("⚠ Diferencia detectada. Revisa si algún CSV está vacío o si el lector no pudo parsear filas.")
    print(f"CSV:   {OUTPUT_CSV}")
    print(f"Excel: {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
