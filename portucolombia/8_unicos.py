# unicos_unificado.py
import os, io, argparse
from pathlib import Path
import pandas as pd
import numpy as np

# -------- Config --------
BASE_DIR = Path(__file__).resolve().parent
#DEFAULT_NAME = "4_rnt_activos_por_categoria.csv"
DEFAULT_NAME = "output/unido_fact.csv"
OUT_DIR  = BASE_DIR / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "valores_unicos.csv"

# Columnas a listar (en el orden que pediste)
COLUMNS = [
    "departamento","municipio","anio","mes","indicador","valor_num",
    "pais_origen","atractivo","categoria_grupo","categoria","colegio",
    "colegio","razon_social","direccion","rnt"
]

# -------- Helpers --------
def find_latest_fact(base: Path, name=DEFAULT_NAME):
    # Busca en output/** primero; luego en la raíz
    candidates = list((base / "output").rglob(name))
    if not candidates and (base / name).exists():
        candidates = [base / name]
    if not candidates:
        candidates = sorted((base / "output").rglob("fact_largo*.csv"),
                             key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def read_csv_robusto(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig","utf-8","latin-1","cp1252"]
    for enc in encodings:
        try:
            return pd.read_csv(path, dtype=str, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    # Fallback latin-1
    with open(path, "rb") as f:
        data = f.read().decode("latin-1", errors="ignore")
    return pd.read_csv(io.StringIO(data), dtype=str, sep=None, engine="python")

def sort_smart(values):
    """Ordena numéricamente si puede, de lo contrario alfabéticamente."""
    vals = [v for v in values if v is not None]
    try:
        nums = [float(str(x).replace(",",".")) for x in vals]
        order = np.argsort(nums)
        vals = [vals[i] for i in order]
    except Exception:
        vals = sorted(vals, key=lambda x: (x is None, str(x)))
    return vals

# -------- CLI --------
ap = argparse.ArgumentParser(description="Genera un SOLO archivo con los valores únicos de columnas del FACT.")
ap.add_argument("--csv", type=str, default=None, help="Ruta al fact_largo_portucolombia.csv (opcional)")
args = ap.parse_args()

if args.csv:
    csv_path = Path(args.csv)
else:
    csv_path = find_latest_fact(BASE_DIR)

if not csv_path or not csv_path.exists():
    print("❌ No se encontró el CSV del FACT.")
    print(f"• Buscado: output\\**\\{DEFAULT_NAME} y .\\{DEFAULT_NAME}")
    print('• Usa: python unicos_unificado.py --csv "output\\YYYYMMDD_HHMMSS\\fact_largo_portucolombia.csv"')
    raise SystemExit(1)

print(f"✅ Leyendo: {csv_path}")
df = read_csv_robusto(csv_path)

# -------- Armar un solo archivo con todos los únicos --------
rows = []
for col in COLUMNS:
    if col not in df.columns:
        print(f"[AVISO] Columna no encontrada y se omite: {col}")
        continue
    s = df[col].astype(str)
    # limpiar vacíos a NaN y excluirlos
    s = s.apply(lambda x: x.strip()).replace({"": np.nan})
    uniques = s.dropna().unique().tolist()
    uniques = sort_smart(uniques)
    for v in uniques:
        rows.append({"columna": col, "valor": v})

out = pd.DataFrame(rows, columns=["columna","valor"])

# Guardar un único archivo
out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
print(f"📄 Únicos guardados en: {OUT_CSV}")
print("   (Columnas: columna, valor) — sin archivos adicionales.")
