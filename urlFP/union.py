# ==== CONFIGURA AQUÍ ====
INPUT_DIR  = r"C:\Users\carlo\OneDrive\Escritorio\TrabajoURL\urlFP\--input"   # carpeta con TODOS los CSV
OUTPUT_DIR = r"C:\Users\carlo\OneDrive\Escritorio\TrabajoURL\urlFP\output" # carpeta de salida

# Modo NO DESTRUCTIVO (no filtra ni deduplica por defecto)
APLICAR_FILTRO_DEPARTAMENTOS = False
DEPARTAMENTOS_OBJ = ["Cauca", "Putumayo", "Huila", "Caquetá"]  # usado solo si activas el filtro
DEDUP_MODE = "none"  # opciones: "none", "by_enlace", "by_titulo_ubicacion_depto"
# ========================

from pathlib import Path
import pandas as pd
import re, os
from urllib.parse import urlparse

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Renombres suaves de sinónimos -> canónicos (no limita columnas; conservamos todo lo demás)
EQUIV = {
    "title":"titulo","nombre":"titulo","name":"titulo",
    "category":"categoria","tipo_de_contenido":"categoria",
    "description":"descripcion","resumen":"descripcion","detalle":"detalles",
    "url":"enlace","link":"enlace","href":"enlace",
    "location":"ubicacion","ubicación":"ubicacion","lugar":"ubicacion","direccion":"ubicacion",
    "type":"tipo","clase":"tipo",
    "facebook":"facebook","instagram":"instagram","tiktok":"tiktok","youtube":"youtube","youtub":"youtube",
    "rrss":"redes","social":"redes","redes":"redes",
    "observaciones":"detalles","info":"detalles",
    "ciudad":"municipio","localidad":"municipio","vereda":"municipio",
    "depto":"departamento",
    "source":"fuente","portal":"fuente",
    "_list_url":"list_url","listurl":"list_url",
    # Normalizamos nombres que luego ELIMINAREMOS a petición tuya
    "fecha":"fecha_publicacion","fecha_pub":"fecha_publicacion","publicacion":"fecha_publicacion",
    "fecha_actualizada":"fecha_actualizacion","act":"fecha_actualizacion",
    "scrape_date":"fecha_extraccion","extraccion":"fecha_extraccion","fecha_extraccion":"fecha_extraccion","fecha_extracción":"fecha_extraccion",
    "image":"imagen","imagen_url":"imagen","thumbnail":"imagen",
    "price":"precio","valor":"precio","tarifa":"precio","coste":"precio",
    "phone":"telefono","teléfono":"telefono","movil":"celular","mobile":"celular","cel":"celular",
    "correo":"email","correo_electronico":"email","mail":"email",
    "hora":"horario"
}

# Columnas a eliminar exactamente (por solicitud)
DROP_EXACT = {
    "imagen","tipo","precio","telefono","celular","whatsapp","email",
    "fecha_publicacion","fecha_actualizacion","fecha_extraccion","horario","origen_archivo"
}
# Y además eliminar cualquier columna cuyo nombre contenga 'dalle'
DROP_IF_CONTAINS = ["dalle"]

# Orden amigable al inicio (si existen); el resto se conserva detrás
PREFERRED_ORDER = [
    "titulo","categoria","descripcion","enlace","ubicacion","detalles",
    "municipio","departamento","departamento_detectado","departamento_final",
    "instagram","facebook","tiktok","youtube","redes","fuente","list_url"
]

def _norm_col(col: str) -> str:
    c = col.strip().lower()
    c = (c.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
           .replace("ï","i").replace("ö","o").replace("ü","u"))
    return EQUIV.get(c, c)

def _strip_or_none(x):
    # Robusto: maneja Series (cuando había duplicadas) y valores sueltos
    if isinstance(x, pd.Series):
        x = x.dropna()
        if x.empty: return None
        for val in x:
            s = str(val).strip()
            if s: return s
        return None
    try:
        if pd.isna(x): return None
    except Exception:
        pass
    s = str(x).strip()
    return s if s else None

def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fusiona columnas duplicadas con coalesce fila a fila (primer valor no nulo/útil),
    trabajando SIEMPRE con Series para evitar errores de .str sobre DataFrame.
    """
    cols = list(df.columns)
    name_to_positions = {}
    for i, c in enumerate(cols):
        name_to_positions.setdefault(c, []).append(i)

    for name, idxs in name_to_positions.items():
        if len(idxs) <= 1:
            continue
        # base = primera aparición como Serie
        base = df.iloc[:, idxs[0]].copy()
        base = base.where(~base.astype(str).str.strip().eq(""), pd.NA)

        # Coalesce con el resto de apariciones
        for j in idxs[1:]:
            col = df.iloc[:, j]
            col = col.where(~col.astype(str).str.strip().eq(""), pd.NA)
            base = base.combine_first(col)

        # Escribe el resultado en la PRIMERA columna y elimina el resto
        df.iloc[:, idxs[0]] = base
        for j in sorted(idxs[1:], reverse=True):
            df.drop(df.columns[j], axis=1, inplace=True)

    return df

def _std_depto(texto: str):
    if not isinstance(texto, str) or not texto.strip():
        return None
    s = texto.lower()
    s = s.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
    s = re.sub(r"departamento\s+del?\s+", "", s)
    if "cauca"   in s: return "Cauca"
    if "putumayo" in s: return "Putumayo"
    if "huila"    in s: return "Huila"
    if "caqueta"  in s: return "Caquetá"
    return None

def _infer_depto_row(row):
    # NO pisa 'departamento' original; solo sugiere 'departamento_detectado'
    for col in ("departamento","ubicacion","descripcion","detalles","titulo"):
        val = row.get(col)
        if pd.notna(val):
            dd = _std_depto(str(val))
            if dd: return dd
    return None

def _pick_category(texto: str):
    if not isinstance(texto, str): return None
    s = texto.lower()
    if "hotel" in s or "aloj" in s: return "alojamiento"
    if "rest" in s or "gastr" in s: return "gastronomia"
    if "tour" in s or "plan" in s or "experienc" in s: return "producto_turistico"
    if "evento" in s or "festival" in s or "feria" in s: return "evento"
    if "parque" in s or "reserva" in s or "natural" in s: return "recurso_natural"
    if "museo" in s or "monumento" in s or "patrimonio" in s: return "recurso_cultural"
    return None

def _unificar_redes_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Crea columna 'redes' combinando instagram/facebook/tiktok/youtube/redes (conservando originales)
    redes_cols = [c for c in df.columns if c in ["instagram","facebook","tiktok","youtube","redes"]]
    if not redes_cols:
        df["redes"] = pd.NA
        return df
    def _join(row):
        vals = []
        for c in redes_cols:
            v = row.get(c)
            if pd.notna(v) and str(v).strip():
                vals.append(str(v).strip())
        if not vals: return None
        seen, out = set(), []
        for part in ";".join(vals).replace(",", ";").split(";"):
            p = part.strip()
            if p and p.lower() not in seen:
                seen.add(p.lower()); out.append(p)
        return "; ".join(out) if out else None
    df["redes"] = df.apply(_join, axis=1)
    return df

def _dominio(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except:
        return ""

def _asigna_fuente(row, nombre_archivo: str):
    # Si ya hay 'fuente' no vacía, respétala
    f = row.get("fuente")
    if isinstance(f, str) and f.strip():
        fs = f.strip().lower()
        if "fontur" in fs: return "Fontur"
        if "procolombia" in fs or "colombia.travel" in fs: return "ProColombia"
        return f
    # Inferir por nombre de archivo
    base = nombre_archivo.lower()
    if "fontur" in base: return "Fontur"
    if "procolombia" in base: return "ProColombia"
    # Inferir por dominio del enlace
    enlace = row.get("enlace")
    if isinstance(enlace, str) and enlace.strip():
        dom = _dominio(enlace)
        if "fontur.com.co" in dom: return "Fontur"
        if "procolombia.co" in dom or "colombia.travel" in dom: return "ProColombia"
    return None

def _leer_csv(path: Path) -> pd.DataFrame:
    """
    Lee CSV detectando separador.
    - engine='python' SIN low_memory (autodetección de separador).
    - Si falla, reintenta con engine='c' y separadores comunes.
    """
    try:
        return pd.read_csv(
            path,
            dtype=str,
            encoding="utf-8",
            sep=None,          # autodetecta
            engine="python",
            # on_bad_lines="skip",  # activa SOLO si hay filas corruptas y quieres continuar
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            path,
            dtype=str,
            encoding="latin-1",
            sep=None,
            engine="python",
            # on_bad_lines="skip",
        )
    except Exception as e1:
        for enc in ("utf-8", "latin-1"):
            for sep in (",", ";", "\t", "|"):
                try:
                    return pd.read_csv(
                        path,
                        dtype=str,
                        encoding=enc,
                        sep=sep,
                        engine="c",
                        low_memory=False,  # aquí sí aplica con 'c'
                        # on_bad_lines="skip",
                    )
                except Exception:
                    continue
        print(f"[ADVERTENCIA] No se pudo leer {path.name}: {e1}")
        return pd.DataFrame()

def leer_unir_normalizar(carpeta: str) -> pd.DataFrame:
    archivos = sorted(Path(carpeta).rglob("*.csv"))
    if not archivos:
        raise SystemExit(f"No se encontraron CSV en: {carpeta}")

    frames = []
    for p in archivos:
        df = _leer_csv(p)
        if df.empty:
            continue

        # 1) Renombrar columnas conocidas; conservar el resto
        df = df.rename(columns={c: _norm_col(c) for c in df.columns})
        # 2) Colapsar duplicadas (coalesce)
        df = _coalesce_duplicate_columns(df)

        # 3) Limpieza mínima solo en campos clave (sin tocar el resto)
        for c in [col for col in ["titulo","categoria","descripcion","enlace","ubicacion","detalles","municipio","departamento","instagram","facebook","tiktok","youtube","redes","list_url","fuente"] if col in df.columns]:
            df[c] = df[c].apply(_strip_or_none)

        # 4) Categoría si falta (no pisa si ya existe)
        if "categoria" not in df.columns or df["categoria"].isna().all():
            df["categoria"] = df.apply(lambda r: _pick_category((r.get("descripcion") or r.get("titulo") or "")), axis=1)
        else:
            df["categoria"] = df.apply(lambda r: r["categoria"] if pd.notna(r.get("categoria")) and str(r.get("categoria")).strip() else _pick_category((r.get("descripcion") or r.get("titulo") or "")), axis=1)

        # 5) Detectar departamento (no pisa el original)
        df["departamento_detectado"] = df.apply(_infer_depto_row, axis=1)
        if "departamento" not in df.columns:
            df["departamento"] = pd.NA
        df["departamento_final"] = df.apply(
            lambda r: r["departamento"] if pd.notna(r.get("departamento")) and str(r.get("departamento")).strip()
                      else r.get("departamento_detectado"),
            axis=1
        )

        # 6) Unificar redes -> 'redes' (conservando originales)
        df = _unificar_redes_cols(df)

        # 7) Asignar/normalizar fuente (sin pisar si ya venía)
        df["fuente"] = df.apply(lambda r: _asigna_fuente(r, os.path.basename(p)), axis=1)

        # 8) Eliminar SOLO lo pedido + columnas que contienen 'dalle'
        drop_cols = [c for c in df.columns if c in DROP_EXACT]
        drop_cols += [c for c in df.columns if any(tok in c.lower() for tok in DROP_IF_CONTAINS)]
        if drop_cols:
            df.drop(columns=list(set(drop_cols)), inplace=True, errors="ignore")

        # 9) Reordenar: primero preferidas (si existen), luego el resto
        preferred_existing = [c for c in PREFERRED_ORDER if c in df.columns]
        others = [c for c in df.columns if c not in preferred_existing]
        df = df[preferred_existing + others]

        frames.append(df)

    master = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if master.empty:
        return master

    # 10) Marcar duplicados (NO borrar)
    if "enlace" in master.columns:
        master["es_duplicado_enlace"] = master.duplicated(subset=["enlace"], keep="first")
    if all(c in master.columns for c in ["titulo","ubicacion","departamento_final"]):
        master["es_duplicado_titulo_ubicacion_depto"] = master.duplicated(subset=["titulo","ubicacion","departamento_final"], keep="first")

    # 11) Deduplicación efectiva (opcional)
    if DEDUP_MODE == "by_enlace" and "enlace" in master.columns:
        master = master.drop_duplicates(subset=["enlace"], keep="first", ignore_index=True)
    elif DEDUP_MODE == "by_titulo_ubicacion_depto" and all(c in master.columns for c in ["titulo","ubicacion","departamento_final"]):
        master = master.drop_duplicates(subset=["titulo","ubicacion","departamento_final"], keep="first", ignore_index=True)

    # 12) Filtro por departamentos (opcional; desactivado por defecto)
    if APLICAR_FILTRO_DEPARTAMENTOS and "departamento_final" in master.columns:
        master = master[master["departamento_final"].isin(DEPARTAMENTOS_OBJ)].copy()

    return master

# Ejecutar
master = leer_unir_normalizar(INPUT_DIR)

# Salidas
if master.empty:
    raise SystemExit("No se pudo construir el consolidado (¿CSV vacíos o ilegibles?).")

master_csv  = Path(OUTPUT_DIR) / "master_consolidado.csv"
excel_path  = Path(OUTPUT_DIR) / "consolidado_por_departamento.xlsx"

master.to_csv(master_csv, index=False, encoding="utf-8-sig")

with pd.ExcelWriter(excel_path, engine="openpyxl") as xw:
    master.to_excel(xw, index=False, sheet_name="MASTER")
    if "departamento_final" in master.columns:
        for d in sorted([d for d in master["departamento_final"].dropna().unique()]):
            df_d = master[master["departamento_final"] == d]
            if not df_d.empty:
                df_d.to_excel(xw, index=False, sheet_name=str(d)[:31])

print("✔ Consolidado listo (no destructivo)")
print(f" - CSV maestro: {master_csv}")
print(f" - Excel por departamento: {excel_path}")
