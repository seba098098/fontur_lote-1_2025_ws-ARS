# unir_huila_solo_csv.py
import os
import re
import unicodedata
import pandas as pd

# ---- RUTAS DE ENTRADA ----
RUTA_GOB = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\acotur_huila_competitiva\input\Lugares_Huila.csv"
RUTA_ACOTUR = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\acotur_huila_competitiva\input\HUILA.csv"

# ---- SALIDA (SOLO CSV) ----
CARPETA_SALIDA = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\acotur_huila_competitiva\output"
os.makedirs(CARPETA_SALIDA, exist_ok=True)
CSV_SALIDA = os.path.join(CARPETA_SALIDA, "huila.csv")

def leer_csv_robusto(path):
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def quitar_tildes(s):
    if pd.isna(s): return s
    s = str(s)
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def normalizar_texto(s, titulo=False):
    if pd.isna(s): return None
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" ,", ",")
    s = s.replace("’", "'").replace("`", "'")
    return s.title() if titulo else s

def normalizar_municipio(s):
    if pd.isna(s): return None
    s_raw = normalizar_texto(s, titulo=True)
    s_base = quitar_tildes(s_raw).lower()
    mapping = {
        "san agustin": "San Agustín",
        "nataga": "Nátaga",
        "iquira": "Íquira",
        "elias": "Elías",
        "santa maria": "Santa María",
        "yaguara": "Yaguará",
        "garzon": "Garzón",
        "hobó": "Hobo",
    }
    return mapping.get(s_base, s_raw)

def estandarizar_departamento(s):
    if pd.isna(s): return None
    s = normalizar_texto(s).upper()
    return "Huila" if quitar_tildes(s) == "HUILA" else s.title()

def armar_esquema_gob(df):
    return pd.DataFrame({
        "fuente": "gobernacion",
        "nombre": df.get("Nombre"),
        "municipio": df.get("Municipio"),
        "departamento": df.get("Departamento"),
        "categoria": df.get("Categoria"),
        "subtipo": None,
        "rnt": None,
        "descripcion": df.get("Info"),
        "certificaciones": None,
        "email": None,
        "redes_sociales": None,
        "url": df.get("URL"),
        "decreto": df.get("Decreto"),
    })

def armar_esquema_acotur(df):
    return pd.DataFrame({
        "fuente": "acotur",
        "nombre": df.get("Nombre"),
        "municipio": df.get("Municipio"),
        "departamento": df.get("Departamento"),
        "categoria": df.get("Categorías"),
        "subtipo": None,
        "rnt": df.get("RNT"),
        "descripcion": df.get("Descripción"),
        "certificaciones": df.get("Certificaciones"),
        "email": df.get("Email"),
        "redes_sociales": df.get("Redes Sociales"),
        "url": df.get("URL"),
        "decreto": None,
    })

def limpiar_y_normalizar(df):
    for col in ["nombre","municipio","departamento","categoria","subtipo","descripcion",
                "certificaciones","email","redes_sociales","url","decreto"]:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_texto)

    df["municipio"] = df["municipio"].apply(normalizar_municipio)
    df["departamento"] = df["departamento"].apply(estandarizar_departamento)

    if "rnt" in df.columns:
        df["rnt"] = df["rnt"].apply(lambda x: None if pd.isna(x) else (re.sub(r"[^\d]", "", str(x)).strip() or None))

    if "categoria" in df.columns:
        df["categoria"] = df["categoria"].apply(lambda x: ", ".join([c.strip() for c in str(x).split(",")]) if pd.notna(x) else None)
    return df

def main():
    gob = leer_csv_robusto(RUTA_GOB)
    acotur = leer_csv_robusto(RUTA_ACOTUR)

    gob_std = limpiar_y_normalizar(armar_esquema_gob(gob))
    acotur_std = limpiar_y_normalizar(armar_esquema_acotur(acotur))

    unido = pd.concat([gob_std, acotur_std], ignore_index=True)

    unido = unido.drop_duplicates(subset=["nombre","municipio","departamento","url"], keep="first")

    cols = ["fuente","nombre","municipio","departamento","categoria","subtipo","rnt",
            "descripcion","certificaciones","email","redes_sociales","url","decreto"]
    unido = unido[cols].sort_values(by=["municipio","nombre"], na_position="last")

    unido.to_csv(CSV_SALIDA, index=False, encoding="utf-8-sig")
    print(f"[OK] CSV combinado: {CSV_SALIDA}")

if __name__ == "__main__":
    main()
