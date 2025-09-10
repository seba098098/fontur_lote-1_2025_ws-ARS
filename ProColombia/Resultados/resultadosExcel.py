import pandas as pd
import os
import json

def leer_archivo(ruta):
    """Lee un archivo CSV o JSON dependiendo de su extensión."""
    _, ext = os.path.splitext(ruta)
    if ext.lower() == '.csv':
        return pd.read_csv(ruta)
    elif ext.lower() == '.json':
        with open(ruta, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return pd.json_normalize(data)  # Normaliza el JSON en formato de tabla
    else:
        raise ValueError(f"Archivo no soportado: {ext}")

def unir_archivos_en_excel(nombres_archivos, nombre_salida="salida.xlsx"):
    """
    Une 4 archivos (CSV o JSON) en un único Excel con las hojas:
    Caquetá, Huila, Putumayo y Tolima (en ese orden).
    
    :param nombres_archivos: Lista con los 4 nombres de archivo en el orden correcto (CSV o JSON).
    :param nombre_salida: Nombre del archivo Excel de salida.
    """
    carpeta = os.getcwd()  # Carpeta actual

    hojas = ["Caquetá", "Huila", "Putumayo", "Tolima"]

    with pd.ExcelWriter(os.path.join(carpeta, nombre_salida), engine="openpyxl") as writer:
        for hoja, archivo in zip(hojas, nombres_archivos):
            ruta = os.path.join(carpeta, archivo)
            try:
                df = leer_archivo(ruta)
                df.to_excel(writer, sheet_name=hoja, index=False)
                print(f"[OK] {archivo} -> hoja '{hoja}'")
            except Exception as e:
                print(f"[ERROR] No se pudo procesar {archivo}: {e}")

# ============================
# Ejemplo de uso
# ============================
archivos = [
    "turismo_caqueta_completo_detalles.csv",      # o "turismo_caqueta_completo_detalles.json"
    "turismo_huila_completo_detalles.csv",        # o "turismo_huila_completo_detalles.json"
    "turismo_putumayo_completo_detalles.csv",     # o "turismo_putumayo_completo_detalles.json"
    "turismo_tolima_completo_detalles.csv"        # o "turismo_tolima_completo_detalles.json"
]

unir_archivos_en_excel(archivos, nombre_salida="excel/ProColombia_final.xlsx")
