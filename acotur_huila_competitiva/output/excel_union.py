import pandas as pd
import os

def unir_csv_en_excel(nombres_csv, nombre_salida="salida.xlsx"):
    """
    Une 4 archivos CSV en un único Excel con las hojas:
    Caquetá, Huila, Putumayo y Tolima (en ese orden).
    
    :param nombres_csv: Lista con los 4 nombres de archivo CSV en el orden correcto.
    :param nombre_salida: Nombre del archivo Excel de salida.
    """
    carpeta = os.getcwd()  # Carpeta actual

    hojas = ["Caquetá", "Huila", "Putumayo", "Tolima"] # dejar esto quieto solo cambiar el archivo 

    with pd.ExcelWriter(os.path.join(carpeta, nombre_salida), engine="openpyxl") as writer:
        for hoja, archivo in zip(hojas, nombres_csv):
            ruta = os.path.join(carpeta, archivo)
            df = pd.read_csv(ruta)
            df.to_excel(writer, sheet_name=hoja, index=False)
            print(f"[OK] {archivo} -> hoja '{hoja}'")

# ============================
# Ejemplo de uso con este orden de departamentos
# ============================
archivos = [
    "CAQUETA.csv",
    "huila.csv",
    "PUTUMAYO.csv",
    "TOLIMA.csv"
]

unir_csv_en_excel(archivos, nombre_salida="excel/portuColombia_final.xlsx")
