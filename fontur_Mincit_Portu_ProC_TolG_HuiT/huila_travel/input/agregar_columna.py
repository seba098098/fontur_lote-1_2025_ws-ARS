import pandas as pd

# Ruta del archivo existente
ruta = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\turismo_huila\inventario_huila_detallado.csv"

# Leer el CSV
df = pd.read_csv(ruta)

# Agregar la columna 'Departamento' con valor fijo 'Huila'
df["Departamento"] = "Huila"

# Guardar el archivo actualizado
df.to_csv(ruta, index=False, encoding="utf-8")

print("✅ Columna 'Departamento' agregada con valor 'Huila' en todas las filas.")
