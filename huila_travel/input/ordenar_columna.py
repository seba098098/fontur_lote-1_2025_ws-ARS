import pandas as pd

# Ruta de tu archivo actual
ruta = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\turismo_huila\inventario_huila_detallado.csv"

# Leer el CSV
df = pd.read_csv(ruta)

# Definir el nuevo orden de columnas
nuevo_orden = [
    "Departamento",
    "Municipio",
    "Nombre",
    "Link",
    "Vereda",
    "Direccion",
    "Codigo",
    "Responsable",
    "Telefono",
    "EstadoConservacion",
    "ConstitucionBien",
    "Representatividad",
    "Ubicación",     # la que venía del scraping inicial
    "Descripción",   # la que venía del scraping inicial
    "DescripcionCompleta"
]

# Reordenar columnas (solo las que existan en el DataFrame)
columnas_existentes = [col for col in nuevo_orden if col in df.columns]
df = df[columnas_existentes]

# Guardar en un nuevo archivo
ruta_salida = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\turismo_huila\inventario_huila_final.csv"
df.to_csv(ruta_salida, index=False, encoding="utf-8")

print("✅ Archivo reorganizado y guardado en inventario_huila_final.csv")
