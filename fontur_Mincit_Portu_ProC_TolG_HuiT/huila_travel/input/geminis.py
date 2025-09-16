import pandas as pd 
import google.generativeai as genai
import json
import re
import time

# Configura tu API Key
genai.configure(api_key="AIzaSyCWSewUpa3JGNcsvkGRcNR03Sl_0CDeemg")

# Modelos a probar
modelos = ["gemini-1.5-flash", "gemini-1.5-pro","gemini-2.5-pro","gemini-2.5-flash","gemini-2.5-flash-lite","gemini-2.5-flash-image-preview"]

# Archivo original
ruta = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\turismo_huila\inventario_huila_final.csv"
df = pd.read_csv(ruta)

# Archivo de salida
ruta_salida = r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\turismo_huila\inventario_huila_gemini.csv"

# Lista para almacenar resultados
datos_limpios = []

def limpiar_json(texto):
    """Limpia el texto de Gemini y devuelve JSON válido si es posible"""
    texto = re.sub(r"```(json)?", "", texto).strip()
    start = texto.find("{")
    end = texto.rfind("}")
    if start != -1 and end != -1:
        texto = texto[start:end+1]
    return json.loads(texto)

for idx, row in df.iterrows():
    texto_prompt = f"""
    A partir del siguiente texto turístico, clasifica y organiza los campos en formato JSON.
    Texto: {row.get('DescripcionCompleta', '')}

    Devuelve SOLO en JSON válido (en este orden, sin incluir ninguna otra clave), sin explicaciones ni markdown.
    Campos:
    Departamento (fijo "Huila"),
    Municipio,
    enfoque_turistico (elige uno o varios entre: Cultura, Aventura, Naturaleza, Salud y Bienestar),
    aspectos (elige uno o varios entre: Infraestructura, Conectividad, Planta turistica, Atractivo, Servicios, Condiciones basicas, Sostenibilidad).
    Nombre,
    Vereda,
    Direccion,
    Codigo,
    Responsable,
    Telefono,
    EstadoConservacion,
    ConstitucionBien,
    Representatividad,
    Descripcion (texto breve y limpio),
    Link,
    Link: {row.get('Link', '')}
    """

    procesado = False

    for modelo in modelos:
        try:
            model = genai.GenerativeModel(modelo)
            respuesta = model.generate_content(texto_prompt)

            limpio = limpiar_json(respuesta.text)
            datos_limpios.append(limpio)

            print(f"✅ Fila {idx+1}/{len(df)} procesada con {modelo}: {limpio.get('Nombre','')}")
            procesado = True
            break  # si funciona con un modelo, no probar más

        except Exception as e:
            print(f"⚠️ Error en fila {idx+1} con modelo {modelo}: {e}")
            time.sleep(2)  # espera antes de intentar otro modelo
            continue

    if not procesado:
        print(f"❌ No se pudo procesar fila {idx+1}. Guardando progreso parcial.")

    # 🔹 Guardado progresivo cada 5 filas o si hay error
    if (idx + 1) % 5 == 0 or not procesado:
        df_temp = pd.DataFrame(datos_limpios)
        df_temp.to_csv(ruta_salida, index=False, encoding="utf-8")
        print(f"💾 Progreso guardado hasta la fila {idx+1}")

# Guardar al final
df_final = pd.DataFrame(datos_limpios)
df_final.to_csv(ruta_salida, index=False, encoding="utf-8")
print("🎉 Proceso terminado. Archivo limpio guardado en inventario_huila_gemini.csv")
