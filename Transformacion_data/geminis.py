import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

# pip install google-generativeai
import google.generativeai as genai

# ===================== Config =====================
RUTA_JSON = Path(r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\Fontur\Resultados\fontur_caqueta_2019plus_20250909-051445.json")
GEMINI_MODEL = "gemini-2.0-flash-lite"   # mayor calidad para extracción
MAX_RETRIES = 3
SLEEP_BETWEEN_RETRIES = 2.5  # seg (se aplica backoff exponencial)
SALIDA_JSON = Path(r"D:\fontur_lote-1_2025_ws-ARS\scrapeo\Transformacion_data\fontur\fontur_caqueta_unificado.json")


# 🔐 PON AQUÍ TU API KEY (evita subir este archivo al repo)
API_KEY_GEMINI = "AIzaSyCJzYy1CJavEdNaRiosj3paJB3WWwsDCi8"

# Campos exactos solicitados
CAMPOS = [ 
  "departamento","municipio","cod_dane_depto","cod_dane_mpio",
  "categoria","subcategoria","tipo_producto","estado_operacion",
  "razon_social_nombre_comercial","info_de_razon_social","nombre_atractivo","explicacion",
  "direccion","barrio_vereda","telefono","email",
  "sitio_web","redes_facebook","redes_instagram","redes_whatsapp",
  "tiene_sitio_web","tiene_facebook","tiene_instagram","tiene_whatsapp",
  "registro_completo",
  "tipo_fuente","coordenadas","precio_referencia","horario","etiquetas",
  "fecha_extraccion"
]

ESTADO_OPERACION = ["activo", "cerrado", "en remodelación", "temporal"]
# ===================== Few-shots (guían el formato y el uso de null) =====================
FEW_SHOTS = [ 
    {
        "titulo": "Hotel Sol de Oriente en Florencia anuncia renovación",
        "detalles": "El Hotel Sol de Oriente (Florencia, Caquetá) informó mejoras en habitaciones. Teléfono: (8) 1234567. Sitio web: https://soloriente.co",
        "salida": {
            "departamento": "Caquetá", "municipio": "Florencia",
            "cod_dane_depto": "18", "cod_dane_mpio": "18001",
            "categoria": "Alojamiento", "subcategoria": "Hotel", "tipo_producto": "Hotel",
            "estado_operacion": "activo",
            "razon_social_nombre_comercial": "Hotel Sol de Oriente",
            "info_de_razon_social": "Establecimiento hotelero local en Florencia",
            "nombre_atractivo": "Hotel Sol de Oriente",
            "explicacion": "Hotel en Florencia que anunció renovación de habitaciones.",
            "direccion": None, "barrio_vereda": None, "telefono": "(8) 1234567", "email": None,
            "sitio_web": "https://soloriente.co",
            "redes_facebook": None, "redes_instagram": None, "redes_whatsapp": None,
            "tiene_sitio_web": True, "tiene_facebook": False, "tiene_instagram": False, "tiene_whatsapp": False,
            "registro_completo": False,
            "tipo_fuente": "noticia oficial", "coordenadas": None,
            "precio_referencia": None, "horario": None, "etiquetas": ["hotel","turismo","alojamiento"],
            "fecha_extraccion": "2025-09-14"
        }
    },
    {
        "titulo": "Lanzamiento de la Ruta del Río en Mocoa",
        "detalles": "Se invita a la comunidad a conocer el corredor turístico del río. No se publican contactos ni redes.",
        "salida": {
            "departamento": "Putumayo", "municipio": "Mocoa",
            "cod_dane_depto": "86", "cod_dane_mpio": "86001",
            "categoria": "Ruta/Corredor", "subcategoria": None, "tipo_producto": "Corredor turístico",
            "estado_operacion": "activo",
            "razon_social_nombre_comercial": "Ruta del Río",
            "info_de_razon_social": "Proyecto turístico comunitario impulsado por la Alcaldía de Mocoa",
            "nombre_atractivo": "Ruta del Río",
            "explicacion": "Nuevo corredor turístico en Mocoa, promovido como atractivo natural.",
            "direccion": None, "barrio_vereda": None, "telefono": None, "email": None,
            "sitio_web": None,
            "redes_facebook": None, "redes_instagram": None, "redes_whatsapp": None,
            "tiene_sitio_web": False, "tiene_facebook": False, "tiene_instagram": False, "tiene_whatsapp": False,
            "registro_completo": False,
            "tipo_fuente": "nota de prensa", "coordenadas": None,
            "precio_referencia": None, "horario": None, "etiquetas": ["corredor","turismo","naturaleza"],
            "fecha_extraccion": "2025-09-14"
        }
    },
    {
        "titulo": "Museo del Oro de Bogotá amplía horarios",
        "detalles": "El Museo del Oro (Bogotá, Colombia) anunció que ahora abrirá de martes a domingo, de 9:00 am a 6:00 pm. Entrada general $5.000 COP. Sitio web: https://museodeloro.banrep.gov.co",
        "salida": {
            "departamento": "Cundinamarca", "municipio": "Bogotá",
            "cod_dane_depto": "11", "cod_dane_mpio": "11001",
            "categoria": "Cultural", "subcategoria": "Museo", "tipo_producto": "Museo",
            "estado_operacion": "activo",
            "razon_social_nombre_comercial": "Museo del Oro - Banco de la República",
            "info_de_razon_social": "Museo administrado por el Banco de la República de Colombia",
            "nombre_atractivo": "Museo del Oro",
            "explicacion": "Museo en Bogotá que amplió horarios de atención al público.",
            "direccion": None, "barrio_vereda": None, "telefono": None, "email": None,
            "sitio_web": "https://museodeloro.banrep.gov.co",
            "redes_facebook": None, "redes_instagram": None, "redes_whatsapp": None,
            "tiene_sitio_web": True, "tiene_facebook": False, "tiene_instagram": False, "tiene_whatsapp": False,
            "registro_completo": True,
            "tipo_fuente": "fuente oficial", "coordenadas": None,
            "precio_referencia": "5000 COP",
            "horario": "Martes a domingo, 9:00 am - 6:00 pm",
            "etiquetas": ["museo","cultura","turismo"],
            "fecha_extraccion": "2025-09-14"
        }
    }
]


# ===================== Utilidades =====================
def cargar_json_desde_archivo(ruta: Path) -> List[Dict[str, Any]]:
    """Carga un JSON (lista u objeto) desde archivo y siempre retorna lista de dicts."""
    with ruta.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("El JSON debe ser un objeto o una lista de objetos.")

def limpiar_a_json_puro(texto: str) -> str:
    """Quita fences ```json ... ``` si las hubiera y retorna solo el JSON."""
    t = texto.strip()
    if t.startswith("```"):
        t = t.strip("`")
        parts = t.split("\n", 1)
        if len(parts) == 2:
            t = parts[1]
        t = t.strip()
    if t.endswith("```"):
        t = t[:-3].strip()
    return t

def asegurar_claves(obj: Dict[str, Any], claves: List[str]) -> Dict[str, Any]:
    """Garantiza que existan todas las claves y elimina extras."""
    for k in claves:
        if k not in obj:
            obj[k] = None
    return {k: obj.get(k, None) for k in claves}

def imprimir_detalle(detalles: str, max_chars: int = 400) -> None:
    """Imprime el 'detalle' con limpieza básica y recorte opcional."""
    limpio = " ".join(detalles.split())
    if len(limpio) > max_chars:
        print(f"Detalle (preview {max_chars} chars):")
        print(limpio[:max_chars] + "...")
    else:
        print("Detalle:")
        print(limpio)

# ===================== Prompt =====================
def _few_shots_text() -> str:
    bloques = []
    for e in FEW_SHOTS:
        bloques.append(
f"""EJEMPLO:
TÍTULO:
{e['titulo']}
DETALLES:
{e['detalles']}
RESPUESTA_JSON:
{json.dumps(e['salida'], ensure_ascii=False)}"""
        )
    return "\n\n".join(bloques)

def construir_prompt(titulo: str, detalles: str) -> str:
    campos = ", ".join(CAMPOS)
    ejemplos = _few_shots_text()
    return f"""
Eres un extractor estricto. Devuelve SOLO un objeto JSON con estas claves EXACTAS:
[{campos}]

📌 REGLAS
1. **Fuentes**: priorizar información proveniente de fuentes oficiales (Ministerios, Fontur, Gobernaciones, Cámaras de Comercio, DANE, Alcaldías, ProColombia, etc.).  
   - Si no existe en la fuente oficial, se pueden usar otras confiables, pero deben quedar claramente sustentadas.  

2. **Prioridad territorial**: dar mayor relevancia y extraer primero datos relacionados con Caquetá, Putumayo, Huila y Tolima, incluso si aparecen tangencialmente.  

3. **Estado de operación**: usar EXCLUSIVAMENTE el catálogo fijo:
   - "activo"
   - "cerrado"
   - "en remodelación"
   - "temporal"

4. **Nulos**: si no hay evidencia directa, registrar el campo con null, pero intenta evitar vacíos usando:
   - Derivación de códigos DANE a partir de departamento y municipio.  
   - Clasificación automática en "categoria", "subcategoria" o "tipo_producto" según lo mencionado (ej. hotel, museo, corredor, atractivo natural).  
   - Si hay varias entidades (ejemplo: hotel, museo y parque en el mismo texto), generar múltiples objetos JSON, uno por cada entidad.o por otra entidades que encuentre   

5. **Booleanos**: usar true o false únicamente cuando la evidencia explícita esté presente.  

6. **Formato de salida**:
   - Responde EXCLUSIVAMENTE con JSON válido.
   - No uses ```json ni bloques de código.
   - No añadas texto adicional, explicaciones ni comentarios.
   - No inventes campos fuera de la lista.
   - Si hay varias entidades, devuelve un array JSON con objetos.

{ejemplos}

CASO A PROCESAR
TÍTULO:
{titulo}

DETALLES:
{detalles}
""".strip()

# ===================== Gemini =====================
def inicializar_gemini():
    """Inicializa el modelo Gemini con la API key en el código."""
    if not API_KEY_GEMINI or API_KEY_GEMINI == "TU_API_KEY_AQUI":
        raise RuntimeError("API_KEY_GEMINI no está configurada. Reemplázala por tu clave real.")
    genai.configure(api_key=API_KEY_GEMINI)
    return genai.GenerativeModel(GEMINI_MODEL)

def llamar_gemini(model, titulo: str, detalles: str) -> List[Dict[str, Any]]:
    """Llama a Gemini y retorna SIEMPRE una lista de objetos JSON normalizados."""
    prompt = construir_prompt(titulo, detalles)
    last_err: Optional[Exception] = None

    for intento in range(1, MAX_RETRIES + 1):
        try:
            resp = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.0,
                    "top_p": 0.9,
                    "top_k": 40,
                    "response_mime_type": "application/json"
                }
            )

            print("=== RESPUESTA CRUDA GEMINI ===")
            print(resp.text)
            print("=== FIN RESPUESTA CRUDA ===")

            if not resp or not resp.text:
                raise RuntimeError("Respuesta vacía del modelo.")

            bruto = resp.text.strip()
            data = json.loads(bruto)

            # 🔥 Aceptar tanto dict como lista
            if isinstance(data, dict):
                return [asegurar_claves(data, CAMPOS)]
            elif isinstance(data, list):
                return [asegurar_claves(d, CAMPOS) for d in data if isinstance(d, dict)]
            else:
                raise ValueError("La respuesta no es un objeto ni un array JSON.")

        except Exception as e:
            last_err = e
            wait = SLEEP_BETWEEN_RETRIES * (2 ** (intento - 1))
            print(f"[Gemini] Error intento {intento}/{MAX_RETRIES}: {e} | Reintentando en {wait:.1f}s")
            if intento < MAX_RETRIES:
                time.sleep(wait)

    raise RuntimeError(f"No se obtuvo JSON válido. Último error: {last_err}")


def unir_json_distintos(original: Dict[str, Any], gemini: Dict[str, Any]) -> Dict[str, Any]:
    """
    Une dos JSON con estructuras distintas:
    - Conserva todo lo del original.
    - Agrega campos del JSON Gemini solo si no son null.
    """
    combinado = original.copy()
    for k, v in gemini.items():
        if v is not None:  # no agregamos null
            combinado[k] = v
    return combinado

def guardar_json_en_archivo(data: Any, archivo: Path) -> None:
    with archivo.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    try:
        model = inicializar_gemini()
    except Exception as e:
        print(f"Error inicializando Gemini: {e}")
        return

    try:
        items = cargar_json_desde_archivo(RUTA_JSON)
    except Exception as e:
        print(f"Error cargando JSON: {e}")
        return

    print(f"Elementos a procesar: {len(items)}\n")

    resultados_finales = []  # 🔥 aquí acumulamos los objetos unidos

    for idx, obj in enumerate(items, start=1):
        titulo = (obj.get("titulo") or "").strip()
        detalles = (obj.get("detalles") or obj.get("descripcion") or "").strip()

        print("=" * 120)
        print(f"[{idx}] ENVIANDO A GEMINI — Título: {titulo if titulo else '(sin título)'}")

        if not (titulo or detalles):
            print("(!) Item sin título y sin detalles. Omitido.")
            continue

        imprimir_detalle(detalles, max_chars=400)

        try:
            resultado_gemini = llamar_gemini(model, titulo, detalles)

            # 🔥 Ahora resultado_gemini siempre es una lista de dicts
            for res in resultado_gemini:
                combinado = unir_json_distintos(obj, res)
                resultados_finales.append(combinado)

                print("JSON combinado (Original + Gemini):")
                print(json.dumps(combinado, ensure_ascii=False, indent=2))

        except Exception as e:
            print(f"(!) Falló la extracción con Gemini para el item {idx}: {e}")

    # 5) Guardar todo el resultado unificado en un archivo nuevo
    if resultados_finales:
        guardar_json_en_archivo(resultados_finales, SALIDA_JSON)
        print(f"\n✅ Archivo final guardado en: {SALIDA_JSON}")
        print(f"🔎 Total objetos combinados: {len(resultados_finales)}")
        print("Ejemplo del primero:")
        print(json.dumps(resultados_finales[0], ensure_ascii=False, indent=2))
    else:
        print("⚠️ No se generó ningún JSON válido. Revisa los errores anteriores.")

if __name__ == "__main__":
    print("🚀 Iniciando script Gemini…")
    main()
