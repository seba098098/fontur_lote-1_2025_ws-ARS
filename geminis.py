from google import genai

# Pon tu API key aquí
API_KEY = "AIzaSyCWSewUpa3JGNcsvkGRcNR03Sl_0CDeemg"

# Inicializa el cliente con tu API key
client = genai.Client(api_key=API_KEY)

# Ejemplo: hacerle una pregunta al modelo
resp = client.models.generate_content(
    model="gemini-2.5-flash",   # también puedes probar "gemini-2.5-pro"
    contents="¿Cuál es la capital del departamento del Huila en Colombia?"
)

# Mostrar la respuesta
print("Respuesta de Gemini:", resp.text)
