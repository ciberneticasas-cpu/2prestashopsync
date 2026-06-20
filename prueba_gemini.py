from google import genai
import os
import sys

# Validar que la API key esté presente
if "GEMINI_API_KEY" not in os.environ:
    print("Error: La variable de entorno GEMINI_API_KEY no está configurada.")
    sys.exit(1)

# Inicializar el cliente (detecta automáticamente la variable de entorno)
client = genai.Client()

print("Enviando petición a Gemini...")

try:
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents='Escribe un poema de dos líneas sobre la terminal de Linux.',
    )
    print("\n--- Respuesta de Gemini ---")
    print(response.text)
    print("---------------------------")
except Exception as e:
    print(f"\nHubo un error al conectar con la API: {e}")
