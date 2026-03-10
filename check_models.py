import google.generativeai as genai
import os

# Pega tu API KEY aquí abajo entre las comillas
API_KEY = 'AIzaSyBqcV-wZQ8Xb0W88qMPQpnisLqbSovujZQ'

genai.configure(api_key=API_KEY)

print("--- CONSULTANDO MODELOS DISPONIBLES ---")
try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"Modelo encontrado: {m.name}")
except Exception as e:
    print(f"Error: {e}")