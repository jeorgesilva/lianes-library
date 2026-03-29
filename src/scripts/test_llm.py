import requests
import os
from dotenv import load_dotenv

# Carrega as variáveis do seu ficheiro .env 
load_dotenv()

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN")
# Usaremos um modelo leve e popular para o teste
API_URL = "https://api-inference.huggingface.co/models/distilbert-base-uncased-finetuned-sst-2-english"
headers = {"Authorization": f"Bearer {HUGGINGFACE_TOKEN}"}

def testar_sentimento(texto):
    payload = {"inputs": texto}
    print(f"🚀 Enviando requisição para: {texto}...")
    
    response = requests.post(API_URL, headers=headers, json=payload)
    
    if response.status_code == 200:
        print("✅ SUCESSO! A API respondeu corretamente.")
        print(f"📊 Resultado: {response.json()}")
    elif response.status_code == 503:
        print("⏳ O modelo está a carregar no Hugging Face. Tente novamente em alguns segundos.")
    else:
        print(f"❌ ERRO {response.status_code}: {response.text}")

if __name__ == "__main__":
    if not HUGGINGFACE_TOKEN:
        print("⚠️ ERRO: Variável HUGGINGFACE_TOKEN não encontrada no seu .env!")
    else:
        # Teste com uma frase positiva
        testar_sentimento("I absolutely loved reading Harry Potter, it was a magical experience!")