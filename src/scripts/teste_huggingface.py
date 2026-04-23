import requests
import os
from pathlib import Path
from dotenv import load_dotenv

# Garante a leitura do .env na raiz
env_path = Path(__file__).resolve().parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Use o nome da variável que está no seu .env
HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN") 

# NOVA URL conforme a mensagem de erro da Hugging Face
API_URL = "https://router.huggingface.co/hf-inference/models/distilbert-base-uncased-finetuned-sst-2-english"

headers = {"Authorization": f"Bearer {HUGGINGFACE_TOKEN}"}

def testar_sentimento(texto):
    payload = {"inputs": texto}
    print(f"🚀 Enviando requisição para o novo roteador: {texto}...")
    
    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            print("✅ SUCESSO! O novo roteador respondeu corretamente.")
            print(f"📊 Resultado: {response.json()}")
        else:
            print(f"❌ ERRO {response.status_code}: {response.text}")
            
    except Exception as e:
        print(f"⚠️ Erro na conexão: {e}")

if __name__ == "__main__":
    if not HUGGINGFACE_TOKEN:
        print("⚠️ ERRO: Variável HF_API_TOKEN não encontrada no seu .env!")
    else:
        testar_sentimento("I absolutely loved reading Harry Potter, it was a magical experience!")