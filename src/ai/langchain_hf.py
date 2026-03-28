import os
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint

# Força a leitura do .env para garantir que o HF_API_TOKEN é carregado
load_dotenv(override=True)

DEFAULT_MODEL = "mistralai/Mistral-7B-Instruct-v0.2"

def get_llm(api_token: str | None = None):
    # Procura a chave. O LangChain às vezes prefere o nome HUGGINGFACEHUB_API_TOKEN
    token = api_token or os.getenv("HF_API_TOKEN") or os.getenv("HUGGINGFACEHUB_API_TOKEN")
    
    if not token:
        raise ValueError("🚨 Token do Hugging Face não encontrado! Verifique o seu ficheiro .env.")
        
    endpoint = HuggingFaceEndpoint(
        repo_id=DEFAULT_MODEL,
        task="text-generation",
        huggingfacehub_api_token=token,
        max_new_tokens=512,  # Evita que a IA fale infinitamente e dê erro de timeout
        temperature=0.7      # Deixa as respostas criativas, mas focadas
    )
    return ChatHuggingFace(llm=endpoint)

def ask_llm(prompt: str, api_token: str | None = None) -> str:
    try:
        llm = get_llm(api_token=api_token)
        messages = [
            SystemMessage(content="Você é a assistente virtual de uma biblioteca. Responda de forma educada, curta e direta."), 
            HumanMessage(content=prompt)
        ]
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        print(f"❌ Erro ao comunicar com a IA (Hugging Face): {e}")
        return "Desculpe, a minha inteligência artificial está a recarregar as energias. Tente novamente em alguns segundos!"