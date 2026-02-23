import os

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint


DEFAULT_MODEL = "mistralai/Mistral-7B-Instruct-v0.2"


def get_llm(api_token: str | None = None):
    token = api_token or os.getenv("HF_API_TOKEN")
    if not token:
        raise ValueError("HF_API_TOKEN não configurado")
    endpoint = HuggingFaceEndpoint(
        repo_id=DEFAULT_MODEL,
        task="text-generation",
        huggingfacehub_api_token=token,
    )
    return ChatHuggingFace(llm=endpoint)


def ask_llm(prompt: str, api_token: str | None = None) -> str:
    llm = get_llm(api_token=api_token)
    messages = [SystemMessage(content="Você é um assistente de biblioteca."), HumanMessage(content=prompt)]
    return llm.invoke(messages).content
