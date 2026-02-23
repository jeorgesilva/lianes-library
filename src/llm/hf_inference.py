import os

from huggingface_hub import InferenceClient


DEFAULT_MODEL = "mistralai/Mistral-7B-Instruct-v0.2"


def get_client(api_token: str | None = None) -> InferenceClient:
    token = api_token or os.getenv("HF_API_TOKEN")
    if not token:
        raise ValueError("HF_API_TOKEN não configurado")
    return InferenceClient(model=DEFAULT_MODEL, token=token)


def simple_completion(prompt: str, api_token: str | None = None) -> str:
    client = get_client(api_token=api_token)
    return client.text_generation(prompt, max_new_tokens=256)
