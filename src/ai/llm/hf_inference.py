import os

from huggingface_hub import InferenceClient


DEFAULT_MODEL = "Qwen/Qwen2.5-7B-Instruct"


def get_client(api_token: str | None = None) -> InferenceClient:
    token = api_token or os.getenv("HF_API_TOKEN")
    if not token:
        raise ValueError("HF_API_TOKEN não configurado")
    return InferenceClient(model=DEFAULT_MODEL, token=token)


def simple_completion(prompt: str, api_token: str | None = None) -> str:
    client = get_client(api_token=api_token)
    # HuggingFace's Inference API now routes this model through providers that
    # only support the "conversational" task, not raw text_generation.
    completion = client.chat_completion(
        messages=[{"role": "user", "content": prompt}],
        max_tokens=256,
    )
    return completion.choices[0].message.content
