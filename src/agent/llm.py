"""DeepSeek API client (OpenAI-compatible)."""
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(Path(__file__).parents[2] / ".env")


def get_client() -> OpenAI:
    token = os.getenv("deep_seek_token")
    if not token:
        raise ValueError("deep_seek_token not found in .env")
    return OpenAI(
        api_key=token,
        base_url="https://api.deepseek.com",
    )


def chat(messages: list[dict], model: str = "deepseek-chat", temperature: float = 0.3) -> str:
    client = get_client()
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
    )
    return resp.choices[0].message.content.strip()


def chat_with_usage(
    messages: list[dict],
    model: str = "deepseek-chat",
    temperature: float = 0.3,
) -> tuple[str, dict]:
    """Same as chat() but returns (text, usage_dict) with token counts."""
    client = get_client()
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
    )
    usage = {
        "prompt_tokens":     resp.usage.prompt_tokens     if resp.usage else 0,
        "completion_tokens": resp.usage.completion_tokens if resp.usage else 0,
        "total_tokens":      resp.usage.total_tokens      if resp.usage else 0,
    }
    return resp.choices[0].message.content.strip(), usage
