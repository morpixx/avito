import os
import asyncio
from .prompt_builder import build_prompts

async def generate_texts(description: str, n: int) -> list:
    """
    Stub text generation: return prompts directly without calling real LLM.
    """
    prompts = build_prompts(description, n)
    # Use prompts as generated texts
    return prompts
