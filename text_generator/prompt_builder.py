def build_prompts(description: str, num: int) -> list:
    """
    Generate a list of prompts for the LLM to create unique listing texts.
    """
    prompts = []
    for i in range(1, num + 1):
        prompts.append(
            f"Сгенерируй уникальный текст объявления #{i} на русском языке на основе описания:\n{description}\n"
            "В тексте сохрани все факты из описания и добавь креативные детали, чтобы объявления различались."  # noqa
        )
    return prompts
