import pytest
from text_generator.prompt_builder import build_prompts


def test_build_prompts_length():
    desc = "Просторная квартира"
    prompts = build_prompts(desc, 5)
    assert isinstance(prompts, list)
    assert len(prompts) == 5
    for i, p in enumerate(prompts, start=1):
        assert f"#{i}" in p
        assert desc in p
