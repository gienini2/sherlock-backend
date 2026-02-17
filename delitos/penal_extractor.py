# penal_extractor.py

import json
from pathlib import Path


def cargar_prompt_penal():
    path = Path(__file__).parent / "penal_system.txt"
    return path.read_text(encoding="utf-8")


async def extract_hechos_penales(texto: str, anthropic_client):

    prompt = cargar_prompt_penal()

    message = anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1200,
        system=prompt,
        messages=[{"role": "user", "content": texto}]
    )

    response_text = ""
    for block in message.content:
        if block.type == "text":
            response_text += block.text

    response_text = response_text.strip()

    # limpieza markdown
    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]

    return json.loads(response_text)