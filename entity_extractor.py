# extractor/entity_extractor.py

import re

PLATE_REGEX = r'\b\d{4}[A-Z]{3}\b'
DNI_REGEX = r'\b\d{8}[A-Z]\b'

def extract_entities(text: str) -> dict:
    """
    NO consulta BD
    NO normaliza con conocimiento externo
    """
    vehicles = re.findall(PLATE_REGEX, text)
    dnis = re.findall(DNI_REGEX, text)

    locations = []
    for line in text.splitlines():
        if any(x in line.lower() for x in ["carrer", "carretera", "avinguda", "plaça"]):
            locations.append(line.strip())

    return {
        "vehicles": list(set(vehicles)),
        "persons": [{"dni": dni} for dni in dnis],
        "locations": locations
    }