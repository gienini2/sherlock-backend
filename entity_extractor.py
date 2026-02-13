import re

PLATE_REGEX = r'\b\d{4}[^A-Z0-9]?[A-Z]{3}\b'
DNI_REGEX = r'\b\d{8}[A-Z]\b'

def extract_entities(text: str) -> dict:
    """
    NO consulta BD
    NO normaliza con conocimiento externo
    """

    # Normalizamos a mayúsculas
    text_upper = text.upper()

    # --- MATRÍCULAS ---
    raw_plates = re.findall(PLATE_REGEX, text_upper)
    vehicles = []

    for plate in raw_plates:
        clean_plate = re.sub(r'[^A-Z0-9]', '', plate)
        vehicles.append(clean_plate)

    # --- DNIs ---
    dnis = re.findall(DNI_REGEX, text_upper)

    # --- UBICACIONES ---
    locations = []
    for line in text.splitlines():
        if any(x in line.lower() for x in ["carrer", "carretera", "avinguda", "plaça"]):
            locations.append(line.strip())

    return {
        "vehicles": list(set(vehicles)),
        "persons": [{"dni": dni} for dni in dnis],
        "locations": locations
    }


