# patrimonio_engine.py

import os
import json
import math
from typing import Dict, List


VARIABLES_PATRIMONIO = [
    "animo_lucro",
    "apoderamiento",
    "fuerza_en_cosas",
    "violencia_persona",
    "intimidacion",
    "uso_arma",
    "cuantia_economica",
    "acceso_no_autorizado",
    "especial_valor_bien",
    "engaño",
    "abuso_confianza",
    "uso_indebido_bien",
    "deterioro_bien",
    "ocupacion_inmueble",
    "falsificacion_medio_pago"
]


def vector_to_list(vector: Dict) -> List[float]:
    return [float(vector.get(var, 0.0)) for var in VARIABLES_PATRIMONIO]


def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))

    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0

    return dot / (norm1 * norm2)


def load_patrimonio_matrix(base_path: str) -> Dict[str, List[float]]:
    matrix = {}

    for file in os.listdir(base_path):
        if file.startswith("CP-Patrimonio") and file.endswith(".json"):
            delito = file.replace("CP-Patrimonio-", "").replace(".json", "")
            path = os.path.join(base_path, file)

            with open(path, "r", encoding="utf-8") as f:
                vector = json.load(f)

            matrix[delito] = vector_to_list(vector)

    return matrix


def patrimonio_similarity(vector_texto: Dict, base_path: str) -> Dict:

    vector_texto_list = vector_to_list(vector_texto)
    matrix = load_patrimonio_matrix(base_path)

    ranking = []

    for delito, vector_delito in matrix.items():
        score = cosine_similarity(vector_texto_list, vector_delito)

        ranking.append({
            "delito": delito,
            "score": round(score, 6)
        })

    ranking.sort(key=lambda x: x["score"], reverse=True)

    return {
        "modulo": "patrimonio",
        "ranking": ranking
    }