# catalog_loader.py

import json
import os


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def merge_familias(catalogos):
    familias = []
    for cat in catalogos:
        if "familias" in cat:
            familias.extend(cat["familias"])
    return familias


def extract_banderas(catalogos):
    banderas = []
    for cat in catalogos:
        if "banderas_penales" in cat:
            banderas.extend(cat["banderas_penales"])
        if "bandera" in cat:
            banderas.append(cat["bandera"])
    return list(set(banderas))


def load_catalogo_completo(base_path: str):

    archivos = [
        "sherlock_cp_t1v1.json",
        "sherlock_cp_t2v1.json",
        "sherlock_cp_t3v1.json",
        "sherlock_cp_t4v1.json",
        "sherlock_cp_t5v1.json",
        "sherlock_cp_t6v1.json",
        "sherlock_cp_t7v1.json",
        "sherlock_cp_t8-segvial-v1.json",
        "sherlock_cp_capastransversales-v1.json",
        "sherlock_cp_capamenor-v1.json",
        "sherlock_cp_capamenorautor-v1.json",
    ]

    catalogos = []

    for archivo in archivos:
        path = os.path.join(base_path, archivo)
        if os.path.exists(path):
            catalogos.append(load_json(path))

    return {
        "familias": merge_familias(catalogos),
        "banderas": extract_banderas(catalogos)
    }