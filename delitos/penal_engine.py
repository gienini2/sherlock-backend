# penal_engine.py

from typing import Dict, List, Any


SCORE_THRESHOLD = 0.35


def evaluar_condiciones_minimas(hechos: Dict, condiciones: List[str]) -> bool:
    for condicion in condiciones:
        if not hechos.get(condicion, False):
            return False
    return True


def contar_indicadores(textos: List[str], patrones: List[str]) -> int:
    count = 0
    for patron in patrones:
        for t in textos:
            if patron.lower() in t.lower():
                count += 1
                break
    return count


def calcular_score(base: float,
                   fuertes: int,
                   debiles: int,
                   exclusion_parcial: bool,
                   exclusion_total: bool) -> float:

    score = base
    score += 0.15 * fuertes
    score += 0.05 * debiles

    if exclusion_parcial:
        score -= 0.30
    if exclusion_total:
        score -= 1.0

    return max(0, min(score, 1))


def evaluar_conducta(conducta: Dict,
                     hechos: Dict,
                     textos: List[str]) -> Dict | None:

    condiciones = conducta.get("condiciones_minimas", [])

    if not evaluar_condiciones_minimas(hechos, condiciones):
        return None

    fuertes = contar_indicadores(textos,
                                 conducta.get("indicadores_fuertes", []))

    debiles = contar_indicadores(textos,
                                 conducta.get("indicadores_debiles", []))

    exclusion_total = False
    exclusion_parcial = False

    for excl in conducta.get("llindar_penal", {}).get("excluye", []):
        for t in textos:
            if excl.lower() in t.lower():
                exclusion_total = True

    score = calcular_score(
        conducta.get("nivel_confianza_base", 0),
        fuertes,
        debiles,
        exclusion_parcial,
        exclusion_total
    )

    if score < SCORE_THRESHOLD:
        return None

    return {
        "conducta_id": conducta.get("id_conducta"),
        "articulos_orientativos": conducta.get("articulos_orientativos", []),
        "score": round(score, 2),
        "detencion_posible": conducta.get("detencion_posible", False),
        "condiciones_cumplidas": condiciones,
        "indicadores_fuertes_detectados": fuertes,
        "indicadores_debiles_detectados": debiles,
        "exclusion_total": exclusion_total
    }


def penal_engine(hechos_struct: Dict, catalogo: Dict) -> Dict:

    resultados = []

    hechos = hechos_struct["hechos_detectados"]
    textos = hechos_struct["hechos_relevantes_textuales"]

    for familia in catalogo.get("familias", []):
        for conducta in familia.get("conductas", []):

            resultado = evaluar_conducta(conducta, hechos, textos)

            if resultado:
                resultado["familia"] = familia.get("familia")
                resultados.append(resultado)

    resultados.sort(key=lambda x: x["score"], reverse=True)

    concurso = len(resultados) > 1

    if not resultados:
        return {
            "evaluacion_penal": [],
            "nota_general": "No se aprecia encaje penal suficiente con la información disponible."
        }

    return {
        "evaluacion_penal": resultados,
        "concurso_posible": concurso,
        "nota_general": "La conducta descrita podría encajar en varias tipologías según aproximación probabilística."
    }