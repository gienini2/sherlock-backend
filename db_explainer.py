# annotator/db_explainer.py

def explain(resolutions: list) -> list[str]:
    explanations = []

    for r in resolutions:
        if r["match"] == "exact":
            f = r["facts"]
            explanations.append(
                f"La matrícula {r['entity']} consta a la base de dades "
                f"({f['marca']} {f['modelo']}), titular {f['titular']}. "
                f"Apareix en {f['apariciones']} actuacions prèvies."
            )
        elif r["match"] == "partial":
            explanations.append(
                f"La matrícula {r['entity']} presenta coincidència parcial "
                f"amb {r['facts']['similar_plate']}."
            )

    return explanations