# resolver/resolver.py

from resolver.similarity import plate_similarity

def resolve_vehicle(plate: str, db) -> dict:
    exact = db.get_vehicle_exact(plate)
    if exact:
        return {
            "entity": plate,
            "type": "vehicle",
            "match": "exact",
            "facts": {
                "marca": exact["marca"],
                "modelo": exact["modelo"],
                "titular": exact["titular"],
                "apariciones": db.get_vehicle_history_count(plate)
            }
        }

    similars = db.get_vehicle_similar(plate)
    for v in similars:
        if plate_similarity(plate, v["matricula"]) >= 0.7:
            return {
                "entity": plate,
                "type": "vehicle",
                "match": "partial",
                "facts": {
                    "similar_plate": v["matricula"]
                }
            }

    return {
        "entity": plate,
        "type": "vehicle",
        "match": "none",
        "facts": {}
    }