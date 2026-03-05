import sqlite3
import logging
from typing import Dict, List

logger = logging.getLogger("matcher_service")


class MatcherService:

    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # DB helper
    # ------------------------------------------------------------------

    def _q(self, sql, params=()):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"DB error: {e}")
            return []

    # ------------------------------------------------------------------
    # ENTRYPOINT
    # ------------------------------------------------------------------

    def contrastar_entidades(self, entidades: Dict):

        logger.info("Iniciando contraste de entidades")

        result = {
            "personas": self._match_personas(entidades.get("personas", [])),
            "vehiculos": self._match_vehiculos(entidades.get("vehiculos", [])),
            "ubicaciones": self._match_ubicaciones(entidades.get("ubicaciones", [])),
        }

        logger.info(
            f"Contraste completado: "
            f"{len(result['vehiculos'])} vehículos, "
            f"{len(result['personas'])} personas, "
            f"{len(result['ubicaciones'])} ubicaciones"
        )

        return result

    # ------------------------------------------------------------------
    # PERSONAS
    # ------------------------------------------------------------------

    def _match_personas(self, personas: List[Dict]):

        matches = []

        for p in personas:

            nombre = p.get("nombre", "")
            apellidos = p.get("apellidos", "")

            if not nombre:
                continue

            rows = self._q(
                """
                SELECT dni, nombre, apellidos, direccion, telefono
                FROM persons
                WHERE nombre LIKE ? OR apellidos LIKE ?
                LIMIT 5
                """,
                (f"%{nombre}%", f"%{apellidos}%"),
            )

            if rows:
                matches.append({
                    "texto": f"{nombre} {apellidos}".strip(),
                    "match_type": "EXACTO",
                    "confidence": 1.0,
                    "db_record": rows[0],
                    "position": p.get("position")
                })

        return matches

    # ------------------------------------------------------------------
    # VEHICULOS
    # ------------------------------------------------------------------

    def _match_vehiculos(self, vehiculos: List[Dict]):

        matches = []

        for v in vehiculos:

            plate = v.get("plate")

            if not plate:
                continue

            rows = self._q(
                """
                SELECT plate, brand, model, color
                FROM vehicles
                WHERE plate = ?
                """,
                (plate,),
            )

            if rows:
                matches.append({
                    "texto": plate,
                    "match_type": "EXACTO",
                    "confidence": 1.0,
                    "db_record": rows[0],
                    "position": v.get("position")
                })

        return matches

    # ------------------------------------------------------------------
    # UBICACIONES
    # ------------------------------------------------------------------

    def _match_ubicaciones(self, ubicaciones: List[Dict]):

        matches = []

        for u in ubicaciones:

            name = u.get("canonical_name")

            if not name:
                continue

            rows = self._q(
                """
                SELECT id, canonical_name
                FROM locations
                WHERE canonical_name LIKE ?
                LIMIT 5
                """,
                (f"%{name}%",),
            )

            if rows:
                matches.append({
                    "texto": name,
                    "match_type": "EXACTO",
                    "confidence": 1.0,
                    "db_record": rows[0],
                    "position": u.get("position")
                })

        return matches


# ----------------------------------------------------------------------
# UTIL para API
# ----------------------------------------------------------------------

def matches_to_dict(matches):

    return matches
