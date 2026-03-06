import sqlite3
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger("matcher_service")


class MatcherService:

    def __init__(self, db_path: str):
        self.db_path = db_path

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

    def close(self):
        pass

    def contrastar_entidades(self, entidades: Dict) -> Dict:
        logger.info("Iniciando contraste de entidades")
        result = {
            "personas":    self._match_personas(entidades.get("personas", [])),
            "vehiculos":   self._match_vehiculos(entidades.get("vehiculos", [])),
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
    # Problema anterior: la query usaba OR suelto entre nombre y apellidos
    # separados, lo que devolvía cualquier "Angeles" o "Juan" de la BD.
    #
    # Solución: buscamos por CADA palabra del texto extraído y luego
    # filtramos en Python los resultados que no tengan mínimo 2 palabras
    # coincidentes. Esto elimina los falsos positivos de 0%.
    # ------------------------------------------------------------------

    def _match_personas(self, personas: List[Dict]) -> List[Dict]:
        matches = []

        for p in personas:
            nombre    = (p.get("nombre")    or "").strip()
            apellidos = (p.get("apellidos") or "").strip()

            if not nombre:
                continue

            # Palabras significativas del texto (≥3 chars para excluir "Ha", "De", "El"...)
            palabras = [
                w for w in f"{nombre} {apellidos}".split()
                if len(w) >= 3
            ]

            if not palabras:
                continue

            # Query dinámica: una condición LIKE por cada palabra
            condiciones = " OR ".join(
                ["nombre LIKE ? OR apellidos LIKE ?"] * len(palabras)
            )
            params = []
            for w in palabras:
                params.extend([f"%{w}%", f"%{w}%"])

            rows = self._q(
                f"""
                SELECT dni, nombre, apellidos, direccion, telefono, fecha_nacimiento
                FROM persons
                WHERE {condiciones}
                LIMIT 20
                """,
                params,
            )

            if not rows:
                matches.append({
                    "texto":            f"{nombre} {apellidos}".strip(),
                    "match_type":       "SIN_COINCIDENCIA",
                    "confidence":       0.0,
                    "db_record":        None,
                    "position":         p.get("position"),
                    "entidad_original": p,
                })
                continue

            # Filtrar en Python: mínimo 2 palabras del texto deben coincidir
            # en el registro de BD. Esto elimina los "Angeles" sueltos.
            MIN_COINCIDENCIAS = 2
            candidatos = []

            for row in rows:
                conf, n = self._confidence_persona(palabras, row)
                if n >= MIN_COINCIDENCIAS:
                    candidatos.append((conf, row))

            if not candidatos:
                matches.append({
                    "texto":            f"{nombre} {apellidos}".strip(),
                    "match_type":       "SIN_COINCIDENCIA",
                    "confidence":       0.0,
                    "db_record":        None,
                    "position":         p.get("position"),
                    "entidad_original": p,
                })
                continue

            # Ordenar por confianza descendente, todos como PARCIAL
            candidatos.sort(key=lambda x: x[0], reverse=True)

            for conf, row in candidatos:
                matches.append({
                    "texto":            f"{nombre} {apellidos}".strip(),
                    "match_type":       "PARCIAL",
                    "confidence":       conf,
                    "db_record":        row,
                    "position":         p.get("position"),
                    "entidad_original": p,
                })

        return matches

    def _confidence_persona(self, palabras: List[str], db_row: Dict) -> Tuple[float, int]:
        """
        Cuenta cuántas palabras del texto extraído aparecen en el nombre completo de BD.
        Devuelve (confidence, num_coincidencias).
        Ejemplo: ["Luz","Estrella","Gangas"] vs "Luz Estrella Gangas Alvear" → (0.75, 3)
        """
        nom_db = f"{db_row.get('nombre','')} {db_row.get('apellidos','')}".upper()
        n = sum(1 for p in palabras if p.upper() in nom_db)
        conf = round(n / len(palabras), 2) if palabras else 0.0
        return conf, n

    # ------------------------------------------------------------------
    # VEHICULOS
    # ------------------------------------------------------------------

    def _match_vehiculos(self, vehiculos: List[Dict]) -> List[Dict]:
        matches = []
        for v in vehiculos:
            matricula = (v.get("matricula") or v.get("plate") or "").strip().upper()
            if not matricula:
                continue

            rows = self._q(
                "SELECT plate, brand, model, color FROM vehicles WHERE plate = ?",
                (matricula,),
            )
            if rows:
                matches.append({
                    "texto":            matricula,
                    "match_type":       "EXACTO",
                    "confidence":       1.0,
                    "db_record":        rows[0],
                    "position":         v.get("position"),
                    "entidad_original": v,
                })
            else:
                rows_p = self._q(
                    "SELECT plate, brand, model, color FROM vehicles WHERE plate LIKE ? LIMIT 5",
                    (f"%{matricula[:4]}%",),
                ) if len(matricula) >= 4 else []

                matches.append({
                    "texto":            matricula,
                    "match_type":       "PARCIAL" if rows_p else "SIN_COINCIDENCIA",
                    "confidence":       0.7 if rows_p else 0.0,
                    "db_record":        rows_p[0] if rows_p else None,
                    "position":         v.get("position"),
                    "entidad_original": v,
                })
        return matches

    # ------------------------------------------------------------------
    # UBICACIONES
    # ------------------------------------------------------------------

    def _match_ubicaciones(self, ubicaciones: List[Dict]) -> List[Dict]:
        matches = []
        for u in ubicaciones:
            name = (
                u.get("canonical_name")
                or u.get("texto_completo")
                or u.get("nombre_via")
                or ""
            ).strip()
            if not name:
                continue

            rows = self._q(
                "SELECT id, canonical_name FROM locations WHERE canonical_name LIKE ? LIMIT 5",
                (f"%{name}%",),
            )
            if rows:
                matches.append({
                    "texto":            name,
                    "match_type":       "EXACTO",
                    "confidence":       1.0,
                    "db_record":        rows[0],
                    "position":         u.get("position"),
                    "entidad_original": u,
                })
            else:
                matches.append({
                    "texto":            name,
                    "match_type":       "SIN_COINCIDENCIA",
                    "confidence":       0.0,
                    "db_record":        None,
                    "position":         u.get("position"),
                    "entidad_original": u,
                })
        return matches


def matches_to_dict(matches: Dict) -> Dict:
    return matches
