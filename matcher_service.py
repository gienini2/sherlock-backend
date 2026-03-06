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

    def close(self):
        """Mètode close() per compatibilitat amb el shutdown de l'orquestrador."""
        pass  # SQLite obre/tanca connexió per consulta, res a tancar aquí

    # ------------------------------------------------------------------
    # ENTRYPOINT
    # ------------------------------------------------------------------

    def contrastar_entidades(self, entidades: Dict) -> Dict:
        """
        Retorna directament el dict de matches (sense clau 'matches' intermèdia).
        Format de retorn:
            {
                "personas":    [...],
                "vehiculos":   [...],
                "ubicaciones": [...],
            }
        Cada element porta:
            - texto:            text original extret
            - match_type:       PARCIAL | SIN_COINCIDENCIA
            - confidence:       float 0.0-1.0
            - db_record:        dict de la BD (o None)
            - position:         {start, end} al text original
            - entidad_original: la entitat sencera tal com ve del extractor
        """
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
    # PERSONAS — retorna TOTS els resultats com PARCIAL
    # Si no hi ha cap coincidència → SIN_COINCIDENCIA (però l'orquestrador
    # l'injectarà com [[PERSONA:NOM||NOU]] gràcies a _injectar_marcadors)
    # ------------------------------------------------------------------

    def _match_personas(self, personas: List[Dict]) -> List[Dict]:
        matches = []

        for p in personas:
            nombre    = (p.get("nombre")    or "").strip()
            apellidos = (p.get("apellidos") or "").strip()

            if not nombre:
                continue

            rows = self._q(
                """
                SELECT dni, nombre, apellidos, direccion, telefono, fecha_nacimiento
                FROM persons
                WHERE nombre    LIKE ?
                   OR apellidos LIKE ?
                   OR nombre    LIKE ?
                LIMIT 10
                """,
                (
                    f"%{nombre}%",
                    f"%{apellidos}%",
                    f"%{apellidos}%",
                ),
            )

            if rows:
                # Totes les coincidències com PARCIAL — l'agent tria a la pestanya BD
                for row in rows:
                    matches.append({
                        "texto":            f"{nombre} {apellidos}".strip(),
                        "match_type":       "PARCIAL",
                        "confidence":       self._confidence_persona(p, row),
                        "db_record":        row,
                        "position":         p.get("position"),
                        "entidad_original": p,   # ← necessari per _injectar_marcadors
                    })
            else:
                # Sense coincidència → el redactor la marcarà com NOU
                matches.append({
                    "texto":            f"{nombre} {apellidos}".strip(),
                    "match_type":       "SIN_COINCIDENCIA",
                    "confidence":       0.0,
                    "db_record":        None,
                    "position":         p.get("position"),
                    "entidad_original": p,
                })

        return matches

    def _confidence_persona(self, extreta: Dict, db_row: Dict) -> float:
        """
        Càlcul de similitud simple entre l'entitat extreta i el registre de BD.
        Compara nom + cognoms per paraules coincidents.
        """
        nom_extret = f"{extreta.get('nombre','')} {extreta.get('apellidos','')}".upper().split()
        nom_db     = f"{db_row.get('nombre','')} {db_row.get('apellidos','')}".upper().split()

        if not nom_extret or not nom_db:
            return 0.5

        coincidencies = sum(1 for p in nom_extret if p in nom_db)
        total         = max(len(nom_extret), len(nom_db))

        return round(coincidencies / total, 2)

    # ------------------------------------------------------------------
    # VEHICULOS — coincidència exacta per matrícula
    # FIX: el extractor envia 'matricula', no 'plate'
    # ------------------------------------------------------------------

    def _match_vehiculos(self, vehiculos: List[Dict]) -> List[Dict]:
        matches = []

        for v in vehiculos:
            # L'extractor regex retorna 'matricula'; l'extractor Claude retorna 'matricula' també
            matricula = (v.get("matricula") or v.get("plate") or "").strip().upper()

            if not matricula:
                continue

            rows = self._q(
                """
                SELECT plate, brand, model, color
                FROM vehicles
                WHERE plate = ?
                """,
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
                # Cerca parcial pels primers 4 caràcters (números de la matrícula)
                rows_parcial = self._q(
                    """
                    SELECT plate, brand, model, color
                    FROM vehicles
                    WHERE plate LIKE ?
                    LIMIT 5
                    """,
                    (f"%{matricula[:4]}%",),
                ) if len(matricula) >= 4 else []

                match_type = "PARCIAL" if rows_parcial else "SIN_COINCIDENCIA"
                matches.append({
                    "texto":            matricula,
                    "match_type":       match_type,
                    "confidence":       0.7 if rows_parcial else 0.0,
                    "db_record":        rows_parcial[0] if rows_parcial else None,
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
            # El extractor regex retorna 'texto_completo'
            name = (
                u.get("canonical_name")
                or u.get("texto_completo")
                or u.get("nombre_via")
                or ""
            ).strip()

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


# ----------------------------------------------------------------------
# UTIL — ara no cal transformar res, el dict ja té el format correcte
# Mantenim la funció per compatibilitat amb els imports existents
# ----------------------------------------------------------------------

def matches_to_dict(matches: Dict) -> Dict:
    """
    Passthrough. El MatcherService ja retorna el format correcte.
    Funció mantinguda per compatibilitat amb imports existents.
    """
    return matches
