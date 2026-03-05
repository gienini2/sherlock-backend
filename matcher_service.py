"""
MATCHER SERVICE - Sistema de Contraste de Entidades con Base de Datos
=====================================================================

Servicio DETERMINISTA que:
- Recibe entidades extraídas (vehículos, personas, ubicaciones)
- Consulta hermano_mayor.db (READ-ONLY)
- Calcula coincidencias exactas y fuzzy
- Enriquece con histórico
- Devuelve JSON estructurado

NO razona, NO escribe texto, NO inventa datos.

CAMBIOS v2:
- Conexión SQLite persistente (una sola conexión por instancia)
- Pre-filtro LIKE antes del fuzzy en vehículos y personas
- calcular_nivel_riesgo corregido (considera antigüedad)
- Helpers _q / _q1 eliminan boilerplate repetido
"""

import sqlite3
import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from token_matcher import TokenMatcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Librerías de similitud (con fallback básico)
# ---------------------------------------------------------------------------
try:
    from rapidfuzz import fuzz
    from unidecode import unidecode
    FUZZY_AVAILABLE = True
except ImportError:
    logger.warning("rapidfuzz o unidecode no instalados. Usando fuzzy matching básico.")
    FUZZY_AVAILABLE = False

    def levenshtein_distance(s1: str, s2: str) -> int:
        if len(s1) < len(s2):
            return levenshtein_distance(s2, s1)
        if not s2:
            return len(s1)
        prev = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            curr = [i + 1]
            for j, c2 in enumerate(s2):
                curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
            prev = curr
        return prev[-1]

    def _fuzz_ratio_basic(s1: str, s2: str) -> float:
        if not s1 or not s2:
            return 0.0
        ml = max(len(s1), len(s2))
        return ((ml - levenshtein_distance(s1.lower(), s2.lower())) / ml) * 100

    def unidecode(s: str) -> str:  # type: ignore[override]
        trans = str.maketrans(
            'áéíóúàèìòùäëïöüâêîôûñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÂÊÎÔÛÑÇ',
            'aeiouaeiouaeiouaeiouncrAEIOUAEIOUAEIOUAEIOUNCR'
        )
        return s.translate(trans)

    class _FuzzBasic:
        @staticmethod
        def ratio(s1: str, s2: str) -> float:
            return _fuzz_ratio_basic(s1, s2)

    fuzz = _FuzzBasic()  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Dataclasses de resultado
# ---------------------------------------------------------------------------

@dataclass
class VehiculoMatch:
    entidad_original: Dict
    match_type: str        # EXACTO | PARCIAL | SIN_COINCIDENCIA | ERROR
    confidence: float
    db_record: Optional[Dict]
    enrichment: Dict


@dataclass
class PersonaMatch:
    entidad_original: Dict
    match_type: str
    confidence: float
    db_record: Optional[Dict]
    enrichment: Dict


@dataclass
class UbicacionMatch:
    entidad_original: Dict
    match_type: str
    confidence: float
    db_record: Optional[Dict]
    enrichment: Dict


class DatabaseError(Exception):
    pass


# ---------------------------------------------------------------------------
# Helper de nivel de riesgo (módulo-level para testabilidad)
# ---------------------------------------------------------------------------

def calcular_nivel_riesgo(num_actuaciones: int, dias_ultima: int) -> str:
    """
    Calcula nivel de riesgo basado en historial.

    Criterios corregidos:
    - ALTO:  última actuación ≤30 días
             O ≥5 actuaciones Y última ≤365 días
    - MEDIO: ≥2 actuaciones O última ≤90 días
    - BAJO:  resto
    """
    if num_actuaciones == 0:
        return "BAJO"
    if dias_ultima <= 30:
        return "ALTO"
    if num_actuaciones >= 5 and dias_ultima <= 365:
        return "ALTO"
    if num_actuaciones >= 2 or dias_ultima <= 90:
        return "MEDIO"
    return "BAJO"


# ---------------------------------------------------------------------------
# Clase principal
# ---------------------------------------------------------------------------

class MatcherService:
    """
    Servicio de contraste determinista.
    Usa una única conexión SQLite compartida (read-only URI).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._validate_db()

        # Conexión persistente en modo read-only
        self._conn = sqlite3.connect(
            f"file:{db_path}?mode=ro",
            uri=True,
            check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row

        from db_adapter import HermanoMayorDB
        self._db = HermanoMayorDB(db_path)

        self.token_matcher = TokenMatcher()
        logger.info("[MATCHER] Servicio inicializado (conexión persistente)")

    def close(self):
        """Tanca connexions. Cridat pel shutdown de FastAPI."""
        try:
            if hasattr(self, '_db') and self._db and hasattr(self._db, 'conn'):
                self._db.conn.close()
                logger.info("[MATCHER] Connexió BD tancada")
        except Exception as e:
            logger.warning(f"[MATCHER] Error tancant connexió: {e}")

    @property
    def db_adapter(self):
        return self._db

    # -----------------------------------------------------------------------
    # Validación
    # -----------------------------------------------------------------------

    def _validate_db(self):
        if not Path(self.db_path).exists():
            raise DatabaseError(f"Base de datos no encontrada: {self.db_path}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                tables = {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                missing = {'vehicles', 'persons', 'locations'} - tables
                if missing:
                    raise DatabaseError(f"Tablas faltantes: {missing}")
                logger.info(f"BD validada: {len(tables)} tablas")
        except sqlite3.Error as e:
            raise DatabaseError(f"Error validando BD: {e}")

    # -----------------------------------------------------------------------
    # Entrada principal
    # -----------------------------------------------------------------------

    def contrastar_entidades(self, entidades: Dict) -> Dict:
        logger.info("Iniciando contraste de entidades")
        matches = {
            "vehiculos":   self._match_vehiculos(entidades.get("vehiculos", [])),
            "personas":    self._match_personas(entidades.get("personas", [])),
            "ubicaciones": self._match_ubicaciones(entidades.get("ubicaciones", [])),
        }
        logger.info(
            f"Contraste completado: {len(matches['vehiculos'])} vehículos, "
            f"{len(matches['personas'])} personas, "
            f"{len(matches['ubicaciones'])} ubicaciones"
        )
        return {"matches": matches}

    # -----------------------------------------------------------------------
    # Helpers de query (eliminan el boilerplate de abrir conexión)
    # -----------------------------------------------------------------------

    def _q(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Ejecuta query y devuelve lista de dicts."""
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def _q1(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        """Ejecuta query y devuelve primer resultado o None."""
        row = self._conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    # =========================================================================
    # VEHÍCULOS
    # =========================================================================

    def _match_vehiculos(self, vehiculos: List[Dict]) -> List[VehiculoMatch]:
        results = []
        for v in vehiculos:
            try:
                results.append(self._match_vehiculo_single(v))
            except Exception as e:
                logger.error(f"Error procesando vehículo {v}: {e}")
                results.append(VehiculoMatch(v, "ERROR", 0.0, None, {"error": str(e)}))
        return results

    def _match_vehiculo_single(self, vehiculo: Dict) -> VehiculoMatch:
        matricula = vehiculo.get("matricula", "").strip()
        if not matricula:
            return VehiculoMatch(vehiculo, "SIN_COINCIDENCIA", 0.0, None, {})

        mn = self._normalizar_matricula(matricula)

        exact = self._buscar_vehiculo_exacto(mn)
        if exact:
            return self._build_vehiculo_match(vehiculo, exact, "EXACTO", 1.0)

        fuzzy = self._buscar_vehiculo_fuzzy(
            mn,
            vehiculo.get("marca", "").strip(),
            vehiculo.get("modelo", "").strip()
        )
        if fuzzy and fuzzy[0][1] >= 0.85:
            return self._build_vehiculo_match(vehiculo, fuzzy[0][0], "PARCIAL", fuzzy[0][1])

        return VehiculoMatch(vehiculo, "SIN_COINCIDENCIA", 0.0, None, {})

    def _buscar_vehiculo_exacto(self, mn: str) -> Optional[Dict]:
        return self._q1(
            "SELECT vehicle_id, plate, brand, model, dni_titular "
            "FROM vehicles "
            "WHERE UPPER(REPLACE(REPLACE(plate,' ',''),'-','')) = ?",
            (mn,)
        )

    def _buscar_vehiculo_fuzzy(
        self, mn: str, marca: str, modelo: str
    ) -> List[Tuple[Dict, float]]:
        """Pre-filtra por prefijo numérico antes del fuzzy."""
        prefijo = mn[:4] if len(mn) >= 4 else mn
        filas = self._q(
            "SELECT vehicle_id, plate, brand, model, dni_titular "
            "FROM vehicles "
            "WHERE UPPER(REPLACE(REPLACE(plate,' ',''),'-','')) LIKE ?",
            (f"{prefijo}%",)
        )
        if not filas and len(mn) >= 7:
            sufijo = mn[-3:]
            filas = self._q(
                "SELECT vehicle_id, plate, brand, model, dni_titular "
                "FROM vehicles "
                "WHERE UPPER(REPLACE(REPLACE(plate,' ',''),'-','')) LIKE ?",
                (f"%{sufijo}",)
            )

        candidates = []
        for v in filas:
            pn        = self._normalizar_matricula(v['plate'])
            sim_plate = fuzz.ratio(mn, pn) / 100.0
            sim_brand = fuzz.ratio(self._normalizar_texto(marca),  self._normalizar_texto(v.get('brand') or '')) / 100.0 if marca else 0.0
            sim_model = fuzz.ratio(self._normalizar_texto(modelo), self._normalizar_texto(v.get('model') or '')) / 100.0 if modelo else 0.0
            conf      = sim_plate * 0.70 + sim_brand * 0.15 + sim_model * 0.15
            if conf >= 0.70:
                candidates.append((v, conf))

        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[:3]

    def _build_vehiculo_match(
        self, entidad: Dict, db_record: Dict, match_type: str, confidence: float
    ) -> VehiculoMatch:
        enrichment: Dict = {}
        if db_record.get('dni_titular'):
            titular = self._get_persona_by_dni(db_record['dni_titular'])
            if titular:
                enrichment['titular'] = {k: titular[k] for k in ('dni', 'nombre', 'apellidos')}
        vid = db_record.get('vehicle_id')
        if vid:
            enrichment['apariciones_previas']   = self._count_vehicle_appearances(vid)
            enrichment['eventos_previos']        = self._get_vehicle_events(vid)
            enrichment['conductores_habituales'] = self._get_vehicle_drivers(vid)
        return VehiculoMatch(entidad, match_type, confidence, db_record, enrichment)

    def _count_vehicle_appearances(self, vehicle_id: int) -> int:
        row = self._q1(
            "SELECT COUNT(*) AS c FROM entity_links WHERE entity_type='vehicle' AND entity_id=?",
            (vehicle_id,)
        )
        return row['c'] if row else 0

    def _get_vehicle_events(self, vehicle_id: int, limit: int = 5) -> List[str]:
        rows = self._q(
            "SELECT source_event_id FROM entity_links "
            "WHERE entity_type='vehicle' AND entity_id=? "
            "ORDER BY created_at_ts DESC LIMIT ?",
            (vehicle_id, limit)
        )
        return [str(r['source_event_id']) for r in rows]

    def _get_vehicle_drivers(self, vehicle_id: int) -> List[Dict]:
        return self._q(
            "SELECT p.dni, p.nombre, p.apellidos, vpl.confidence, vpl.relation_type "
            "FROM vehicle_person_links vpl "
            "JOIN persons p ON vpl.person_id = p.dni "
            "WHERE vpl.vehicle_id=? ORDER BY vpl.confidence DESC LIMIT 3",
            (vehicle_id,)
        )

    # =========================================================================
    # PERSONAS
    # =========================================================================

    def _match_personas(self, personas: List[Dict]) -> List[PersonaMatch]:
        results = []
        for p in personas:
            try:
                results.append(self._match_persona_single(p))
            except Exception as e:
                logger.error(f"Error procesando persona {p}: {e}")
                results.append(PersonaMatch(p, "ERROR", 0.0, None, {"error": str(e)}))
        return results

    def _match_persona_single(self, persona: Dict) -> PersonaMatch:
        dni       = persona.get("dni", "").strip()
        nombre    = persona.get("nombre", "").strip()
        apellidos = persona.get("apellidos", "").strip()

        if dni:
            exact = self._buscar_persona_by_dni(dni)
            if exact:
                return self._build_persona_match(persona, exact, "EXACTO", 1.0)

        if nombre or apellidos:
            fuzzy = self._buscar_persona_by_nombre(nombre, apellidos)
            if fuzzy and fuzzy[0][1] >= 0.60:
                conf       = fuzzy[0][1]
                match_type = "EXACTO" if conf >= 0.95 else "PARCIAL"
                return self._build_persona_match(persona, fuzzy[0][0], match_type, conf)

        return PersonaMatch(persona, "SIN_COINCIDENCIA", 0.0, None, {})

    def _buscar_persona_by_dni(self, dni: str) -> Optional[Dict]:
        return self._q1(
            "SELECT dni, nombre, apellidos, direccion, telefono, "
            "fecha_nacimiento, sexo, observaciones "
            "FROM persons WHERE UPPER(REPLACE(dni,' ',''))=?",
            (self._normalizar_dni(dni),)
        )

    def _buscar_persona_by_nombre(
        self, nombre: str, apellidos: str
    ) -> List[Tuple[Dict, float]]:
        """Pre-filtra por primer token de apellidos antes del token matching."""
        primer_token = apellidos.split()[0] if apellidos.split() else nombre[:3] if nombre else ""
        if primer_token:
            candidatos = self._q(
                "SELECT dni, nombre, apellidos, direccion, telefono, "
                "fecha_nacimiento, sexo, observaciones "
                WHERE apellidos LIKE ? OR nombre LIKE ?
                """,
                (f"%{primer_token}%", f"%{nombre}%")
                )
        else:
            candidatos = self._q(
                "SELECT dni, nombre, apellidos, direccion, telefono, "
                "fecha_nacimiento, sexo, observaciones FROM persons"
            )

        resultados = self.token_matcher.buscar_persona_fuzzy_tokens(
            nombre, apellidos, candidatos, umbral=0.70
        )
        return [(p, c) for p, c, _ in resultados]

    def _build_persona_match(
        self, entidad: Dict, db_record: Dict, match_type: str, confidence: float
    ) -> PersonaMatch:
        enrichment: Dict = {}
        dni = db_record.get('dni')
        if dni:
            enrichment['apariciones_previas']    = self._count_person_appearances(dni)
            enrichment['roles_previos']          = self._get_person_roles(dni)
            enrichment['vehiculos_relacionados'] = self._get_person_vehicles(dni)
        return PersonaMatch(entidad, match_type, confidence, db_record, enrichment)

    def _count_person_appearances(self, dni: str) -> int:
        row = self._q1(
            "SELECT COUNT(*) AS c FROM entity_links WHERE entity_type='person' AND entity_id=?",
            (dni,)
        )
        return row['c'] if row else 0

    def _get_person_roles(self, dni: str, limit: int = 5) -> List[str]:
        rows = self._q(
            "SELECT DISTINCT role FROM person_roles WHERE dni=? ORDER BY created_at DESC LIMIT ?",
            (dni, limit)
        )
        return [r['role'] for r in rows]

    def _get_person_vehicles(self, dni: str) -> List[Dict]:
        return self._q(
            "SELECT v.plate, v.brand, v.model, vpl.relation_type "
            "FROM vehicle_person_links vpl "
            "JOIN vehicles v ON vpl.vehicle_id = v.vehicle_id "
            "WHERE vpl.person_id=? LIMIT 5",
            (dni,)
        )

    # =========================================================================
    # UBICACIONES
    # =========================================================================

    def _match_ubicaciones(self, ubicaciones: List[Dict]) -> List[UbicacionMatch]:
        results = []
        for u in ubicaciones:
            try:
                results.append(self._match_ubicacion_single(u))
            except Exception as e:
                logger.error(f"Error procesando ubicación {u}: {e}")
                results.append(UbicacionMatch(u, "ERROR", 0.0, None, {"error": str(e)}))
        return results

    def _match_ubicacion_single(self, ubicacion: Dict) -> UbicacionMatch:
        tipo_via   = ubicacion.get("tipo_via", "").strip()
        nombre_via = ubicacion.get("nombre_via", "").strip()
        numero     = ubicacion.get("numero", "").strip()

        if not nombre_via:
            return UbicacionMatch(ubicacion, "SIN_COINCIDENCIA", 0.0, None, {})

        exact = self._buscar_ubicacion_exacta(tipo_via, nombre_via, numero)
        if exact:
            return self._build_ubicacion_match(ubicacion, exact, "EXACTO", 1.0)

        alias = self._buscar_ubicacion_alias(nombre_via, numero)
        if alias:
            return self._build_ubicacion_match(ubicacion, alias, "EXACTO", 0.95)

        fuzzy = self._buscar_ubicacion_fuzzy(tipo_via, nombre_via, numero)
        if fuzzy and fuzzy[0][1] >= 0.85:
            return self._build_ubicacion_match(ubicacion, fuzzy[0][0], "PARCIAL", fuzzy[0][1])

        return UbicacionMatch(ubicacion, "SIN_COINCIDENCIA", 0.0, None, {})

    def _buscar_ubicacion_exacta(
        self, tipo_via: str, nombre_via: str, numero: str
    ) -> Optional[Dict]:
        parts  = [
            "SELECT location_id, street_type, street_name, number, canonical_name, "
            "city, postal_code, latitude, longitude FROM locations WHERE 1=1"
        ]
        params: List = []
        if tipo_via:
            parts.append("AND UPPER(street_type)=?");  params.append(tipo_via.upper())
        if nombre_via:
            parts.append("AND UPPER(street_name)=?");  params.append(nombre_via.upper())
        if numero:
            parts.append("AND number=?");              params.append(numero)
        return self._q1(" ".join(parts), tuple(params))

    def _buscar_ubicacion_alias(self, nombre_via: str, numero: str) -> Optional[Dict]:
        return self._q1(
            "SELECT l.location_id, l.street_type, l.street_name, l.number, "
            "l.canonical_name, l.city, l.postal_code, l.latitude, l.longitude "
            "FROM locations l "
            "JOIN location_aliases la ON l.location_id = la.location_id "
            "WHERE UPPER(la.alias_name) LIKE ?",
            (f"%{nombre_via.upper()}%",)
        )

    def _buscar_ubicacion_fuzzy(
        self, tipo_via: str, nombre_via: str, numero: str
    ) -> List[Tuple[Dict, float]]:
        prefijo = self._normalizar_texto(nombre_via)[:3]
        if prefijo:
            filas = self._q(
                "SELECT location_id, street_type, street_name, number, canonical_name, "
                "city, postal_code, latitude, longitude FROM locations "
                "WHERE UPPER(street_name) LIKE ?",
                (f"{prefijo}%",)
            )
        else:
            filas = self._q(
                "SELECT location_id, street_type, street_name, number, canonical_name, "
                "city, postal_code, latitude, longitude FROM locations"
            )

        nombre_norm = self._normalizar_texto(nombre_via)
        candidates  = []
        for loc in filas:
            sim  = fuzz.ratio(nombre_norm, self._normalizar_texto(loc.get('street_name') or '')) / 100.0
            bono = 0.1 if numero and loc.get('number') == numero else 0.0
            conf = min(sim + bono, 1.0)
            if conf >= 0.70:
                candidates.append((loc, conf))

        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[:3]

    def _build_ubicacion_match(
        self, entidad: Dict, db_record: Dict, match_type: str, confidence: float
    ) -> UbicacionMatch:
        enrichment: Dict = {}
        lid = db_record.get('location_id')
        if lid:
            enrichment['apariciones_previas'] = self._count_location_appearances(lid)
            enrichment['eventos_recurrentes'] = self._get_location_event_types(lid)
            enrichment['alias']               = self._get_location_aliases(lid)
        return UbicacionMatch(entidad, match_type, confidence, db_record, enrichment)

    def _count_location_appearances(self, location_id: int) -> int:
        row = self._q1(
            "SELECT COUNT(*) AS c FROM entity_links WHERE entity_type='location' AND entity_id=?",
            (location_id,)
        )
        return row['c'] if row else 0

    def _get_location_event_types(self, location_id: int, limit: int = 5) -> List[str]:
        rows = self._q(
            "SELECT DISTINCT ed.capitulo "
            "FROM events_drag ed "
            "JOIN entity_links el ON ed.event_id = el.source_event_id "
            "WHERE el.entity_type='location' AND el.entity_id=? "
            "ORDER BY ed.fecha_evento DESC LIMIT ?",
            (location_id, limit)
        )
        return [r['capitulo'] for r in rows if r.get('capitulo')]

    def _get_location_aliases(self, location_id: int) -> List[str]:
        rows = self._q(
            "SELECT alias_name FROM location_aliases WHERE location_id=?",
            (location_id,)
        )
        return [r['alias_name'] for r in rows]

    # =========================================================================
    # UTILIDADES
    # =========================================================================

    def _get_persona_by_dni(self, dni: str) -> Optional[Dict]:
        return self._buscar_persona_by_dni(dni)

    @staticmethod
    def _normalizar_matricula(matricula: str) -> str:
        return re.sub(r'[\s\-]', '', matricula).upper() if matricula else ""

    @staticmethod
    def _normalizar_dni(dni: str) -> str:
        return re.sub(r'\s', '', dni).upper() if dni else ""

    @staticmethod
    def _normalizar_texto(texto: str) -> str:
        if not texto:
            return ""
        return re.sub(r'\s+', ' ', unidecode(texto).upper()).strip()


# ---------------------------------------------------------------------------
# Exportación
# ---------------------------------------------------------------------------

def matches_to_dict(matches: Dict) -> Dict:
    """Convertir objetos Match a diccionarios para JSON."""
    return {
        "vehiculos":   [asdict(m) for m in matches.get("vehiculos", [])],
        "personas":    [asdict(m) for m in matches.get("personas", [])],
        "ubicaciones": [asdict(m) for m in matches.get("ubicaciones", [])],
    }
