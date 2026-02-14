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
"""

import sqlite3
import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

# Configuración de logging PRIMERO
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Librerías de similitud
try:
    from rapidfuzz import fuzz
    from unidecode import unidecode
    FUZZY_AVAILABLE = True
except ImportError:
    logger.warning("rapidfuzz o unidecode no instalados. Usando fuzzy matching básico.")
    FUZZY_AVAILABLE = False
    
    # Implementación básica de Levenshtein
    def levenshtein_distance(s1: str, s2: str) -> int:
        """Distancia de Levenshtein básica"""
        if len(s1) < len(s2):
            return levenshtein_distance(s2, s1)
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def fuzz_ratio_basic(s1: str, s2: str) -> float:
        """Ratio de similitud básico (0-100)"""
        if not s1 or not s2:
            return 0.0
        max_len = max(len(s1), len(s2))
        if max_len == 0:
            return 100.0
        distance = levenshtein_distance(s1.lower(), s2.lower())
        return ((max_len - distance) / max_len) * 100
    
    def unidecode_basic(s: str) -> str:
        """Normalización básica sin librería"""
        # Mapeo simple de caracteres comunes (ambos strings deben tener igual longitud)
        trans = str.maketrans(
            'áéíóúàèìòùäëïöüâêîôûñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÂÊÎÔÛÑÇ',
            'aeiouaeiouaeiouaeiouncrAEIOUAEIOUAEIOUAEIOUNCR'  # Añadida R al final
        )
        return s.translate(trans)
    
    # Crear objeto compatible
    class FuzzBasic:
        @staticmethod
        def ratio(s1: str, s2: str) -> float:
            return fuzz_ratio_basic(s1, s2)
    
    fuzz = FuzzBasic()
    unidecode = unidecode_basic


@dataclass
class VehiculoMatch:
    """Resultado de match de vehículo"""
    entidad_original: Dict
    match_type: str  # EXACTO, PARCIAL, SIN_COINCIDENCIA
    confidence: float
    db_record: Optional[Dict]
    enrichment: Dict


@dataclass
class PersonaMatch:
    """Resultado de match de persona"""
    entidad_original: Dict
    match_type: str
    confidence: float
    db_record: Optional[Dict]
    enrichment: Dict


@dataclass
class UbicacionMatch:
    """Resultado de match de ubicación"""
    entidad_original: Dict
    match_type: str
    confidence: float
    db_record: Optional[Dict]
    enrichment: Dict


class DatabaseError(Exception):
    """Error de acceso a base de datos"""
    pass


class MatcherService:
    """
    Servicio de contraste determinista.
    Responsabilidad única: consultar BD y calcular similitudes.
    """
    
    def __init__(self, db_path: str):
        """
        Args:
            db_path: Ruta a hermano_mayor.db
        """
        self.db_path = db_path
        self._validate_db()
    @property
    def db_adapter(self):
           """Expone el adaptador de BD para el explainer"""
          return self._db 
                
    def _validate_db(self):
        """Validar que la BD existe y tiene el esquema esperado"""
        if not Path(self.db_path).exists():
            raise DatabaseError(f"Base de datos no encontrada: {self.db_path}")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = {row[0] for row in cursor.fetchall()}
                
                required = {'vehicles', 'persons', 'locations'}
                missing = required - tables
                if missing:
                    raise DatabaseError(f"Tablas faltantes: {missing}")
                    
                logger.info(f"BD validada: {len(tables)} tablas encontradas")
        except sqlite3.Error as e:
            raise DatabaseError(f"Error validando BD: {e}")
    @property
    def db_adapter(self):
            """
            Expone un adaptador de BD para uso del explainer.
            Crea una instancia temporal de HermanoMayorDB.
            """
            from db_adapter import HermanoMayorDB
            return HermanoMayorDB(self.db_path)
        
    def contrastar_entidades(self, entidades: Dict) -> Dict:
        """
        Entrada principal del servicio.
        
        Args:
            entidades: {
                "vehiculos": [...],
                "personas": [...],
                "ubicaciones": [...]
            }
            
        Returns:
            {
                "matches": {
                    "vehiculos": [VehiculoMatch, ...],
                    "personas": [PersonaMatch, ...],
                    "ubicaciones": [UbicacionMatch, ...]
                }
            }
        """
        logger.info(f"Iniciando contraste de entidades")
        
        matches = {
            "vehiculos": [],
            "personas": [],
            "ubicaciones": []
        }
        
        # Procesar cada tipo de entidad
        if "vehiculos" in entidades:
            matches["vehiculos"] = self._match_vehiculos(entidades["vehiculos"])
            
        if "personas" in entidades:
            matches["personas"] = self._match_personas(entidades["personas"])
            
        if "ubicaciones" in entidades:
            matches["ubicaciones"] = self._match_ubicaciones(entidades["ubicaciones"])
        
        logger.info(f"Contraste completado: {len(matches['vehiculos'])} vehículos, "
                   f"{len(matches['personas'])} personas, {len(matches['ubicaciones'])} ubicaciones")
        
        return {"matches": matches}
    
    # =========================================================================
    # MATCHING DE VEHÍCULOS
    # =========================================================================
    
    def _match_vehiculos(self, vehiculos: List[Dict]) -> List[VehiculoMatch]:
        """Contrastar lista de vehículos con BD"""
        results = []
        
        for vehiculo in vehiculos:
            try:
                match = self._match_vehiculo_single(vehiculo)
                results.append(match)
            except Exception as e:
                logger.error(f"Error procesando vehículo {vehiculo}: {e}")
                # Devolver sin coincidencia en caso de error
                results.append(VehiculoMatch(
                    entidad_original=vehiculo,
                    match_type="ERROR",
                    confidence=0.0,
                    db_record=None,
                    enrichment={"error": str(e)}
                ))
        
        return results
    
    def _match_vehiculo_single(self, vehiculo: Dict) -> VehiculoMatch:
        """Contrastar un vehículo individual"""
        matricula = vehiculo.get("matricula", "").strip()
        marca = vehiculo.get("marca", "").strip()
        modelo = vehiculo.get("modelo", "").strip()
        
        if not matricula:
            return VehiculoMatch(
                entidad_original=vehiculo,
                match_type="SIN_COINCIDENCIA",
                confidence=0.0,
                db_record=None,
                enrichment={}
            )
        
        # Normalizar matrícula
        matricula_norm = self._normalizar_matricula(matricula)
        
        # 1. Búsqueda EXACTA
        exact_match = self._buscar_vehiculo_exacto(matricula_norm)
        if exact_match:
            return self._build_vehiculo_match(vehiculo, exact_match, "EXACTO", 1.0)
        
        # 2. Búsqueda FUZZY
        fuzzy_matches = self._buscar_vehiculo_fuzzy(matricula_norm, marca, modelo)
        if fuzzy_matches:
            best_match, confidence = fuzzy_matches[0]
            if confidence >= 0.85:
                return self._build_vehiculo_match(vehiculo, best_match, "PARCIAL", confidence)
        
        # 3. Sin coincidencia
        return VehiculoMatch(
            entidad_original=vehiculo,
            match_type="SIN_COINCIDENCIA",
            confidence=0.0,
            db_record=None,
            enrichment={}
        )
    
    def _buscar_vehiculo_exacto(self, matricula_norm: str) -> Optional[Dict]:
        """Búsqueda exacta por matrícula"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT vehicle_id, plate, brand, model, dni_titular
                FROM vehicles
                WHERE UPPER(REPLACE(REPLACE(plate, ' ', ''), '-', '')) = ?
            """, (matricula_norm,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def _buscar_vehiculo_fuzzy(self, matricula_norm: str, marca: str, modelo: str) -> List[Tuple[Dict, float]]:
        """Búsqueda fuzzy por matrícula + marca/modelo"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Traer todos los vehículos (en BD pequeña es viable, para BD grande usar LIKE)
            cursor.execute("""
                SELECT vehicle_id, plate, brand, model, dni_titular
                FROM vehicles
            """)
            
            candidates = []
            for row in cursor.fetchall():
                vehicle = dict(row)
                
                # Calcular similitud de matrícula
                plate_norm = self._normalizar_matricula(vehicle['plate'])
                sim_plate = fuzz.ratio(matricula_norm, plate_norm) / 100.0
                
                # Calcular similitud de marca/modelo (opcional)
                sim_brand = 0.0
                sim_model = 0.0
                if marca and vehicle['brand']:
                    sim_brand = fuzz.ratio(
                        self._normalizar_texto(marca),
                        self._normalizar_texto(vehicle['brand'])
                    ) / 100.0
                if modelo and vehicle['model']:
                    sim_model = fuzz.ratio(
                        self._normalizar_texto(modelo),
                        self._normalizar_texto(vehicle['model'])
                    ) / 100.0
                
                # Confidence ponderado
                # Prioridad: matrícula (70%), marca (15%), modelo (15%)
                confidence = (sim_plate * 0.7) + (sim_brand * 0.15) + (sim_model * 0.15)
                
                if confidence >= 0.7:  # Umbral mínimo
                    candidates.append((vehicle, confidence))
            
            # Ordenar por confidence descendente
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[:3]  # Top 3
    
    def _build_vehiculo_match(self, entidad: Dict, db_record: Dict, match_type: str, confidence: float) -> VehiculoMatch:
        """Construir resultado de match con enriquecimiento"""
        enrichment = {}
        
        # Obtener titular
        if db_record.get('dni_titular'):
            titular = self._get_persona_by_dni(db_record['dni_titular'])
            if titular:
                enrichment['titular'] = {
                    'dni': titular['dni'],
                    'nombre': titular['nombre'],
                    'apellidos': titular['apellidos']
                }
        
        # Obtener apariciones previas
        vehicle_id = db_record.get('vehicle_id')
        if vehicle_id:
            enrichment['apariciones_previas'] = self._count_vehicle_appearances(vehicle_id)
            enrichment['eventos_previos'] = self._get_vehicle_events(vehicle_id)
            enrichment['conductores_habituales'] = self._get_vehicle_drivers(vehicle_id)
        
        return VehiculoMatch(
            entidad_original=entidad,
            match_type=match_type,
            confidence=confidence,
            db_record=db_record,
            enrichment=enrichment
        )
    
    def _count_vehicle_appearances(self, vehicle_id: int) -> int:
        """Contar apariciones previas de un vehículo"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM entity_links
                WHERE entity_type = 'vehicle' AND entity_id = ?
            """, (vehicle_id,))
            return cursor.fetchone()[0]
    
    def _get_vehicle_events(self, vehicle_id: int, limit: int = 5) -> List[str]:
        """Obtener IDs de eventos previos"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT source_event_id FROM entity_links
                WHERE entity_type = 'vehicle' AND entity_id = ?
                ORDER BY created_at_ts DESC
                LIMIT ?
            """, (vehicle_id, limit))
            return [str(row[0]) for row in cursor.fetchall()]
    
    def _get_vehicle_drivers(self, vehicle_id: int) -> List[Dict]:
        """Obtener conductores habituales conocidos"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT p.dni, p.nombre, p.apellidos, vpl.confidence, vpl.relation_type
                FROM vehicle_person_links vpl
                JOIN persons p ON vpl.person_id = p.dni
                WHERE vpl.vehicle_id = ?
                ORDER BY vpl.confidence DESC
                LIMIT 3
            """, (vehicle_id,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # MATCHING DE PERSONAS
    # =========================================================================
    
    def _match_personas(self, personas: List[Dict]) -> List[PersonaMatch]:
        """Contrastar lista de personas con BD"""
        results = []
        
        for persona in personas:
            try:
                match = self._match_persona_single(persona)
                results.append(match)
            except Exception as e:
                logger.error(f"Error procesando persona {persona}: {e}")
                results.append(PersonaMatch(
                    entidad_original=persona,
                    match_type="ERROR",
                    confidence=0.0,
                    db_record=None,
                    enrichment={"error": str(e)}
                ))
        
        return results
    
    def _match_persona_single(self, persona: Dict) -> PersonaMatch:
        """Contrastar una persona individual"""
        dni = persona.get("dni", "").strip()
        nombre = persona.get("nombre", "").strip()
        apellidos = persona.get("apellidos", "").strip()
        
        # 1. Búsqueda por DNI (más confiable)
        if dni:
            exact_match = self._buscar_persona_by_dni(dni)
            if exact_match:
                return self._build_persona_match(persona, exact_match, "EXACTO", 1.0)
        
        # 2. Búsqueda por nombre+apellidos (fuzzy)
        if nombre or apellidos:
            fuzzy_matches = self._buscar_persona_by_nombre(nombre, apellidos)
            if fuzzy_matches:
                best_match, confidence = fuzzy_matches[0]
                if confidence >= 0.85:
                    return self._build_persona_match(persona, best_match, "PARCIAL", confidence)
        
        # 3. Sin coincidencia
        return PersonaMatch(
            entidad_original=persona,
            match_type="SIN_COINCIDENCIA",
            confidence=0.0,
            db_record=None,
            enrichment={}
        )
    
    def _buscar_persona_by_dni(self, dni: str) -> Optional[Dict]:
        """Búsqueda exacta por DNI"""
        dni_norm = self._normalizar_dni(dni)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT dni, nombre, apellidos, direccion, telefono, 
                       fecha_nacimiento, sexo, observaciones
                FROM persons
                WHERE UPPER(REPLACE(dni, ' ', '')) = ?
            """, (dni_norm,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def _buscar_persona_by_nombre(self, nombre: str, apellidos: str) -> List[Tuple[Dict, float]]:
        """Búsqueda fuzzy por nombre y apellidos"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT dni, nombre, apellidos, direccion, telefono,
                       fecha_nacimiento, sexo, observaciones
                FROM persons
            """)
            
            candidates = []
            for row in cursor.fetchall():
                person = dict(row)
                
                # Similitud de nombre
                sim_nombre = 0.0
                if nombre and person['nombre']:
                    sim_nombre = fuzz.ratio(
                        self._normalizar_texto(nombre),
                        self._normalizar_texto(person['nombre'])
                    ) / 100.0
                
                # Similitud de apellidos
                sim_apellidos = 0.0
                if apellidos and person['apellidos']:
                    sim_apellidos = fuzz.ratio(
                        self._normalizar_texto(apellidos),
                        self._normalizar_texto(person['apellidos'])
                    ) / 100.0
                
                # Confidence promedio (nombre 40%, apellidos 60%)
                confidence = (sim_nombre * 0.4) + (sim_apellidos * 0.6)
                
                if confidence >= 0.7:
                    candidates.append((person, confidence))
            
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[:3]
    
    def _build_persona_match(self, entidad: Dict, db_record: Dict, match_type: str, confidence: float) -> PersonaMatch:
        """Construir resultado de match con enriquecimiento"""
        enrichment = {}
        
        dni = db_record.get('dni')
        if dni:
            # Apariciones previas
            enrichment['apariciones_previas'] = self._count_person_appearances(dni)
            
            # Roles previos
            enrichment['roles_previos'] = self._get_person_roles(dni)
            
            # Vehículos relacionados
            enrichment['vehiculos_relacionados'] = self._get_person_vehicles(dni)
        
        return PersonaMatch(
            entidad_original=entidad,
            match_type=match_type,
            confidence=confidence,
            db_record=db_record,
            enrichment=enrichment
        )
    
    def _count_person_appearances(self, dni: str) -> int:
        """Contar apariciones previas de una persona"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM entity_links
                WHERE entity_type = 'person' AND entity_id = ?
            """, (dni,))
            return cursor.fetchone()[0]
    
    def _get_person_roles(self, dni: str, limit: int = 5) -> List[str]:
        """Obtener roles previos de una persona"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT role FROM person_roles
                WHERE dni = ?
                ORDER BY created_at DESC
                LIMIT ?
            """, (dni, limit))
            return [row[0] for row in cursor.fetchall()]
    
    def _get_person_vehicles(self, dni: str) -> List[Dict]:
        """Obtener vehículos relacionados con persona"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT v.plate, v.brand, v.model, vpl.relation_type
                FROM vehicle_person_links vpl
                JOIN vehicles v ON vpl.vehicle_id = v.vehicle_id
                WHERE vpl.person_id = ?
                LIMIT 5
            """, (dni,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # MATCHING DE UBICACIONES
    # =========================================================================
    
    def _match_ubicaciones(self, ubicaciones: List[Dict]) -> List[UbicacionMatch]:
        """Contrastar lista de ubicaciones con BD"""
        results = []
        
        for ubicacion in ubicaciones:
            try:
                match = self._match_ubicacion_single(ubicacion)
                results.append(match)
            except Exception as e:
                logger.error(f"Error procesando ubicación {ubicacion}: {e}")
                results.append(UbicacionMatch(
                    entidad_original=ubicacion,
                    match_type="ERROR",
                    confidence=0.0,
                    db_record=None,
                    enrichment={"error": str(e)}
                ))
        
        return results
    
    def _match_ubicacion_single(self, ubicacion: Dict) -> UbicacionMatch:
        """Contrastar una ubicación individual"""
        tipo_via = ubicacion.get("tipo_via", "").strip()
        nombre_via = ubicacion.get("nombre_via", "").strip()
        numero = ubicacion.get("numero", "").strip()
        
        if not nombre_via:
            return UbicacionMatch(
                entidad_original=ubicacion,
                match_type="SIN_COINCIDENCIA",
                confidence=0.0,
                db_record=None,
                enrichment={}
            )
        
        # 1. Búsqueda exacta
        exact_match = self._buscar_ubicacion_exacta(tipo_via, nombre_via, numero)
        if exact_match:
            return self._build_ubicacion_match(ubicacion, exact_match, "EXACTO", 1.0)
        
        # 2. Búsqueda por alias
        alias_match = self._buscar_ubicacion_alias(nombre_via, numero)
        if alias_match:
            return self._build_ubicacion_match(ubicacion, alias_match, "EXACTO", 0.95)
        
        # 3. Búsqueda fuzzy
        fuzzy_matches = self._buscar_ubicacion_fuzzy(tipo_via, nombre_via, numero)
        if fuzzy_matches:
            best_match, confidence = fuzzy_matches[0]
            if confidence >= 0.85:
                return self._build_ubicacion_match(ubicacion, best_match, "PARCIAL", confidence)
        
        # 4. Sin coincidencia
        return UbicacionMatch(
            entidad_original=ubicacion,
            match_type="SIN_COINCIDENCIA",
            confidence=0.0,
            db_record=None,
            enrichment={}
        )
    
    def _buscar_ubicacion_exacta(self, tipo_via: str, nombre_via: str, numero: str) -> Optional[Dict]:
        """Búsqueda exacta de ubicación"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = """
                SELECT location_id, street_type, street_name, number, canonical_name,
                       city, postal_code, latitude, longitude
                FROM locations
                WHERE 1=1
            """
            params = []
            
            if tipo_via:
                query += " AND UPPER(street_type) = ?"
                params.append(tipo_via.upper())
            
            if nombre_via:
                query += " AND UPPER(street_name) = ?"
                params.append(nombre_via.upper())
            
            if numero:
                query += " AND number = ?"
                params.append(numero)
            
            cursor.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def _buscar_ubicacion_alias(self, nombre_via: str, numero: str) -> Optional[Dict]:
        """Búsqueda por alias de ubicación"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT l.location_id, l.street_type, l.street_name, l.number, 
                       l.canonical_name, l.city, l.postal_code, l.latitude, l.longitude
                FROM locations l
                JOIN location_aliases la ON l.location_id = la.location_id
                WHERE UPPER(la.alias_name) LIKE ?
            """, (f"%{nombre_via.upper()}%",))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def _buscar_ubicacion_fuzzy(self, tipo_via: str, nombre_via: str, numero: str) -> List[Tuple[Dict, float]]:
        """Búsqueda fuzzy de ubicaciones"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT location_id, street_type, street_name, number, canonical_name,
                       city, postal_code, latitude, longitude
                FROM locations
            """)
            
            candidates = []
            for row in cursor.fetchall():
                location = dict(row)
                
                # Similitud de nombre de vía
                sim_name = fuzz.ratio(
                    self._normalizar_texto(nombre_via),
                    self._normalizar_texto(location['street_name'] or "")
                ) / 100.0
                
                # Bonus si el número coincide
                bonus_numero = 0.1 if numero and location['number'] == numero else 0.0
                
                confidence = sim_name + bonus_numero
                
                if confidence >= 0.7:
                    candidates.append((location, min(confidence, 1.0)))
            
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[:3]
    
    def _build_ubicacion_match(self, entidad: Dict, db_record: Dict, match_type: str, confidence: float) -> UbicacionMatch:
        """Construir resultado de match con enriquecimiento"""
        enrichment = {}
        
        location_id = db_record.get('location_id')
        if location_id:
            # Apariciones previas
            enrichment['apariciones_previas'] = self._count_location_appearances(location_id)
            
            # Tipos de eventos recurrentes
            enrichment['eventos_recurrentes'] = self._get_location_event_types(location_id)
            
            # Alias conocidos
            enrichment['alias'] = self._get_location_aliases(location_id)
        
        return UbicacionMatch(
            entidad_original=entidad,
            match_type=match_type,
            confidence=confidence,
            db_record=db_record,
            enrichment=enrichment
        )
    
    def _count_location_appearances(self, location_id: int) -> int:
        """Contar apariciones previas de ubicación"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM entity_links
                WHERE entity_type = 'location' AND entity_id = ?
            """, (location_id,))
            return cursor.fetchone()[0]
    
    def _get_location_event_types(self, location_id: int, limit: int = 5) -> List[str]:
        """Obtener tipos de eventos recurrentes en ubicación"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT ed.capitulo
                FROM events_drag ed
                JOIN entity_links el ON ed.event_id = el.source_event_id
                WHERE el.entity_type = 'location' AND el.entity_id = ?
                ORDER BY ed.fecha_evento DESC
                LIMIT ?
            """, (location_id, limit))
            return [row[0] for row in cursor.fetchall() if row[0]]
    
    def _get_location_aliases(self, location_id: int) -> List[str]:
        """Obtener alias de ubicación"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT alias_name FROM location_aliases
                WHERE location_id = ?
            """, (location_id,))
            return [row[0] for row in cursor.fetchall()]
    
    # =========================================================================
    # UTILIDADES
    # =========================================================================
    
    def _get_persona_by_dni(self, dni: str) -> Optional[Dict]:
        """Obtener persona completa por DNI (utilidad interna)"""
        return self._buscar_persona_by_dni(dni)
    
    @staticmethod
    def _normalizar_matricula(matricula: str) -> str:
        """Normalizar matrícula: sin espacios, sin guiones, mayúsculas"""
        if not matricula:
            return ""
        return re.sub(r'[\s\-]', '', matricula).upper()
    
    @staticmethod
    def _normalizar_dni(dni: str) -> str:
        """Normalizar DNI: sin espacios, mayúsculas"""
        if not dni:
            return ""
        return re.sub(r'\s', '', dni).upper()
    
    @staticmethod
    def _normalizar_texto(texto: str) -> str:
        """Normalizar texto: sin acentos, mayúsculas, sin espacios múltiples"""
        if not texto:
            return ""
        normalized = unidecode(texto).upper()
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized


def matches_to_dict(matches: Dict) -> Dict:
    """Convertir objetos Match a diccionarios para JSON"""
    return {
        "vehiculos": [asdict(m) for m in matches.get("vehiculos", [])],
        "personas": [asdict(m) for m in matches.get("personas", [])],
        "ubicaciones": [asdict(m) for m in matches.get("ubicaciones", [])]
    }
