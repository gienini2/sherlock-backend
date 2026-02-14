"""
SHERLOCK ANNOTATOR - Marcado Semántico Reversible
==================================================

Genera anotaciones JSON con posiciones para marcado en frontend.
100% determinista, sin IA.
"""

import logging
from typing import List, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Anotacion:
    """Representa una anotación en el texto"""
    id: str
    entity: str
    type: str  # VEHICULO, PERSONA, UBICACION
    start: int
    end: int
    match: str  # exact, partial, none
    db_data: Optional[Dict] = None


class AnnotatorService:
    """
    Servicio de anotación de texto con coincidencias de BD.
    
    IMPORTANTE: Este servicio es 100% determinista.
    No usa IA, solo algoritmos de marcado.
    """
    
    def __init__(self):
        logger.info("[ANNOTATOR] Servicio inicializado")
    
    def _crear_anotaciones(
        self,
        matches: Dict,
        texto: str
    ) -> List[Anotacion]:
        """
        Crea lista de anotaciones desde matches del matcher.
        
        Args:
            matches: Resultado del matcher (formato dict)
            texto: Texto original
            
        Returns:
            Lista de objetos Anotacion
        """
        anotaciones = []
        contador_id = 0
        
        # Vehículos
        for vehiculo_match in matches.get("vehiculos", []):
            entidad = vehiculo_match.get("entidad_original", {})
            position = entidad.get("position")
            
            if not position:
                logger.warning(f"Vehículo sin posición: {entidad.get('matricula')}")
                continue
            
            anotacion = Anotacion(
                id=f"v{contador_id}",
                entity=entidad.get("matricula", ""),
                type="VEHICULO",
                start=position["start"],
                end=position["end"],
                match=vehiculo_match.get("match_type", "none"),
                db_data=vehiculo_match.get("db_data")
            )
            
            anotaciones.append(anotacion)
            contador_id += 1
        
        # Personas
        for persona_match in matches.get("personas", []):
            entidad = persona_match.get("entidad_original", {})
            position = entidad.get("position")
            
            if not position:
                logger.warning(f"Persona sin posición: {entidad.get('dni')}")
                continue
            
            # Buscar texto visible (nombre o DNI)
            texto_visible = entidad.get("dni", "")
            if not texto_visible:
                nombre_completo = f"{entidad.get('nombre', '')} {entidad.get('apellidos', '')}".strip()
                texto_visible = nombre_completo
            
            anotacion = Anotacion(
                id=f"p{contador_id}",
                entity=texto_visible,
                type="PERSONA",
                start=position["start"],
                end=position["end"],
                match=persona_match.get("match_type", "none"),
                db_data=persona_match.get("db_data")
            )
            
            anotaciones.append(anotacion)
            contador_id += 1
        
        # Ubicaciones
        for ubicacion_match in matches.get("ubicaciones", []):
            entidad = ubicacion_match.get("entidad_original", {})
            position = entidad.get("position")
            
            if not position:
                continue
            
            anotacion = Anotacion(
                id=f"u{contador_id}",
                entity=entidad.get("texto_completo", ""),
                type="UBICACION",
                start=position["start"],
                end=position["end"],
                match=ubicacion_match.get("match_type", "none"),
                db_data=ubicacion_match.get("db_data")
            )
            
            anotaciones.append(anotacion)
            contador_id += 1
        
        # Ordenar por posición (importante para evitar solapamientos)
        anotaciones.sort(key=lambda a: a.start)
        
        return anotaciones
    
    def anotar_texto(
        self,
        texto: str,
        matches: Dict
    ) -> Dict:
        """
        Genera JSON de anotaciones para marcado en frontend.
        
        Args:
            texto: Texto original
            matches: Resultado del matcher
            
        Returns:
            Dict con:
            {
                "texto_original": str,
                "anotaciones": [
                    {
                        "id": "v0",
                        "entity": "9915GBN",
                        "type": "VEHICULO",
                        "start": 45,
                        "end": 52,
                        "match": "exact",
                        "db_data": {...}
                    },
                    ...
                ]
            }
        """
        logger.info("[ANNOTATOR] Generando anotaciones")
        
        anotaciones = self._crear_anotaciones(matches, texto)
        
        # Convertir a dict serializable
        anotaciones_dict = [
            {
                "id": a.id,
                "entity": a.entity,
                "type": a.type,
                "start": a.start,
                "end": a.end,
                "match": a.match,
                "db_data": a.db_data
            }
            for a in anotaciones
        ]
        
        resultado = {
            "texto_original": texto,
            "anotaciones": anotaciones_dict
        }
        
        logger.info(f"[ANNOTATOR] Generadas {len(anotaciones_dict)} anotaciones")
        
        return resultado
