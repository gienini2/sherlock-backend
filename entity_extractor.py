"""
SHERLOCK ENTITY EXTRACTOR - Extracción con Claude + Posiciones
===============================================================

Extrae entidades de texto policial usando Claude API.
Añade posiciones exactas de texto para marcado semántico.

IMPORTANTE: Este módulo es el ÚNICO punto de IA en el pipeline
de análisis de Sherlock.
"""

import json
import re
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


def cargar_prompt_extractor() -> str:
    """Cargar prompt del sistema para extractor"""
    path = Path(__file__).parent / "extractor_system.txt"
    if not path.exists():
        raise FileNotFoundError(f"Prompt no encontrado: {path}")
    return path.read_text(encoding="utf-8")


def normalizar_texto_busqueda(texto: str) -> str:
    """
    Normaliza texto para búsqueda case-insensitive.
    Preserva el texto original para posiciones exactas.
    """
    return texto.upper().strip()


def buscar_posicion_entidad(
    texto_completo: str,
    entidad_valor: str,
    tipo: str
) -> Optional[Dict]:
    """
    Busca la posición exacta de una entidad en el texto.
    
    Args:
        texto_completo: Texto original completo
        entidad_valor: Valor de la entidad a buscar (ej: "9915GBN")
        tipo: Tipo de entidad (vehiculo, persona, ubicacion)
        
    Returns:
        Dict con start, end, texto_original o None si no se encuentra
    """
    # Normalizar para búsqueda
    texto_upper = normalizar_texto_busqueda(texto_completo)
    entidad_upper = normalizar_texto_busqueda(entidad_valor)
    
    # Buscar posición
    pos = texto_upper.find(entidad_upper)
    
    if pos == -1:
        logger.warning(f"No se encontró '{entidad_valor}' en el texto")
        return None
    
    # Extraer texto original (respeta mayúsculas/minúsculas)
    texto_original = texto_completo[pos:pos + len(entidad_valor)]
    
    return {
        "start": pos,
        "end": pos + len(entidad_valor),
        "texto_original": texto_original
    }


def añadir_posiciones_vehiculos(
    texto: str,
    vehiculos: List[Dict]
) -> List[Dict]:
    """Añade posiciones de texto a vehículos"""
    resultado = []
    
    for vehiculo in vehiculos:
        matricula = vehiculo.get("matricula", "")
        
        if not matricula:
            logger.warning("Vehículo sin matrícula, saltando")
            continue
        
        # Buscar posición
        posicion = buscar_posicion_entidad(texto, matricula, "vehiculo")
        
        # Añadir posición al objeto
        vehiculo_con_posicion = {
            **vehiculo,
            "position": posicion
        }
        
        resultado.append(vehiculo_con_posicion)
    
    return resultado


def añadir_posiciones_personas(
    texto: str,
    personas: List[Dict]
) -> List[Dict]:
    """Añade posiciones de texto a personas"""
    resultado = []
    
    for persona in personas:
        # Buscar por DNI (más preciso)
        dni = persona.get("dni", "")
        
        if dni:
            posicion = buscar_posicion_entidad(texto, dni, "persona")
        else:
            # Fallback: buscar por nombre completo
            nombre_completo = f"{persona.get('nombre', '')} {persona.get('apellidos', '')}".strip()
            if nombre_completo:
                posicion = buscar_posicion_entidad(texto, nombre_completo, "persona")
            else:
                posicion = None
        
        # Añadir posición
        persona_con_posicion = {
            **persona,
            "position": posicion
        }
        
        resultado.append(persona_con_posicion)
    
    return resultado


def añadir_posiciones_ubicaciones(
    texto: str,
    ubicaciones: List[Dict]
) -> List[Dict]:
    """Añade posiciones de texto a ubicaciones"""
    resultado = []
    
    for ubicacion in ubicaciones:
        texto_completo = ubicacion.get("texto_completo", "")
        
        if not texto_completo:
            logger.warning("Ubicación sin texto_completo, saltando")
            continue
        
        posicion = buscar_posicion_entidad(texto, texto_completo, "ubicacion")
        
        ubicacion_con_posicion = {
            **ubicacion,
            "position": posicion
        }
        
        resultado.append(ubicacion_con_posicion)
    
    return resultado


async def extract_entities_claude(
    texto: str,
    anthropic_client
) -> Dict:
    """
    Extrae entidades usando Claude API + añade posiciones.
    
    Este es el ÚNICO punto donde se usa IA en el análisis de Sherlock.
    Todo lo demás (Matcher, Annotator, Explainer) es determinista.
    
    Args:
        texto: Texto policial a analizar (formato DRAG)
        anthropic_client: Cliente Anthropic inicializado
        
    Returns:
        Dict con estructura:
        {
            "vehiculos": [{matricula, marca, modelo, color, position}, ...],
            "personas": [{nombre, apellidos, dni, rol, position}, ...],
            "ubicaciones": [{tipo_via, nombre_via, numero, texto_completo, position}, ...]
        }
    """
    if not anthropic_client:
        raise ValueError("Cliente Anthropic no inicializado")
    
    logger.info("[EXTRACTOR CLAUDE] Iniciando extracción de entidades")
    
    # 1. Cargar prompt del sistema
    prompt_sistema = cargar_prompt_extractor()
    
    # 2. Llamar a Claude
    try:
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            system=prompt_sistema,
            messages=[
                {"role": "user", "content": texto}
            ]
        )
        
        # Extraer respuesta
        response_text = ""
        for block in message.content:
            if block.type == "text":
                response_text += block.text
        
        response_text = response_text.strip()
        
        logger.info(f"[EXTRACTOR CLAUDE] Respuesta recibida: {len(response_text)} chars")
        
    except Exception as e:
        logger.error(f"[EXTRACTOR CLAUDE] Error llamando a Claude: {e}")
        raise
    
    # 3. Limpiar markdown si existe
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    if response_text.startswith("```"):
        response_text = response_text[3:]
    if response_text.endswith("```"):
        response_text = response_text[:-3]
    
    response_text = response_text.strip()
    
    # 4. Parsear JSON
    try:
        entidades_raw = json.loads(response_text)
    except json.JSONDecodeError as e:
        logger.error(f"[EXTRACTOR CLAUDE] Error parseando JSON: {e}")
        logger
