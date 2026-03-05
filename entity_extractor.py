"""
SHERLOCK ENTITY EXTRACTOR - Extracción con Claude + Posiciones
===============================================================

Extrae entidades de texto policial usando Claude API.
Añade posiciones exactas de texto para marcado semántico.

IMPORTANTE: Este módulo es el ÚNICO punto de IA en el pipeline
de análisis de Sherlock.

CAMBIOS v2:
- buscar_posicion_entidad ahora acepta offset para evitar duplicados
- añadir_posiciones_* usan offset acumulativo para múltiples ocurrencias
- Limpieza de markdown más robusta con regex
- extract_entities_regex compilado en módulo para reutilización
"""

import json
import re
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from functools import lru_cache

logger = logging.getLogger(__name__)

# Regex compilados una sola vez a nivel de módulo
PLATE_REGEX = re.compile(r'\b\d{4}[^A-Z0-9]?[A-Z]{3}\b')
DNI_REGEX   = re.compile(r'\b\d{8}[A-Z]\b')
MD_FENCE    = re.compile(r'^```(?:json)?\s*|\s*```$', re.MULTILINE)

# Palabras clave de vía para el extractor regex
_VIA_KEYWORDS = ("carrer", "carretera", "avinguda", "plaça")


@lru_cache(maxsize=1)
def cargar_prompt_extractor() -> str:
    """
    Cargar prompt del sistema para extractor.
    Cacheado: se lee una sola vez por proceso.
    """
    path = Path(__file__).parent / "extractor_system.txt"
    if not path.exists():
        raise FileNotFoundError(f"Prompt no encontrado: {path}")
    return path.read_text(encoding="utf-8")


def normalizar_texto_busqueda(texto: str) -> str:
    """Normaliza texto para búsqueda case-insensitive."""
    return texto.upper().strip()


def buscar_posicion_entidad(
    texto_completo: str,
    entidad_valor: str,
    tipo: str,
    offset: int = 0
) -> Optional[Dict]:
    """
    Busca la posición exacta de una entidad en el texto a partir de offset.

    Args:
        texto_completo: Texto original completo
        entidad_valor:  Valor de la entidad (ej: "9915GBN")
        tipo:           Tipo de entidad (vehiculo, persona, ubicacion) — solo para logging
        offset:         Posición desde la que empezar la búsqueda (evita duplicados)

    Returns:
        Dict con start, end, texto_original o None si no se encuentra
    """
    texto_upper    = normalizar_texto_busqueda(texto_completo)
    entidad_upper  = normalizar_texto_busqueda(entidad_valor)

    pos = texto_upper.find(entidad_upper, offset)

    if pos == -1:
        logger.warning(f"[EXTRACTOR] No se encontró '{entidad_valor}' (tipo={tipo}, offset={offset})")
        return None

    return {
        "start":          pos,
        "end":            pos + len(entidad_valor),
        "texto_original": texto_completo[pos: pos + len(entidad_valor)]
    }


# ---------------------------------------------------------------------------
# Funciones de posicionamiento
# ---------------------------------------------------------------------------

def añadir_posiciones_vehiculos(texto: str, vehiculos: List[Dict]) -> List[Dict]:
    """Añade posiciones de texto a vehículos, manejando matrículas repetidas."""
    resultado: List[Dict] = []
    # Rastrear la última posición encontrada por matrícula
    ultimo_offset: Dict[str, int] = {}

    for vehiculo in vehiculos:
        matricula = vehiculo.get("matricula", "")
        if not matricula:
            logger.warning("[EXTRACTOR] Vehículo sin matrícula, saltando")
            continue

        offset   = ultimo_offset.get(matricula, 0)
        posicion = buscar_posicion_entidad(texto, matricula, "vehiculo", offset)

        if posicion:
            ultimo_offset[matricula] = posicion["end"]

        resultado.append({**vehiculo, "position": posicion})

    return resultado


def añadir_posiciones_personas(texto: str, personas: List[Dict]) -> List[Dict]:
    """Añade posiciones de texto a personas, manejando DNI/nombres repetidos."""
    resultado: List[Dict] = []
    ultimo_offset: Dict[str, int] = {}

    for persona in personas:
        dni = persona.get("dni", "")

        if dni:
            clave    = f"dni:{dni}"
            offset   = ultimo_offset.get(clave, 0)
            posicion = buscar_posicion_entidad(texto, dni, "persona", offset)
        else:
            nombre_completo = f"{persona.get('nombre', '')} {persona.get('apellidos', '')}".strip()
            if nombre_completo:
                clave    = f"nombre:{nombre_completo}"
                offset   = ultimo_offset.get(clave, 0)
                posicion = buscar_posicion_entidad(texto, nombre_completo, "persona", offset)
            else:
                clave    = ""
                posicion = None

        if posicion and clave:
            ultimo_offset[clave] = posicion["end"]

        resultado.append({**persona, "position": posicion})

    return resultado


def añadir_posiciones_ubicaciones(texto: str, ubicaciones: List[Dict]) -> List[Dict]:
    """Añade posiciones de texto a ubicaciones, manejando textos repetidos."""
    resultado: List[Dict] = []
    ultimo_offset: Dict[str, int] = {}

    for ubicacion in ubicaciones:
        texto_ub = ubicacion.get("texto_completo", "")
        if not texto_ub:
            logger.warning("[EXTRACTOR] Ubicación sin texto_completo, saltando")
            continue

        offset   = ultimo_offset.get(texto_ub, 0)
        posicion = buscar_posicion_entidad(texto, texto_ub, "ubicacion", offset)

        if posicion:
            ultimo_offset[texto_ub] = posicion["end"]

        resultado.append({**ubicacion, "position": posicion})

    return resultado


# ---------------------------------------------------------------------------
# Limpieza de respuesta Claude
# ---------------------------------------------------------------------------

def _limpiar_markdown(text: str) -> str:
    """Elimina fences de markdown (```json … ```) de forma robusta."""
    return MD_FENCE.sub("", text).strip()


# ---------------------------------------------------------------------------
# Extractor principal (Claude)
# ---------------------------------------------------------------------------

async def extract_entities_claude(texto: str, anthropic_client) -> Dict:
    """
    Extrae entidades usando Claude API + añade posiciones.

    Este es el ÚNICO punto donde se usa IA en el análisis de Sherlock.
    Todo lo demás (Matcher, Annotator, Explainer) es determinista.

    Args:
        texto:            Texto policial a analizar (formato DRAG)
        anthropic_client: Cliente Anthropic inicializado

    Returns:
        {
            "vehiculos":  [{matricula, marca, modelo, color, position}, ...],
            "personas":   [{nombre, apellidos, dni, rol, position}, ...],
            "ubicaciones":[{tipo_via, nombre_via, numero, texto_completo, position}, ...]
        }
    """
    if not anthropic_client:
        raise ValueError("Cliente Anthropic no inicializado")

    logger.info("[EXTRACTOR CLAUDE] Iniciando extracción de entidades")

    prompt_sistema = cargar_prompt_extractor()

    # --- Llamada a Claude ---
    try:
        message = anthropic_client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1500,
            system=prompt_sistema,
            messages=[{"role": "user", "content": texto}]
        )
        response_text = "".join(
            block.text for block in message.content if block.type == "text"
        ).strip()
        logger.info(f"[EXTRACTOR CLAUDE] Respuesta recibida: {len(response_text)} chars")
    except Exception as e:
        logger.error(f"[EXTRACTOR CLAUDE] Error llamando a Claude: {e}")
        raise

    # --- Limpiar markdown ---
    response_text = _limpiar_markdown(response_text)

    # --- Parsear JSON ---
    try:
        entidades_raw = json.loads(response_text)
    except json.JSONDecodeError as e:
        logger.error(f"[EXTRACTOR CLAUDE] Error parseando JSON: {e}")
        logger.error(f"[EXTRACTOR CLAUDE] JSON recibido: {response_text[:500]}")
        raise

    # --- Añadir posiciones ---
    vehiculos  = entidades_raw.get("vehiculos", [])
    personas   = entidades_raw.get("personas", [])
    ubicaciones = entidades_raw.get("ubicaciones", [])

    resultado = {
        "vehiculos":   añadir_posiciones_vehiculos(texto, vehiculos),
        "personas":    añadir_posiciones_personas(texto, personas),
        "ubicaciones": añadir_posiciones_ubicaciones(texto, ubicaciones),
    }

    logger.info(
        f"[EXTRACTOR CLAUDE] Completado — "
        f"{len(resultado['vehiculos'])} vehículos, "
        f"{len(resultado['personas'])} personas, "
        f"{len(resultado['ubicaciones'])} ubicaciones"
    )
    return resultado


# ---------------------------------------------------------------------------
# Extractor REGEX (fallback ligero para /check-entities)
# ---------------------------------------------------------------------------

def extract_entities_regex(text: str) -> Dict:
    """
    Extractor per regex (SIN Claude). Retorna entitats AMB posicions.

    USOS:
    - Pas 1 de Sherlock: detecció ràpida antes de Claude
    - Endpoint /check (feedback immediat mentre l'agent escriu)

    Detecta:
    - Matrícules (format espanyol: 1234ABC o ABC1234)
    - DNIs (8 dígits + lletra)
    - Noms propis (heurística: 2-4 paraules capitalitzades)
    - Ubicacions (línies amb paraules clau de via)
    """
    text_upper = text.upper()

    # ------------------------------------------------------------------
    # VEHICLES — matrícules amb posició
    # ------------------------------------------------------------------
    vehicles = []
    seen_plates = set()
    for m in PLATE_REGEX.finditer(text_upper):
        plate = re.sub(r'[^A-Z0-9]', '', m.group())
        if plate in seen_plates:
            continue
        seen_plates.add(plate)
        vehicles.append({
            "matricula": plate,
            "marca":     "",
            "modelo":    "",
            "color":     "",
            "position":  {"start": m.start(), "end": m.end()},
        })

    # ------------------------------------------------------------------
    # PERSONES — DNIs amb posició
    # ------------------------------------------------------------------
    persons = []
    seen_dnis = set()
    for m in DNI_REGEX.finditer(text_upper):
        dni = m.group()
        if dni in seen_dnis:
            continue
        seen_dnis.add(dni)
        persons.append({
            "nombre":    "",
            "apellidos": "",
            "dni":       dni,
            "rol":       "",
            "position":  {"start": m.start(), "end": m.end()},
        })

    # ------------------------------------------------------------------
    # NOMS PROPIS — heurística: 2-4 paraules capitalitzades consecutives
    # Exclou paraules funcionals i inici de frase genèric
    # ------------------------------------------------------------------
    _STOPWORDS = {
        "Ha", "He", "Han", "Que", "Una", "Uns", "Les", "Els",
        "Del", "Dels", "Per", "Com", "Amb", "Hem", "Han",
        "Ara", "Quan", "Fins", "Desde", "Sobre",
    }
    NOM_RE = re.compile(
        r'\b([A-ZÁÉÍÓÚÀÈÌÒÙÜÏÑ][a-záéíóúàèìòùüïñ]+(?:\s+[A-ZÁÉÍÓÚÀÈÌÒÙÜÏÑ][a-záéíóúàèìòùüïñ]+){1,3})\b'
    )
    seen_names = set()
    for m in NOM_RE.finditer(text):
        parts = m.group().split()
        if parts[0] in _STOPWORDS:
            continue
        key = m.group().upper()
        if key in seen_names:
            continue
        seen_names.add(key)
        persons.append({
            "nombre":    parts[0],
            "apellidos": " ".join(parts[1:]),
            "dni":       "",
            "rol":       "",
            "position":  {"start": m.start(), "end": m.end()},
        })

    # ------------------------------------------------------------------
    # UBICACIONS — línies/segments amb paraules clau de via
    # ------------------------------------------------------------------
    locations = []
    VIA_RE = re.compile(
        r'\b(carrer|carretera|avinguda|plaça|passatge|ronda|via|passeig|calle|avenida|plaza)\b'
        r'[^.,\n]{3,60}',
        re.IGNORECASE
    )
    seen_locs = set()
    for m in VIA_RE.finditer(text):
        loc = m.group().strip()
        key = loc.upper()
        if key in seen_locs:
            continue
        seen_locs.add(key)
        locations.append({
            "tipo_via":      m.group().split()[0].lower(),
            "nombre_via":    " ".join(m.group().split()[1:]),
            "numero":        "",
            "texto_completo": loc,
            "position":      {"start": m.start(), "end": m.end()},
        })

    return {
        "vehicles":  vehicles,
        "persons":   persons,
        "locations": locations,
    }
