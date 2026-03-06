"""
SHERLOCK ENTITY EXTRACTOR - Extracción con Claude + Posiciones
===============================================================
CAMBIOS v2.1:
- Stopwords ampliadas con palabras funcionales castellano + catalán
  para evitar detectar "Le Han Robado El", "Ha Venido Y Ha Dicho"
  como nombres propios.
- buscar_posicion_entidad con offset para evitar duplicados
- Limpieza de markdown robusta
"""

import json
import re
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from functools import lru_cache

logger = logging.getLogger(__name__)

PLATE_REGEX = re.compile(r'\b\d{4}[^A-Z0-9]?[A-Z]{3}\b')
DNI_REGEX   = re.compile(r'\b\d{8}[A-Z]\b')
MD_FENCE    = re.compile(r'^```(?:json)?\s*|\s*```$', re.MULTILINE)

_VIA_KEYWORDS = ("carrer", "carretera", "avinguda", "plaça")

# ---------------------------------------------------------------------------
# STOPWORDS — palabras funcionales que NUNCA son nombre propio
# Castellano + Catalán. Todas en forma capitalizada (como las ve el regex).
# ---------------------------------------------------------------------------
_STOPWORDS = {
    # Castellano - verbos auxiliares y pronombres
    "Ha", "Han", "Hay", "He", "Has", "Hemos", "Habeis",
    "Era", "Son", "Fue", "Ser", "Ser",
    "Le", "Les", "Los", "Las", "Les",
    "Me", "Te", "Se", "Nos", "Os",
    # Castellano - artículos y determinantes
    "El", "La", "Los", "Las", "Una", "Uno", "Unos", "Unas",
    "Del", "Al",
    # Castellano - preposiciones y conjunciones
    "De", "En", "Con", "Por", "Para", "Sin", "Sobre", "Bajo",
    "Que", "Qui", "Como", "Cuando", "Donde", "Porque", "Pero",
    "Y", "Ni", "O", "U",
    "Si", "No", "Ya", "Muy", "Mas", "Tan",
    # Castellano - pronombres demostrativos/indefinidos
    "Este", "Esta", "Estos", "Estas", "Ese", "Esa",
    "Aquel", "Aquella", "Algo", "Alguien", "Nadie", "Nada",
    "Todo", "Toda", "Todos", "Todas",
    # Castellano - verbos frecuentes conjugados
    "Fue", "Han", "Hay", "Dijo", "Vino", "Iba", "Puso",
    "Llegó", "Salió", "Entró", "Dio", "Hizo", "Vio",
    "Venido", "Dicho", "Pegado", "Robado", "Tiron",
    # Catalán - articles i determinants
    "Els", "Les", "Del", "Dels", "Cal", "Can",
    # Catalán - pronoms i conjuncions
    "Que", "Qui", "Com", "Quan", "Fins", "Sobre",
    "Amb", "Per", "Hem", "Han", "Ara", "Desde",
    "Una", "Uns", "Una", "Unes",
    # Catalán - verbs
    "Ha", "Han", "Hem", "Heu", "Era", "Van", "Vai",
    # Palabras específicas del contexto policial que no son nombres
    "Bolso", "Bossa", "Casa", "Calle", "Carrer", "Comissaria",
    "Tiron", "Robo", "Robado", "Furt", "Robatori",
}


@lru_cache(maxsize=1)
def cargar_prompt_extractor() -> str:
    path = Path(__file__).parent / "extractor_system.txt"
    if not path.exists():
        raise FileNotFoundError(f"Prompt no encontrado: {path}")
    return path.read_text(encoding="utf-8")


def normalizar_texto_busqueda(texto: str) -> str:
    return texto.upper().strip()


def buscar_posicion_entidad(
    texto_completo: str,
    entidad_valor: str,
    tipo: str,
    offset: int = 0
) -> Optional[Dict]:
    texto_upper   = normalizar_texto_busqueda(texto_completo)
    entidad_upper = normalizar_texto_busqueda(entidad_valor)
    pos = texto_upper.find(entidad_upper, offset)
    if pos == -1:
        logger.warning(f"[EXTRACTOR] No se encontró '{entidad_valor}' (tipo={tipo}, offset={offset})")
        return None
    return {
        "start":          pos,
        "end":            pos + len(entidad_valor),
        "texto_original": texto_completo[pos: pos + len(entidad_valor)]
    }


def añadir_posiciones_vehiculos(texto: str, vehiculos: List[Dict]) -> List[Dict]:
    resultado: List[Dict] = []
    ultimo_offset: Dict[str, int] = {}
    for vehiculo in vehiculos:
        matricula = vehiculo.get("matricula", "")
        if not matricula:
            continue
        offset   = ultimo_offset.get(matricula, 0)
        posicion = buscar_posicion_entidad(texto, matricula, "vehiculo", offset)
        if posicion:
            ultimo_offset[matricula] = posicion["end"]
        resultado.append({**vehiculo, "position": posicion})
    return resultado


def añadir_posiciones_personas(texto: str, personas: List[Dict]) -> List[Dict]:
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
    resultado: List[Dict] = []
    ultimo_offset: Dict[str, int] = {}
    for ubicacion in ubicaciones:
        texto_ub = ubicacion.get("texto_completo", "")
        if not texto_ub:
            continue
        offset   = ultimo_offset.get(texto_ub, 0)
        posicion = buscar_posicion_entidad(texto, texto_ub, "ubicacion", offset)
        if posicion:
            ultimo_offset[texto_ub] = posicion["end"]
        resultado.append({**ubicacion, "position": posicion})
    return resultado


def _limpiar_markdown(text: str) -> str:
    return MD_FENCE.sub("", text).strip()


async def extract_entities_claude(texto: str, anthropic_client) -> Dict:
    if not anthropic_client:
        raise ValueError("Cliente Anthropic no inicializado")
    logger.info("[EXTRACTOR CLAUDE] Iniciando extracción de entidades")
    prompt_sistema = cargar_prompt_extractor()
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
    except Exception as e:
        logger.error(f"[EXTRACTOR CLAUDE] Error llamando a Claude: {e}")
        raise
    response_text = _limpiar_markdown(response_text)
    try:
        entidades_raw = json.loads(response_text)
    except json.JSONDecodeError as e:
        logger.error(f"[EXTRACTOR CLAUDE] Error parseando JSON: {e}")
        raise

    vehiculos   = entidades_raw.get("vehiculos", [])
    personas    = entidades_raw.get("personas", [])
    ubicaciones = entidades_raw.get("ubicaciones", [])

    resultado = {
        "vehiculos":   añadir_posiciones_vehiculos(texto, vehiculos),
        "personas":    añadir_posiciones_personas(texto, personas),
        "ubicaciones": añadir_posiciones_ubicaciones(texto, ubicaciones),
    }
    return resultado


# ---------------------------------------------------------------------------
# Extractor REGEX (fallback sin Claude)
# ---------------------------------------------------------------------------

def extract_entities_regex(text: str) -> Dict:
    """
    Extractor por regex SIN Claude.
    IMPORTANTE: recibe el texto YA capitalizado por el orquestador.
    """
    text_upper = text.upper()

    # VEHICLES
    vehicles = []
    seen_plates = set()
    for m in PLATE_REGEX.finditer(text_upper):
        plate = re.sub(r'[^A-Z0-9]', '', m.group())
        if plate in seen_plates:
            continue
        seen_plates.add(plate)
        vehicles.append({
            "matricula": plate,
            "marca": "", "modelo": "", "color": "",
            "position": {"start": m.start(), "end": m.end()},
        })

    # PERSONAS — DNIs
    persons = []
    seen_dnis = set()
    for m in DNI_REGEX.finditer(text_upper):
        dni = m.group()
        if dni in seen_dnis:
            continue
        seen_dnis.add(dni)
        persons.append({
            "nombre": "", "apellidos": "", "dni": dni, "rol": "",
            "position": {"start": m.start(), "end": m.end()},
        })

    # PERSONAS — nombres propios por heurística
    # Regex: 2-4 palabras capitalizadas consecutivas
    # FIX: excluir stopwords ampliadas
    NOM_RE = re.compile(
        r'\b([A-ZÁÉÍÓÚÀÈÌÒÙÜÏÑ][a-záéíóúàèìòùüïñ]+(?:\s+[A-ZÁÉÍÓÚÀÈÌÒÙÜÏÑ][a-záéíóúàèìòùüïñ]+){1,3})\b'
    )
    seen_names = set()
    for m in NOM_RE.finditer(text):
        parts = m.group().split()

        # Filtro 1: La primera palabra no puede ser stopword
        if parts[0] in _STOPWORDS:
            continue

        # Filtro 2: Ninguna parte puede ser stopword
        # (elimina "Luz Estrella Gangas Ha" porque "Ha" es stopword)
        if any(p in _STOPWORDS for p in parts):
            continue

        # Filtro 3: mínimo 2 palabras para evitar falsos positivos
        if len(parts) < 2:
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

    # UBICACIONES
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
            "tipo_via":       m.group().split()[0].lower(),
            "nombre_via":     " ".join(m.group().split()[1:]),
            "numero":         "",
            "texto_completo": loc,
            "position":       {"start": m.start(), "end": m.end()},
        })

    return {
        "vehicles":  vehicles,
        "persons":   persons,
        "locations": locations,
    }
