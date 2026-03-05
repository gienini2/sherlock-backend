"""
SHERLOCK ANNOTATOR v4
=====================

Responsabilitat:
  Llegir el text DRAG (que conté marcadors [[TIPUS:dades|QUALITAT]])
  i generar la llista d'anotacions (spans) per al frontend.

COLORS AL FRONTEND:
  EXACTO  → blau   (dada verificada 100%)
  PARCIAL → taronja (coincidència parcial, l'agent ha de revisar)
  NOU     → verd   (entitat nova trobada al DRAG, no estava al col·loquial)

100% determinista. Sense IA.
"""

import re
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Regex per detectar marcadors al text
MARKER_RE = re.compile(
    r'\[\[(PERSONA|VEHICLE|UBICACIO):([^\]|]+)\|([^\]|]*)\|(EXACTO|PARCIAL)\]\]'
)


@dataclass
class Anotacion:
    id:                str
    type:              str      # PERSONA | VEHICLE | UBICACIO
    start:             int      # posició al text NET (sense marcadors)
    end:               int
    match:             str      # EXACTO | PARCIAL | NOU
    texto_original:    str      # text que apareix al marcador (nom BD)
    texto_enriquecido: str      # text formatat per mostrar al frontend
    confidence:        float
    historial_count:   int
    db_data:           Optional[Dict] = field(default=None)


class AnnotatorService:
    """
    Servei d'anotació determinista.
    Llegeix marcadors [[...]] del text DRAG i genera spans per al frontend.
    """

    def __init__(self):
        logger.info("[ANNOTATOR] Servei inicialitzat")

    # -------------------------------------------------------------------------
    # ENTRADA PRINCIPAL
    # -------------------------------------------------------------------------

    def anotar_text_amb_marcadors(self, texto_drag: str, matches: Dict) -> Dict:
        """
        Processa el text DRAG amb marcadors i genera:
          - texto_drag:  text NET per mostrar (marcadors substituïts per text visible)
          - anotaciones: llista de spans per pintar

        Args:
            texto_drag: Text DRAG amb marcadors [[TIPUS:dades|qualitat]]
            matches:    Output de matches_to_dict() per enriquir amb historial

        Returns:
            {
              "texto_drag":  str,   ← text net per al frontend
              "anotaciones": [...]  ← spans per pintar
            }
        """
        logger.info("[ANNOTATOR] Processant marcadors")

        # Construir índex de matches per cercar ràpid per nom/dni
        idx_matches = _indexar_matches(matches)

        anotacions: List[Anotacion] = []
        cid = 0

        # Reconstruir text net eliminant sintaxi [[...]] i calculant posicions
        text_net   = ""
        cursor_net = 0   # posició al text net
        cursor_raw = 0   # posició al text amb marcadors

        for m in MARKER_RE.finditer(texto_drag):
            tipus    = m.group(1)   # PERSONA | VEHICLE | UBICACIO
            dades    = m.group(2)   # nom BD o matrícula
            extra    = m.group(3)   # DNI, info vehicle, o buit
            qualitat = m.group(4)   # EXACTO | PARCIAL

            # Afegir text entre el cursor i el marcador
            text_net   += texto_drag[cursor_raw:m.start()]
            cursor_net += m.start() - cursor_raw
            cursor_raw  = m.end()

            # Text visible que substitueix el marcador
            text_visible   = _text_visible(tipus, dades, extra, qualitat)
            text_enriq     = _text_enriquecido(tipus, dades, extra, qualitat)

            start_ann = cursor_net
            end_ann   = cursor_net + len(text_visible)

            # Buscar match al índex per obtenir historial
            match_info     = _buscar_match_index(idx_matches, tipus, dades, extra)
            confidence     = match_info.get("confidence", 1.0 if qualitat == "EXACTO" else 0.75)
            historial_count = match_info.get("historial_count", 0)
            db_data        = match_info.get("db_data")

            anotacions.append(Anotacion(
                id=f"{tipus[0].lower()}{cid}",
                type=tipus,
                start=start_ann,
                end=end_ann,
                match=qualitat,
                texto_original=dades,
                texto_enriquecido=text_enriq,
                confidence=confidence,
                historial_count=historial_count,
                db_data=db_data,
            ))

            text_net   += text_visible
            cursor_net += len(text_visible)
            cid        += 1

        # Afegir la resta del text després de l'últim marcador
        text_net += texto_drag[cursor_raw:]

        # Eliminar solapaments (no haurien de produir-se, però per seguretat)
        anotacions = _eliminar_solapaments(anotacions)

        result = [_serialitzar(a) for a in anotacions]
        logger.info(f"[ANNOTATOR] {len(result)} anotacions generades")

        return {
            "texto_drag":  text_net,
            "anotaciones": result,
        }

    # Manté compatibilitat amb codi antic que pugui cridar anotar_texto
    def anotar_texto(self, texto: str, matches: Dict) -> Dict:
        return self.anotar_text_amb_marcadors(texto, matches)


# ============================================================================
# HELPERS
# ============================================================================

def _text_visible(tipus: str, dades: str, extra: str, qualitat: str) -> str:
    """
    Text que es mostra al editor en lloc del marcador.

    PERSONA EXACTO:  "Luz Estrella Gangas Alvear"
    PERSONA PARCIAL: "Luz Estrella Gangas Alvear"   (l'agent ho pot revisar pel color)
    VEHICLE EXACTO:  "9915GBN"
    UBICACIO EXACTO: "Avinguda Diagonal 437"
    """
    if tipus == "PERSONA":
        return dades                        # nom complet BD
    if tipus == "VEHICLE":
        return dades                        # matrícula BD
    if tipus == "UBICACIO":
        return dades                        # nom canònic BD
    return dades


def _text_enriquecido(tipus: str, dades: str, extra: str, qualitat: str) -> str:
    """
    Text enriquit per al tooltip / panel lateral del frontend.

    PERSONA EXACTO:  "Luz Estrella Gangas Alvear (X65234520A)"
    VEHICLE EXACTO:  "9915GBN · VW Golf"
    UBICACIO EXACTO: "Avinguda Diagonal 437"
    """
    if tipus == "PERSONA" and extra:
        return f"{dades} ({extra})"
    if tipus == "VEHICLE" and extra:
        return f"{dades} · {extra}"
    return dades


def _indexar_matches(matches: Dict) -> Dict:
    """
    Construeix un índex { clau_normalitzada → match_info }
    per buscar ràpidament per nom BD o matrícula.
    """
    idx = {}

    for m in matches.get("personas", []):
        db = m.get("db_record") or {}
        clau = f"{db.get('nombre','')} {db.get('apellidos','')}".strip().upper()
        if clau:
            idx[clau] = {
                "confidence":     m.get("confidence", 0.0),
                "historial_count": (m.get("enrichment") or {}).get("apariciones_previas", 0),
                "db_data":        db,
            }
        dni = db.get("dni", "").upper()
        if dni:
            idx[dni] = idx.get(clau, {
                "confidence":     m.get("confidence", 0.0),
                "historial_count": 0,
                "db_data":        db,
            })

    for m in matches.get("vehiculos", []):
        db  = m.get("db_record") or {}
        clau = db.get("plate", "").upper()
        if clau:
            idx[clau] = {
                "confidence":     m.get("confidence", 0.0),
                "historial_count": (m.get("enrichment") or {}).get("apariciones_previas", 0),
                "db_data":        db,
            }

    for m in matches.get("ubicaciones", []):
        db   = m.get("db_record") or {}
        clau = db.get("canonical_name", "").upper()
        if clau:
            idx[clau] = {
                "confidence":     m.get("confidence", 0.0),
                "historial_count": (m.get("enrichment") or {}).get("apariciones_previas", 0),
                "db_data":        db,
            }

    return idx


def _buscar_match_index(idx: Dict, tipus: str, dades: str, extra: str) -> Dict:
    """Cerca match a l'índex per nom, DNI o matrícula."""
    clau = dades.upper()
    if clau in idx:
        return idx[clau]
    if extra:
        clau2 = extra.upper()
        if clau2 in idx:
            return idx[clau2]
    return {}


def _eliminar_solapaments(anotacions: List[Anotacion]) -> List[Anotacion]:
    result: List[Anotacion] = []
    ultim_end = -1
    for ann in sorted(anotacions, key=lambda a: a.start):
        if ann.start >= ultim_end:
            result.append(ann)
            ultim_end = ann.end
        else:
            logger.debug(f"[ANNOTATOR] Solapament ignorat: {ann.id}")
    return result


def _serialitzar(a: Anotacion) -> Dict:
    return {
        "id":                a.id,
        "type":              a.type,
        "start":             a.start,
        "end":               a.end,
        "match":             a.match,
        "texto_original":    a.texto_original,
        "texto_enriquecido": a.texto_enriquecido,
        "confidence":        round(a.confidence, 3),
        "historial_count":   a.historial_count,
        "db_data":           a.db_data,
    }
