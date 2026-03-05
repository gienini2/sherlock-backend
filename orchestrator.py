"""
SHERLOCK — Orquestador v4.0
============================

FLUX ACORDAT:

  text col·loquial (entrada de l'agent)
       │
       ▼
  [PAS 1] extract_entities_regex
       │  Detecta matrícules, DNIs, noms, ubicacions per regex (sense IA)
       │  Cada entitat porta posició {start, end} al text original
       ▼
  [PAS 2] contrastar_entidades (MatcherService)
       │  Compara contra BD amb similitud
       │  → EXACTO  (≥0.95): cert match
       │  → PARCIAL (≥0.70): match probable
       │  → SIN_COINCIDENCIA: no trobat
       ▼
  [PAS 3] _injectar_marcadors
       │  Substitueix text original per marcadors protegits:
       │    "luz estrella gangas" → [[PERSONA:Luz Estrella Gangas Alvear|X65234520A|EXACTO]]
       │    "comisaria"          → [[UBICACIO:Comissaria Districte 3||EXACTO]]
       │  Si no hi ha match → el text queda sense tocar
       ▼
  text col·loquial amb marcadors
       │
       ▼
  [PAS 4] _redactar_drag (Claude / Bitàcola)
       │  Claude redacta en format DRAG professional
       │  El prompt li indica que respecti els marcadors [[...]] exactament
       │  → no inventa DNIs ni noms que no estiguin en marcadors
       ▼
  text DRAG amb marcadors intactes
       │
       ▼
  [PAS 5] extract_entities_regex (sobre el text DRAG)
       │  Detecta entitats que hagin aparegut durant la redacció
       │  i que no estaven al text original (cas rar però possible)
       │  → filtra les que ja estan marcades
       ▼
  [PAS 6] AnnotatorService.anotar_text_amb_marcadors
       │  Llegeix marcadors [[...]] del text DRAG
       │  → genera text NET (sense sintaxi [[...]])
       │  → genera spans (start/end/color/tooltip) sobre el text net
       │  → construeix payload per a pestanya BD
       ▼
  AnalyzeResponse → frontend
       ├── texto_drag:   text net per al editor (editable per l'agent)
       ├── anotaciones:  spans per pintar (blau=exacto, taronja=parcial)
       ├── entidades:    dades per a pestanya BD (historial, risc)
       └── ms:           temps total

COLORS AL FRONTEND:
  PERSONA  EXACTO  → blau    (#cce5ff)
  PERSONA  PARCIAL → taronja (#fff3cd)
  VEHICLE  EXACTO  → verd    (#d4edda)
  VEHICLE  PARCIAL → taronja (#fff3cd)
  UBICACIO EXACTO  → lila    (#e2d9f3)
  UBICACIO PARCIAL → taronja (#fff3cd)
"""

import os
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import anthropic

from matcher_service  import MatcherService, matches_to_dict
from annotator_service import AnnotatorService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURACIÓ
# ============================================================================

BASE_DIR          = Path(__file__).parent
DB_PATH           = os.getenv("SHERLOCK_DB_PATH", str(BASE_DIR / "hermano_mayor.db"))
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

if not ANTHROPIC_API_KEY:
    logger.warning("ANTHROPIC_API_KEY no configurada")
if not Path(DB_PATH).exists():
    logger.error(f"Base de datos no encontrada: {DB_PATH}")

# Regex per detectar marcadors ja injectats al text
MARKER_RE = re.compile(
    r'\[\[(PERSONA|VEHICLE|UBICACIO):([^\]|]+)\|([^\]|]*)\|(EXACTO|PARCIAL)\]\]'
)

# ============================================================================
# MODELS PYDANTIC
# ============================================================================

class ProcessRequest(BaseModel):
    texto:      str           = Field(..., description="Text col·loquial de l'agent")
    mode:       str           = Field("parte", description="parte | informe")
    agent_id:   Optional[int] = Field(None)
    session_id: Optional[str] = Field(None)


class AnalyzeResponse(BaseModel):
    texto_coloquial: str
    texto_drag:      str
    anotaciones:     List[Dict]
    entidades:       Dict
    ms:              int


class CheckRequest(BaseModel):
    texto: str


class HealthResponse(BaseModel):
    status:       str
    timestamp:    str
    database_ok:  bool
    anthropic_ok: bool


# ============================================================================
# APP + SERVEIS
# ============================================================================

app = FastAPI(
    title="SHERLOCK API",
    description="Pipeline d'enriquiment i redacció d'informes policials",
    version="4.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

matcher_service:   Optional[MatcherService]      = None
annotator_service: Optional[AnnotatorService]    = None
anthropic_client:  Optional[anthropic.Anthropic] = None


@app.on_event("startup")
async def startup():
    global matcher_service, annotator_service, anthropic_client

    logger.info("Iniciant Sherlock v4.0...")

    try:
        matcher_service = MatcherService(DB_PATH)
        logger.info("✓ MATCHER inicialitzat")
    except Exception as e:
        logger.error(f"✗ MATCHER error: {e}")

    try:
        annotator_service = AnnotatorService()
        logger.info("✓ ANNOTATOR inicialitzat")
    except Exception as e:
        logger.error(f"✗ ANNOTATOR error: {e}")

    if ANTHROPIC_API_KEY:
        try:
            anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            logger.info("✓ ANTHROPIC CLIENT inicialitzat")
        except Exception as e:
            logger.error(f"✗ ANTHROPIC CLIENT error: {e}")
    else:
        logger.warning("✗ ANTHROPIC CLIENT no disponible (falta API key)")


@app.on_event("shutdown")
async def shutdown():
    if matcher_service:
        matcher_service.close()
    logger.info("Sherlock aturat.")


# ============================================================================
# ENDPOINT PRINCIPAL
# ============================================================================

@app.post("/api/v1/process", response_model=AnalyzeResponse)
async def process(request: ProcessRequest):
    """
    Pipeline complet: text col·loquial → informe DRAG anotat.
    """
    _check_services()

    texto_orig = request.texto.strip()
    if not texto_orig:
        raise HTTPException(status_code=400, detail="El text no pot estar buit")

    t0 = datetime.utcnow()
    logger.info(f"[PROCESS] mode={request.mode} len={len(texto_orig)}")

    # -------------------------------------------------------------------------
    # PAS 1+2 — Extracció per regex + contrast amb BD
    # -------------------------------------------------------------------------
    from entity_extractor import extract_entities_regex
    entitats_regex = extract_entities_regex(texto_orig)

    entitats_per_matcher = {
        "vehiculos":   entitats_regex.get("vehicles",  []),
        "personas":    entitats_regex.get("persons",   []),
        "ubicaciones": entitats_regex.get("locations", []),
    }

    n_v = len(entitats_per_matcher["vehiculos"])
    n_p = len(entitats_per_matcher["personas"])
    n_u = len(entitats_per_matcher["ubicaciones"])
    logger.info(f"[PAS 1] Extret: {n_v}v {n_p}p {n_u}u")

    matches_result = matcher_service.contrastar_entidades(entitats_per_matcher)
    matches_dict   = matches_to_dict(matches_result["matches"])

    n_match = sum(
        1 for llista in matches_dict.values()
        for m in llista if m.get("match_type") in ("EXACTO", "PARCIAL")
    )
    logger.info(f"[PAS 2] Matches BD: {n_match}")

    # -------------------------------------------------------------------------
    # PAS 3 — Injectar marcadors al text col·loquial
    # -------------------------------------------------------------------------
    texto_marcat, n_marcadors = _injectar_marcadors(texto_orig, matches_dict)
    logger.info(f"[PAS 3] Marcadors injectats: {n_marcadors}")

    # -------------------------------------------------------------------------
    # PAS 4 — Redactar DRAG (Claude)
    # -------------------------------------------------------------------------
    logger.info("[PAS 4] Redactant DRAG...")
    texto_drag = await _redactar_drag(texto_marcat, request.mode)
    logger.info(f"[PAS 4] DRAG generat: {len(texto_drag)} chars")

    # -------------------------------------------------------------------------
    # PAS 5 — Entitats residuals al text DRAG (les que no estaven al col·loquial)
    # -------------------------------------------------------------------------
    from entity_extractor import extract_entities_regex as eer
    entitats_drag = eer(texto_drag)
    entitats_drag_filtrades = _filtrar_ja_marcades({
        "vehiculos":   entitats_drag.get("vehicles",  []),
        "personas":    entitats_drag.get("persons",   []),
        "ubicaciones": entitats_drag.get("locations", []),
    }, texto_drag)

    n_res = sum(len(v) for v in entitats_drag_filtrades.values())
    if n_res > 0:
        logger.info(f"[PAS 5] {n_res} entitats residuals al DRAG — contrastant amb BD")
        matches_res = matcher_service.contrastar_entidades(entitats_drag_filtrades)
        matches_dict = _fusionar_matches(matches_dict, matches_to_dict(matches_res["matches"]))

    # -------------------------------------------------------------------------
    # PAS 6 — Anotar: llegir marcadors → text net + spans per al frontend
    # -------------------------------------------------------------------------
    logger.info("[PAS 6] Generant anotacions...")
    resultat = annotator_service.anotar_text_amb_marcadors(texto_drag, matches_dict)

    ms = int((datetime.utcnow() - t0).total_seconds() * 1000)
    logger.info(f"[PROCESS] Completat en {ms}ms")

    return AnalyzeResponse(
        texto_coloquial=texto_orig,
        texto_drag=resultat["texto_drag"],
        anotaciones=resultat["anotaciones"],
        entidades=matches_dict,
        ms=ms,
    )


# ============================================================================
# ENDPOINT RÀPID — /check (regex, sense Claude)
# ============================================================================

@app.post("/api/v1/check")
async def check(request: CheckRequest):
    """
    Verificació ràpida per regex.
    Per al feedback immediat mentre l'agent escriu (sense crida a Claude ni BD).
    """
    _check_matcher()
    texto = request.texto.strip()
    if not texto:
        return {"vehiculos": [], "personas": [], "ubicaciones": []}

    from entity_extractor import extract_entities_regex
    entitats = extract_entities_regex(texto)
    entitats_norm = {
        "vehiculos":   entitats.get("vehicles",  []),
        "personas":    entitats.get("persons",   []),
        "ubicaciones": entitats.get("locations", []),
    }
    result = matcher_service.contrastar_entidades(entitats_norm)
    return matches_to_dict(result["matches"])


# ============================================================================
# ENDPOINTS AUXILIARS
# ============================================================================

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok" if (matcher_service and anthropic_client) else "degraded",
        timestamp=datetime.utcnow().isoformat(),
        database_ok=matcher_service is not None,
        anthropic_ok=anthropic_client is not None,
    )


@app.get("/api/v1/stats")
async def stats():
    return {
        "version":   "4.0.0",
        "endpoints": ["/health", "/api/v1/process", "/api/v1/check"],
    }


# ============================================================================
# HELPERS PRIVATS
# ============================================================================

def _check_services():
    if not matcher_service:
        raise HTTPException(503, "MATCHER no disponible")
    if not annotator_service:
        raise HTTPException(503, "ANNOTATOR no disponible")
    if not anthropic_client:
        raise HTTPException(503, "ANTHROPIC CLIENT no disponible")


def _check_matcher():
    if not matcher_service:
        raise HTTPException(503, "MATCHER no disponible")


def _injectar_marcadors(texto: str, matches: Dict):
    """
    Substitueix les ocurrències del text original per marcadors protegits.

    Treballa de dreta a esquerra (per índex DESC) per no invalidar posicions.

    Retorna (text_amb_marcadors, num_marcadors_injectats).
    """
    spans = []

    for m in matches.get("personas", []):
        if m.get("match_type") not in ("EXACTO", "PARCIAL"):
            continue
        pos = (m.get("entidad_original") or {}).get("position")
        if not pos:
            continue
        db  = m.get("db_record") or {}
        nom = f"{db.get('nombre','')} {db.get('apellidos','')}".strip()
        dni = db.get("dni", "")
        marcador = f"[[PERSONA:{nom}|{dni}|{m['match_type']}]]"
        spans.append({"start": pos["start"], "end": pos["end"], "marcador": marcador})

    for m in matches.get("vehiculos", []):
        if m.get("match_type") not in ("EXACTO", "PARCIAL"):
            continue
        pos = (m.get("entidad_original") or {}).get("position")
        if not pos:
            continue
        db    = m.get("db_record") or {}
        plate = db.get("plate", (m.get("entidad_original") or {}).get("matricula", ""))
        info  = f"{db.get('brand','')} {db.get('model','')}".strip()
        marcador = f"[[VEHICLE:{plate}|{info}|{m['match_type']}]]"
        spans.append({"start": pos["start"], "end": pos["end"], "marcador": marcador})

    for m in matches.get("ubicaciones", []):
        if m.get("match_type") not in ("EXACTO", "PARCIAL"):
            continue
        pos = (m.get("entidad_original") or {}).get("position")
        if not pos:
            continue
        db        = m.get("db_record") or {}
        canonical = db.get("canonical_name", (m.get("entidad_original") or {}).get("texto_completo", ""))
        marcador  = f"[[UBICACIO:{canonical}||{m['match_type']}]]"
        spans.append({"start": pos["start"], "end": pos["end"], "marcador": marcador})

    # Substituir de dreta a esquerra
    spans.sort(key=lambda s: s["start"], reverse=True)
    text = texto
    for s in spans:
        text = text[:s["start"]] + s["marcador"] + text[s["end"]:]

    return text, len(spans)


async def _redactar_drag(texto_marcat: str, mode: str) -> str:
    """
    Crida Claude (Bitàcola) per redactar el text DRAG.
    El prompt (redactor_system.txt) li indica que respecti els marcadors [[...]].
    """
    prompt_path = BASE_DIR / "redactor_system.txt"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt no trobat: {prompt_path}")
    prompt = prompt_path.read_text(encoding="utf-8")

    if mode == "informe":
        prompt += "\n\nMODE: INFORME POLICIAL EXTENSIU (250-400 paraules)"
    else:
        prompt += "\n\nMODE: ENTRADA DE BITÀCOLA BREU (60-100 paraules)"

    msg = anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1200,
        system=prompt,
        messages=[{"role": "user", "content": texto_marcat}]
    )
    return "".join(b.text for b in msg.content if b.type == "text").strip()


def _filtrar_ja_marcades(entitats: Dict, texto_drag: str) -> Dict:
    """
    Elimina de la llista d'entitats les que ja estan marcades al text DRAG.
    Preveu doble anotació.
    """
    marcats = set()
    for _, dades, extra, _ in MARKER_RE.findall(texto_drag):
        marcats.add(dades.upper())
        if extra:
            marcats.add(extra.upper())

    def no_marcat(e: Dict) -> bool:
        mat = e.get("matricula", "").upper()
        dni = e.get("dni", "").upper()
        return mat not in marcats and dni not in marcats

    return {
        "vehiculos":   [e for e in entitats.get("vehiculos",   []) if no_marcat(e)],
        "personas":    [e for e in entitats.get("personas",    []) if no_marcat(e)],
        "ubicaciones": [e for e in entitats.get("ubicaciones", []) if no_marcat(e)],
    }


def _fusionar_matches(m1: Dict, m2: Dict) -> Dict:
    """Combina dos dicts de matches sense duplicar."""
    return {
        "vehiculos":   m1.get("vehiculos",   []) + m2.get("vehiculos",   []),
        "personas":    m1.get("personas",    []) + m2.get("personas",    []),
        "ubicaciones": m1.get("ubicaciones", []) + m2.get("ubicaciones", []),
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=8000, reload=True)
