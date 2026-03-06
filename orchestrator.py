"""
SHERLOCK — Orquestrador v4.1
============================

CANVIS v4.1 respecte v4.0:
  - FIX: matches_result["matches"] → matches_result (el MatcherService
    ja retorna el dict directe, sense clau 'matches' intermèdia)
  - FIX: matcher_service.close() → afegit mètode close() al MatcherService
  - FIX: _injectar_marcadors ara gestiona PARCIAL (múltiples candidats)
    i NOU (sense coincidència a la BD)
  - FIX: matches_to_dict ara és un passthrough (sense transformació)
  - FIX: _match_vehiculos ara llegeix 'matricula' (no 'plate')

FLUX ACORDAT:

  text col·loquial (entrada de l'agent)
       │
       ▼
  [PAS 1] extract_entities_regex
       │  Detecta matrícules, DNIs, noms, ubicacions per regex (sense IA)
       │  Cada entitat porta posició {start, end} al text original
       ▼
  [PAS 2] contrastar_entidades (MatcherService)
       │  Compara contra BD
       │  → EXACTO  (vehicles/ubicacions): coincidència certa
       │  → PARCIAL (persones): tots els candidats possibles
       │  → SIN_COINCIDENCIA: no trobat → marcat com NOU al DRAG
       ▼
  [PAS 3] _injectar_marcadors
       │  EXACTO  → [[VEHICLE:9915GBN|VW Golf|EXACTO]]
       │  PARCIAL → [[PERSONA:Juan Garcia (BD: JUAN GARCIA LOPEZ)||PARCIAL]]
       │  NOU     → [[PERSONA:Juan Garcia||NOU]]
       ▼
  text col·loquial amb marcadors
       │
       ▼
  [PAS 4] _redactar_drag (Claude / Bitàcola)
       │  Claude redacta en format DRAG professional
       │  Respecta els marcadors [[...]] exactament
       ▼
  text DRAG amb marcadors intactes
       │
       ▼
  [PAS 5] extract_entities_regex (sobre el text DRAG)
       │  Detecta entitats residuals no presents al text original
       ▼
  [PAS 6] AnnotatorService.anotar_text_amb_marcadors
       │  Llegeix marcadors → text NET + spans per al frontend
       ▼
  AnalyzeResponse → frontend
       ├── texto_drag:   text net per al editor
       ├── anotaciones:  spans per pintar
       ├── entidades:    dades per a pestanya BD
       └── ms:           temps total
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

from matcher_service   import MatcherService, matches_to_dict
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
    r'\[\[(PERSONA|VEHICLE|UBICACIO):([^\]|]+)\|([^\]|]*)\|(EXACTO|PARCIAL|NOU)\]\]'
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
    version="4.1.0"
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

    logger.info("Iniciant Sherlock v4.1...")

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
        matcher_service.close()   # FIX: mètode close() afegit al MatcherService
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

    texto_capitalizado = " ".join(
        w.capitalize() if w.isalpha() else w
        for w in texto_orig.split()
    )
    logger.info(f"[PAS 1] Text capitalitzat per regex: {texto_capitalizado[:80]}...")
    entitats_regex = extract_entities_regex(texto_capitalizado)

    entitats_per_matcher = {
        "vehiculos":   entitats_regex.get("vehicles",  []),
        "personas":    entitats_regex.get("persons",   []),
        "ubicaciones": entitats_regex.get("locations", []),
    }

    n_v = len(entitats_per_matcher["vehiculos"])
    n_p = len(entitats_per_matcher["personas"])
    n_u = len(entitats_per_matcher["ubicaciones"])
    logger.info(f"[PAS 1] Extret: {n_v}v {n_p}p {n_u}u")

    # FIX: contrastar_entidades retorna directament el dict de matches
    # (sense clau 'matches' intermèdia que causava el KeyError)
    matches_dict = matcher_service.contrastar_entidades(entitats_per_matcher)

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
    # PAS 5 — Entitats residuals al text DRAG
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
        matches_res  = matcher_service.contrastar_entidades(entitats_drag_filtrades)
        matches_dict = _fusionar_matches(matches_dict, matches_res)

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
    Verificació ràpida per regex sense crida a Claude ni BD.
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
    return matcher_service.contrastar_entidades(entitats_norm)


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
        "version":   "4.1.0",
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

    Lògica per tipus de match:
      EXACTO  → [[VEHICLE:9915GBN|VW Golf|EXACTO]]
      PARCIAL → [[PERSONA:Juan Garcia|BD:JUAN GARCIA LOPEZ,JUAN GARCIA MARTINEZ|PARCIAL]]
                (tots els candidats separats per coma al camp extra)
      NOU     → [[PERSONA:Juan Garcia||NOU]]
                (l'agent sap que no existeix a la BD)

    Treballa de dreta a esquerra per no invalidar posicions.
    Per a PARCIAL: agrupa tots els candidats de la mateixa entitat en un sol marcador.
    """
    spans = []

    # ---------------------------------------------------------------
    # PERSONES — agrupa tots els PARCIAL per posició
    # ---------------------------------------------------------------
    # Primer construïm un dict {posició → [candidats]}
    from collections import defaultdict
    candidats_per_pos: Dict = defaultdict(list)

    for m in matches.get("personas", []):
        pos = (m.get("entidad_original") or {}).get("position")
        if not pos:
            continue
        clau = (pos["start"], pos["end"])
        candidats_per_pos[clau].append(m)

    for (start, end), candidats in candidats_per_pos.items():
        # Nom original extret del text
        nom_original = candidats[0].get("texto", texto[start:end])

        if all(c.get("match_type") == "SIN_COINCIDENCIA" for c in candidats):
            # Sense cap coincidència → NOU
            marcador = f"[[PERSONA:{nom_original}||NOU]]"
        else:
            # Un o més PARCIAL → llista de candidats BD al segon camp
            candidats_bd = [
                f"{r.get('nombre','')} {r.get('apellidos','')}".strip()
                for c in candidats
                if c.get("match_type") == "PARCIAL"
                for r in [c.get("db_record") or {}]
                if r.get("nombre")
            ]
            extra    = ",".join(candidats_bd) if candidats_bd else ""
            marcador = f"[[PERSONA:{nom_original}|{extra}|PARCIAL]]"

        spans.append({"start": start, "end": end, "marcador": marcador})

    # ---------------------------------------------------------------
    # VEHICLES
    # ---------------------------------------------------------------
    for m in matches.get("vehiculos", []):
        if m.get("match_type") not in ("EXACTO", "PARCIAL"):
            # SIN_COINCIDENCIA — marcar com NOU igualment
            pos = (m.get("entidad_original") or {}).get("position")
            if pos:
                nom = m.get("texto", texto[pos["start"]:pos["end"]])
                spans.append({
                    "start":    pos["start"],
                    "end":      pos["end"],
                    "marcador": f"[[VEHICLE:{nom}||NOU]]"
                })
            continue

        pos = (m.get("entidad_original") or {}).get("position")
        if not pos:
            continue
        db    = m.get("db_record") or {}
        plate = db.get("plate", m.get("texto", ""))
        info  = f"{db.get('brand','')} {db.get('model','')}".strip()
        marcador = f"[[VEHICLE:{plate}|{info}|{m['match_type']}]]"
        spans.append({"start": pos["start"], "end": pos["end"], "marcador": marcador})

    # ---------------------------------------------------------------
    # UBICACIONS
    # ---------------------------------------------------------------
    for m in matches.get("ubicaciones", []):
        pos = (m.get("entidad_original") or {}).get("position")
        if not pos:
            continue

        if m.get("match_type") == "SIN_COINCIDENCIA":
            nom      = m.get("texto", texto[pos["start"]:pos["end"]])
            marcador = f"[[UBICACIO:{nom}||NOU]]"
        else:
            db        = m.get("db_record") or {}
            canonical = db.get("canonical_name", m.get("texto", ""))
            marcador  = f"[[UBICACIO:{canonical}||{m['match_type']}]]"

        spans.append({"start": pos["start"], "end": pos["end"], "marcador": marcador})

    # ---------------------------------------------------------------
    # Substituir de dreta a esquerra (no invalida posicions)
    # ---------------------------------------------------------------
    # Eliminar duplicats de posició (en cas de múltiples PARCIAL per la mateixa posició)
    posicions_vistes = set()
    spans_unics = []
    for s in spans:
        clau = (s["start"], s["end"])
        if clau not in posicions_vistes:
            posicions_vistes.add(clau)
            spans_unics.append(s)

    spans_unics.sort(key=lambda s: s["start"], reverse=True)
    text = texto
    for s in spans_unics:
        text = text[:s["start"]] + s["marcador"] + text[s["end"]:]

    return text, len(spans_unics)


async def _redactar_drag(texto_marcat: str, mode: str) -> str:
    """
    Crida Claude per redactar el text DRAG.
    El prompt indica que respecti els marcadors [[...]].
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
        model="claude-3-haiku-20240307",
        max_tokens=300,
        system=prompt,
        messages=[{"role": "user", "content": texto_marcat}]
    )
    return "".join(b.text for b in msg.content if b.type == "text").strip()


def _filtrar_ja_marcades(entitats: Dict, texto_drag: str) -> Dict:
    """
    Elimina entitats que ja estan marcades al text DRAG.
    Prevé doble anotació.
    """
    marcats = set()
    for _, dades, extra, _ in MARKER_RE.findall(texto_drag):
        marcats.add(dades.upper())
        if extra:
            for e in extra.split(","):
                marcats.add(e.strip().upper())

    def no_marcat(e: Dict) -> bool:
        mat = (e.get("matricula") or e.get("plate") or "").upper()
        dni = (e.get("dni") or "").upper()
        nom = f"{e.get('nombre','')} {e.get('apellidos','')}".strip().upper()
        return mat not in marcats and dni not in marcats and nom not in marcats

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
