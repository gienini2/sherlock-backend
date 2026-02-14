"""
SHERLOCK ORCHESTRATOR - API de Orquestación
===========================================

Coordina el flujo completo:
1. REDACTOR (Claude) - texto vago → texto DRAG (en bitacola-backend)
2. EXTRACTOR (Claude) - texto DRAG → entidades JSON con posiciones
3. MATCHER - entidades → coincidencias BD
4. ANNOTATOR - texto + coincidencias → anotaciones JSON
5. EXPLAINER - matches → explicaciones estructuradas

FastAPI con async para máxima performance.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import anthropic
from matcher_service import MatcherService, matches_to_dict
from annotator_service import AnnotatorService

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Paths
BASE_DIR = Path(__file__).parent
PROMPTS_DIR = BASE_DIR
DB_PATH = os.getenv("SHERLOCK_DB_PATH", str(BASE_DIR / "hermano_mayor.db"))
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Validar configuración
if not ANTHROPIC_API_KEY:
    logger.warning("ANTHROPIC_API_KEY no configurada. El servicio no funcionará correctamente.")

if not Path(DB_PATH).exists():
    logger.error(f"Base de datos no encontrada: {DB_PATH}")

# ============================================================================
# MODELOS PYDANTIC
# ============================================================================

class ExtractRequest(BaseModel):
    """Request para extracción de entidades"""
    texto: str = Field(..., description="Texto policial a analizar")


class CheckEntitiesRequest(BaseModel):
    """Request para check-entities (regex rápido)"""
    texto: str = Field(..., description="Texto a verificar")


class ExplainRequest(BaseModel):
    """Request para explicaciones de DB"""
    matches: Dict = Field(..., description="Matches del matcher")


class EnrichRequest(BaseModel):
    """Request para enriquecer informe"""
    texto_vago: str = Field(..., description="Texto dictado por el agente")
    agent_id: Optional[int] = Field(None, description="ID del agente")
    session_id: Optional[str] = Field(None, description="ID de sesión")


class EnrichResponse(BaseModel):
    """Response con informe enriquecido"""
    texto_original: str
    texto_drag: str
    anotaciones: list
    matches: Dict
    processing_time_ms: int


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: str
    database_ok: bool
    anthropic_ok: bool


# ============================================================================
# INICIALIZACIÓN DE SERVICIOS
# ============================================================================

app = FastAPI(
    title="SHERLOCK API",
    description="Sistema de enriquecimiento de informes policiales",
    version="2.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Servicios globales
matcher_service: Optional[MatcherService] = None
annotator_service: Optional[AnnotatorService] = None
anthropic_client: Optional[anthropic.Anthropic] = None


@app.on_event("startup")
async def startup_event():
    """Inicializar servicios al arrancar"""
    global matcher_service, annotator_service, anthropic_client
    
    logger.info("Iniciando SHERLOCK Orchestrator v2.0...")
    
    # Inicializar MATCHER
    try:
        matcher_service = MatcherService(DB_PATH)
        logger.info("✓ MATCHER inicializado")
    except Exception as e:
        logger.error(f"✗ Error inicializando MATCHER: {e}")
    
    # Inicializar ANNOTATOR
    try:
        annotator_service = AnnotatorService()
        logger.info("✓ ANNOTATOR inicializado")
    except Exception as e:
        logger.error(f"✗ Error inicializando ANNOTATOR: {e}")
    
    # Inicializar Claude client
    if ANTHROPIC_API_KEY:
        try:
            anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            logger.info("✓ Cliente Anthropic inicializado")
        except Exception as e:
            logger.error(f"✗ Error inicializando Anthropic: {e}")
    else:
        logger.warning("✗ Cliente Anthropic NO inicializado (falta API key)")
    
    logger.info("SHERLOCK Orchestrator v2.0 iniciado correctamente")


# ============================================================================
# UTILIDADES
# ============================================================================

def cargar_prompt(nombre_archivo: str) -> str:
    """Cargar prompt desde archivo"""
    path = PROMPTS_DIR / nombre_archivo
    if not path.exists():
        raise FileNotFoundError(f"Prompt no encontrado: {path}")
    return path.read_text(encoding="utf-8")


async def llamar_claude(system_prompt: str, user_message: str, max_tokens: int = 2000) -> str:
    """Llamar a Claude API de forma robusta"""
    if not anthropic_client:
        raise HTTPException(status_code=500, detail="Cliente Anthropic no inicializado")
    
    try:
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_message}
            ]
        )
        
        # Extraer texto de la respuesta
        response_text = ""
        for block in message.content:
            if block.type == "text":
                response_text += block.text
        
        return response_text.strip()
        
    except anthropic.APIError as e:
        logger.error(f"Error en API de Anthropic: {e}")
        raise HTTPException(status_code=502, detail=f"Error llamando a Claude: {str(e)}")
    except Exception as e:
        logger.error(f"Error inesperado llamando a Claude: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check del servicio"""
    database_ok = matcher_service is not None
    anthropic_ok = anthropic_client is not None
    
    status = "ok" if (database_ok and anthropic_ok) else "degraded"
    
    return HealthResponse(
        status=status,
        timestamp=datetime.utcnow().isoformat(),
        database_ok=database_ok,
        anthropic_ok=anthropic_ok
    )


@app.post("/api/v1/check-entities")
async def check_entities(request: CheckEntitiesRequest):
    """
    ENDPOINT LIGERO: Verificación rápida con regex (sin Claude).
    
    Para análisis completo, usar /api/v1/extract o /api/v1/enrich
    """
    texto = request.texto.strip()
    if not texto:
        return {"vehiculos": [], "personas": [], "ubicaciones": []}

    # Extractor regex (rápido, sin Claude)
    from entity_extractor import extract_entities_regex
    raw_entities = extract_entities_regex(texto)

    # Adaptar formato al matcher
    entidades = {
        "vehiculos": [
            {"matricula": v, "marca": "", "modelo": ""}
            for v in raw_entities.get("vehicles", [])
        ],
        "personas": [
            {"dni": p["dni"], "nombre": "", "apellidos": ""}
            for p in raw_entities.get("persons", [])
        ],
        "ubicaciones": []
    }

    # Llamar matcher
    result = matcher_service.contrastar_entidades(entidades)
    
    return matches_to_dict(result["matches"])


@app.post("/api/v1/extract")
async def extract_entities(request: ExtractRequest):
    """
    EXTRACCIÓN COMPLETA con Claude + posiciones.
    
    Input:
        {
            "texto": "texto policial en formato DRAG"
        }
    
    Output:
        {
            "vehiculos": [{matricula, marca, modelo, color, position}, ...],
            "personas": [{nombre, apellidos, dni, rol, position}, ...],
            "ubicaciones": [{tipo_via, nombre_via, numero, position}, ...]
        }
    """
    if not anthropic_client:
        raise HTTPException(
            status_code=503,
            detail="Servicio Claude no disponible"
        )
    
    texto = request.texto.strip()
    if not texto:
        raise HTTPException(
            status_code=400,
            detail="Campo 'texto' requerido"
        )
    
    logger.info("[EXTRACT] Procesando solicitud de extracción")
    
    try:
        from entity_extractor import extract_entities_claude
        
        entidades = await extract_entities_claude(texto, anthropic_client)
        
        logger.info("[EXTRACT] Extracción completada")
        
        return entidades
        
    except Exception as e:
        logger.error(f"[EXTRACT] Error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error en extracción: {str(e)}"
        )


@app.post("/api/v1/explain")
async def explain_matches(request: ExplainRequest):
    """
    Genera explicaciones estructuradas de matches.
    
    Input:
        {
            "matches": {...}  # Output del matcher
        }
    
    Output:
        {
            "explicaciones": [
                {
                    "entity": "9915GBN",
                    "tipo": "VEHICULO",
                    "datos_actuales": {...},
                    "historial": [...],
                    "indicadores": {...}
                },
                ...
            ]
        }
    """
    if not matcher_service:
        raise HTTPException(
            status_code=503,
            detail="Servicio MATCHER no disponible"
        )
    
    matches = request.matches
    
    if not matches:
        return {"explicaciones": []}
    
    logger.info("[EXPLAIN] Generando explicaciones")
    
    try:
        from db_explainer import generar_explicaciones
        
        # Obtener DB adapter del matcher
        db_adapter = matcher_service.db_adapter
        
        explicaciones = generar_explicaciones(matches, db_adapter)
        
        logger.info(f"[EXPLAIN] Generadas {len(explicaciones)} explicaciones")
        
        return {"explicaciones": explicaciones}
        
    except Exception as e:
        logger.error(f"[EXPLAIN] Error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generando explicaciones: {str(e)}"
        )


@app.post("/api/v1/enrich", response_model=EnrichResponse)
async def enrich_report(request: EnrichRequest):
    """
    ENDPOINT COMPLETO: Enriquecimiento full de informe policial.
    
    Flujo:
    1. REDACTOR: texto vago → texto DRAG (debe hacerse en bitacola-backend)
    2. EXTRACTOR: texto DRAG → entidades JSON con posiciones
    3. MATCHER: entidades → coincidencias BD
    4. ANNOTATOR: texto + coincidencias → anotaciones JSON
    
    NOTA: Este endpoint espera recibir texto ya en formato DRAG.
    La redacción debe hacerse previamente en bitacola-backend.
    """
    start_time = datetime.utcnow()
    
    # Validaciones
    if not matcher_service:
        raise HTTPException(status_code=503, detail="Servicio MATCHER no disponible")
    if not annotator_service:
        raise HTTPException(status_code=503, detail="Servicio ANNOTATOR no disponible")
    if not anthropic_client:
        raise HTTPException(status_code=503, detail="Servicio Claude no disponible")
    
    texto_drag = request.texto_vago.strip()
    if not texto_drag:
        raise HTTPException(status_code=400, detail="texto_vago no puede estar vacío")
    
    logger.info(f"[ENRICH] Procesando solicitud - Agent ID: {request.agent_id}")
    
    try:
        # ====================================================================
        # PASO 1: EXTRACTOR (Claude con posiciones)
        # ====================================================================
        logger.info("[EXTRACTOR] Extrayendo entidades con Claude")
        
        from entity_extractor import extract_entities_claude
        
        entidades = await extract_entities_claude(texto_drag, anthropic_client)
        
        logger.info(f"[EXTRACTOR] Completado - {len(entidades.get('vehiculos', []))} vehículos, "
                   f"{len(entidades.get('personas', []))} personas, "
                   f"{len(entidades.get('ubicaciones', []))} ubicaciones")
        
        # ====================================================================
        # PASO 2: MATCHER (local determinista)
        # ====================================================================
        logger.info("[MATCHER] Contrastando con base de datos")
        
        matches_result = matcher_service.contrastar_entidades(entidades)
        matches = matches_result["matches"]
        
        # Convertir a dict para serialización
        matches_dict = matches_to_dict(matches)
        
        logger.info(f"[MATCHER] Completado")
        
        # ====================================================================
        # PASO 3: ANNOTATOR (local determinista)
        # ====================================================================
        logger.info("[ANNOTATOR] Generando anotaciones")
        
        resultado_anotacion = annotator_service.anotar_texto(texto_drag, matches_dict)
        
        logger.info(f"[ANNOTATOR] Completado - {len(resultado_anotacion['anotaciones'])} anotaciones")
        
        # ====================================================================
        # RESPUESTA
        # ====================================================================
        end_time = datetime.utcnow()
        processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
        
        response = EnrichResponse(
            texto_original=texto_drag,
            texto_drag=texto_drag,
            anotaciones=resultado_anotacion["anotaciones"],
            matches=matches_dict,
            processing_time_ms=processing_time_ms
        )
        
        logger.info(f"[ENRICH] Completado exitosamente en {processing_time_ms}ms")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ENRICH] Error inesperado: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@app.get("/api/v1/stats")
async def get_stats():
    """Estadísticas básicas del servicio"""
    return {
        "status": "ok",
        "version": "2.0.0",
        "endpoints": [
            "/health",
            "/api/v1/check-entities",
            "/api/v1/extract",
            "/api/v1/explain",
            "/api/v1/enrich"
        ]
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "orchestrator:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
