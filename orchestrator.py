"""
SHERLOCK ORCHESTRATOR - API de Orquestación
===========================================

Coordina el flujo completo:
1. REDACTOR (Claude) - texto vago → texto DRAG
2. EXTRACTOR (Claude) - texto DRAG → entidades JSON
3. MATCHER - entidades → coincidencias BD
4. ANNOTATOR - texto + coincidencias → texto enriquecido + explicación

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
# Añadir paths para imports locales
#sys.path.append(str(Path(__file__).parent.parent / "matcher"))
#sys.path.append(str(Path(__file__).parent.parent / "annotator"))

from matcher_service import MatcherService, matches_to_dict
from annotator_service import AnnotatorService

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sherlock_orchestrator.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Paths
BASE_DIR = Path(__file__).parent
PROMPTS_DIR = BASE_DIR
DB_PATH = os.getenv("SHERLOCK_DB_PATH", "/mnt/user-data/uploads/hermano_mayor.db")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Validar configuración
if not ANTHROPIC_API_KEY:
    logger.warning("ANTHROPIC_API_KEY no configurada. El servicio no funcionará correctamente.")

if not Path(DB_PATH).exists():
    logger.error(f"Base de datos no encontrada: {DB_PATH}")

# ============================================================================
# MODELOS PYDANTIC
# ============================================================================

class EnrichRequest(BaseModel):
    """Request para enriquecer informe"""
    texto_vago: str = Field(..., description="Texto dictado por el agente")
    agent_id: Optional[int] = Field(None, description="ID del agente")
    session_id: Optional[str] = Field(None, description="ID de sesión")


class EnrichResponse(BaseModel):
    """Response con informe enriquecido"""
    texto_original: str
    texto_drag: str
    texto_enriquecido: str
    explicacion_db: str
    metadata: Dict
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
    version="1.0.0"
)

# CORS (ajustar según necesidades)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar dominios
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
    
    logger.info("Iniciando SHERLOCK Orchestrator...")
    
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
    
    logger.info("SHERLOCK Orchestrator iniciado correctamente")


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
    """
    Llamar a Claude API de forma robusta.
    
    Args:
        system_prompt: Prompt del sistema
        user_message: Mensaje del usuario
        max_tokens: Máximo de tokens a generar
        
    Returns:
        Respuesta de Claude como string
    """
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


@app.post("/api/v1/enrich", response_model=EnrichResponse)
async def enrich_report(request: EnrichRequest):
    """
    Endpoint principal: enriquecer informe policial.
    
    Flujo:
    1. Validar servicios
    2. REDACTOR: texto vago → texto DRAG
    3. EXTRACTOR: texto DRAG → entidades JSON
    4. MATCHER: entidades → coincidencias BD
    5. ANNOTATOR: texto + coincidencias → resultado enriquecido
    """
    start_time = datetime.utcnow()
    
    # Validaciones
    if not matcher_service:
        raise HTTPException(status_code=503, detail="Servicio MATCHER no disponible")
    if not annotator_service:
        raise HTTPException(status_code=503, detail="Servicio ANNOTATOR no disponible")
    if not anthropic_client:
        raise HTTPException(status_code=503, detail="Servicio Claude no disponible")
    
    texto_vago = request.texto_vago.strip()
    if not texto_vago:
        raise HTTPException(status_code=400, detail="texto_vago no puede estar vacío")
    
    logger.info(f"[ENRICH] Procesando solicitud - Agent ID: {request.agent_id}")
    
    try:
        # ====================================================================
        # PASO 1: REDACTOR (Claude)
        # ====================================================================
        logger.info("[REDACTOR] Iniciando conversión a texto DRAG")
        
        prompt_redactor = cargar_prompt("redactor_system.txt")
        texto_drag = await llamar_claude(prompt_redactor, texto_vago, max_tokens=2000)
        
        logger.info(f"[REDACTOR] Completado - {len(texto_drag)} caracteres generados")
        
        # ====================================================================
        # PASO 2: EXTRACTOR (Claude)
        # ====================================================================
        logger.info("[EXTRACTOR] Extrayendo entidades")
        
        prompt_extractor = cargar_prompt("extractor_system.txt")
        entidades_json_raw = await llamar_claude(prompt_extractor, texto_drag, max_tokens=1500)
        
        # Limpiar posibles marcas de markdown
        entidades_json_raw = entidades_json_raw.strip()
        if entidades_json_raw.startswith("```json"):
            entidades_json_raw = entidades_json_raw[7:]
        if entidades_json_raw.startswith("```"):
            entidades_json_raw = entidades_json_raw[3:]
        if entidades_json_raw.endswith("```"):
            entidades_json_raw = entidades_json_raw[:-3]
        entidades_json_raw = entidades_json_raw.strip()
        
        # Parsear JSON
        try:
            entidades = json.loads(entidades_json_raw)
        except json.JSONDecodeError as e:
            logger.error(f"[EXTRACTOR] Error parseando JSON: {e}")
            logger.error(f"[EXTRACTOR] JSON recibido: {entidades_json_raw[:500]}")
            raise HTTPException(status_code=500, detail="Error parseando entidades extraídas")
        
        logger.info(f"[EXTRACTOR] Completado - {len(entidades.get('vehiculos', []))} vehículos, "
                   f"{len(entidades.get('personas', []))} personas, "
                   f"{len(entidades.get('ubicaciones', []))} ubicaciones")
        
        # ====================================================================
        # PASO 3: MATCHER (local determinista)
        # ====================================================================
        logger.info("[MATCHER] Contrastando con base de datos")
        
        matches_result = matcher_service.contrastar_entidades(entidades)
        matches = matches_result["matches"]
        
        # Convertir a dict para serialización
        matches_dict = matches_to_dict(matches)
        
        logger.info(f"[MATCHER] Completado")
        
        # ====================================================================
        # PASO 4: ANNOTATOR (local determinista)
        # ====================================================================
        logger.info("[ANNOTATOR] Anotando texto")
        
        resultado_anotacion = annotator_service.anotar_texto(texto_drag, matches_dict)
        
        logger.info(f"[ANNOTATOR] Completado - {len(resultado_anotacion.metadata['warnings'])} warnings")
        
        # ====================================================================
        # RESPUESTA
        # ====================================================================
        end_time = datetime.utcnow()
        processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
        
        response = EnrichResponse(
            texto_original=texto_vago,
            texto_drag=texto_drag,
            texto_enriquecido=resultado_anotacion.texto_enriquecido,
            explicacion_db=resultado_anotacion.explicacion_db,
            metadata={
                **resultado_anotacion.metadata,
                "agent_id": request.agent_id,
                "session_id": request.session_id,
                "timestamp": start_time.isoformat()
            },
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
    # TODO: Implementar métricas reales (contador de requests, latencias, etc.)
    return {
        "status": "ok",
        "message": "Estadísticas no implementadas aún"
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Configuración para desarrollo
    uvicorn.run(
        "orchestrator:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
