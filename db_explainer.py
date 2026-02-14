"""
SHERLOCK DB EXPLAINER - Explicaciones Estructuradas
====================================================

Genera explicaciones estructuradas de coincidencias con BD.
Incluye timeline cronológico de actuaciones.

100% determinista, sin IA.
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def calcular_nivel_riesgo(num_actuaciones: int, dias_ultima: int) -> str:
    """
    Calcula nivel de riesgo basado en historial.
    
    Args:
        num_actuaciones: Número total de actuaciones previas
        dias_ultima: Días desde última actuación
        
    Returns:
        "BAJO", "MEDIO", "ALTO"
    """
    if num_actuaciones == 0:
        return "BAJO"
    
    if num_actuaciones >= 5 or dias_ultima <= 30:
        return "ALTO"
    
    if num_actuaciones >= 2 or dias_ultima <= 90:
        return "MEDIO"
    
    return "BAJO"


def explicar_vehiculo(match: Dict, db_adapter) -> Dict:
    """
    Genera explicación estructurada de vehículo.
    
    Args:
        match: Match del matcher
        db_adapter: Adaptador de BD
        
    Returns:
        Dict con estructura completa
    """
    entidad = match.get("entidad_original", {})
    matricula = entidad.get("matricula", "")
    db_data = match.get("db_data", {})
    
    # Datos actuales
    datos_actuales = {
        "marca": db_data.get("marca", "Desconocida"),
        "modelo": db_data.get("modelo", "Desconocido"),
        "titular": db_data.get("titular", "Desconocido"),
        "dni_titular": db_data.get("dni_titular", "")
    }
    
    # Historial (consultar BD)
    try:
        historial_raw = db_adapter.get_vehicle_history(matricula)
        historial = []
        
        for registro in historial_raw:
            historial.append({
                "fecha": registro.get("fecha", ""),
                "actuacion": registro.get("tipo_actuacion", ""),
                "agente": registro.get("agente_tip", ""),
                "ubicacion": registro.get("ubicacion", "")
            })
        
        # Ordenar por fecha DESC
        historial.sort(key=lambda x: x["fecha"], reverse=True)
        
    except Exception as e:
        logger.error(f"Error consultando historial: {e}")
        historial = []
    
    # Indicadores
    total_actuaciones = len(historial)
    
    # Calcular días desde última actuación
    dias_ultima = 999
    if historial:
        try:
            fecha_ultima = datetime.fromisoformat(historial[0]["fecha"])
            dias_ultima = (datetime.now() - fecha_ultima).days
        except:
            pass
    
    riesgo = calcular_nivel_riesgo(total_actuaciones, dias_ultima)
    
    return {
        "entity": matricula,
        "tipo": "VEHICULO",
        "confianza": match.get("confidence", 0),
        "datos_actuales": datos_actuales,
        "historial": historial,
        "indicadores": {
            "total_actuaciones": total_actuaciones,
            "ultima_actuacion_dias": dias_ultima if dias_ultima < 999 else None,
            "riesgo": riesgo
        }
    }


def explicar_persona(match: Dict, db_adapter) -> Dict:
    """Genera explicación estructurada de persona"""
    entidad = match.get("entidad_original", {})
    dni = entidad.get("dni", "")
    db_data = match.get("db_data", {})
    
    # Datos actuales
    datos_actuales = {
        "nombre": db_data.get("nombre", ""),
        "apellidos": db_data.get("apellidos", ""),
        "fecha_nacimiento": db_data.get("fecha_nacimiento", ""),
        "direccion": db_data.get("direccion", "")
    }
    
    # Historial
    try:
        historial_raw = db_adapter.get_person_history(dni)
        historial = []
        
        for registro in historial_raw:
            historial.append({
                "fecha": registro.get("fecha", ""),
                "actuacion": registro.get("tipo_actuacion", ""),
                "rol": registro.get("rol", ""),  # denunciante, denunciado, testigo
                "ubicacion": registro.get("ubicacion", "")
            })
        
        historial.sort(key=lambda x: x["fecha"], reverse=True)
        
    except Exception as e:
        logger.error(f"Error consultando historial persona: {e}")
        historial = []
    
    # Indicadores
    total_actuaciones = len(historial)
    
    dias_ultima = 999
    if historial:
        try:
            fecha_ultima = datetime.fromisoformat(historial[0]["fecha"])
            dias_ultima = (datetime.now() - fecha_ultima).days
        except:
            pass
    
    riesgo = calcular_nivel_riesgo(total_actuaciones, dias_ultima)
    
    return {
        "entity": dni,
        "tipo": "PERSONA",
        "confianza": match.get("confidence", 0),
        "datos_actuales": datos_actuales,
        "historial": historial,
        "indicadores": {
            "total_actuaciones": total_actuaciones,
            "ultima_actuacion_dias": dias_ultima if dias_ultima < 999 else None,
            "riesgo": riesgo
        }
    }


def generar_explicaciones(matches: Dict, db_adapter) -> List[Dict]:
    """
    Genera explicaciones estructuradas para todas las coincidencias.
    
    Args:
        matches: Resultado del matcher
        db_adapter: Adaptador de BD
        
    Returns:
        Lista de explicaciones estructuradas
    """
    explicaciones = []
    
    # Vehículos
    for vehiculo_match in matches.get("vehiculos", []):
        if vehiculo_match.get("match_type") in ["exact", "partial"]:
            try:
                explicacion = explicar_vehiculo(vehiculo_match, db_adapter)
                explicaciones.append(explicacion)
            except Exception as e:
                logger.error(f"Error explicando vehículo: {e}")
    
    # Personas
    for persona_match in matches.get("personas", []):
        if persona_match.get("match_type") in ["exact", "partial"]:
            try:
                explicacion = explicar_persona(persona_match, db_adapter)
                explicaciones.append(explicacion)
            except Exception as e:
                logger.error(f"Error explicando persona: {e}")
    
    logger.info(f"[EXPLAINER] Generadas {len(explicaciones)} explicaciones")
    
    return explicaciones
