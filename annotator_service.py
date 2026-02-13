"""
ANNOTATOR SERVICE - Sistema de Marcado de Texto y Generación de Explicaciones
==============================================================================

Servicio DETERMINISTA que:
- Recibe texto original + matches del MATCHER
- Aplica marcas visuales (@ para exacto, ** para parcial)
- Genera bloque explicativo en catalán policial
- Mantiene trazabilidad completa

NO razona, NO consulta BD, NO inventa explicaciones.
"""

import re
import logging
from typing import Dict, List, Tuple
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class AnnotationResult:
    """Resultado de anotación"""
    texto_enriquecido: str
    explicacion_db: str
    metadata: Dict


class AnnotatorService:
    """
    Servicio de anotación determinista.
    Responsabilidad única: marcar texto y generar explicaciones template-based.
    """
    
    # Umbrales de confidence para marcado
    THRESHOLD_EXACTO = 0.95
    THRESHOLD_PARCIAL = 0.70
    
    def anotar_texto(self, texto_original: str, matches: Dict) -> AnnotationResult:
        """
        Entrada principal del servicio.
        
        Args:
            texto_original: Texto DRAG generado por Claude
            matches: Output del MATCHER
            
        Returns:
            AnnotationResult con texto marcado y explicación
        """
        logger.info("Iniciando anotación de texto")
        
        # Construir lista de marcas a aplicar
        marcas = self._construir_marcas(texto_original, matches)
        
        # Aplicar marcas al texto
        texto_enriquecido = self._aplicar_marcas(texto_original, marcas)
        
        # Generar explicación DB
        explicacion_db = self._generar_explicacion(matches)
        
        # Metadata
        metadata = self._generar_metadata(matches)
        
        logger.info(f"Anotación completada: {len(marcas)} marcas aplicadas")
        
        return AnnotationResult(
            texto_enriquecido=texto_enriquecido,
            explicacion_db=explicacion_db,
            metadata=metadata
        )
    
    # =========================================================================
    # CONSTRUCCIÓN DE MARCAS
    # =========================================================================
    
    def _construir_marcas(self, texto: str, matches: Dict) -> List[Tuple[int, int, str, str]]:
        """
        Construir lista de marcas a aplicar.
        
        Returns:
            Lista de (start, end, tipo_marca, texto_entidad)
            donde tipo_marca es '@' o '**'
        """
        marcas = []
        
        # Procesar vehículos
        for match in matches.get("vehiculos", []):
            if match["match_type"] in ("EXACTO", "PARCIAL") and match["confidence"] >= self.THRESHOLD_PARCIAL:
                tipo_marca = "@" if match["confidence"] >= self.THRESHOLD_EXACTO else "**"
                
                # Buscar matrícula en texto
                matricula = match["entidad_original"].get("matricula", "")
                if matricula:
                    posiciones = self._buscar_texto_en_original(texto, matricula)
                    for start, end in posiciones:
                        marcas.append((start, end, tipo_marca, matricula))
                
                # Buscar marca en texto
                marca = match["entidad_original"].get("marca", "")
                if marca and match["confidence"] >= self.THRESHOLD_EXACTO:
                    posiciones = self._buscar_texto_en_original(texto, marca)
                    for start, end in posiciones:
                        marcas.append((start, end, "@", marca))
                
                # Buscar modelo en texto
                modelo = match["entidad_original"].get("modelo", "")
                if modelo and match["confidence"] >= self.THRESHOLD_EXACTO:
                    posiciones = self._buscar_texto_en_original(texto, modelo)
                    for start, end in posiciones:
                        marcas.append((start, end, "@", modelo))
        
        # Procesar personas
        for match in matches.get("personas", []):
            if match["match_type"] in ("EXACTO", "PARCIAL") and match["confidence"] >= self.THRESHOLD_PARCIAL:
                tipo_marca = "@" if match["confidence"] >= self.THRESHOLD_EXACTO else "**"
                
                # Buscar DNI en texto
                dni = match["entidad_original"].get("dni", "")
                if dni:
                    posiciones = self._buscar_texto_en_original(texto, dni)
                    for start, end in posiciones:
                        marcas.append((start, end, tipo_marca, dni))
                
                # Buscar nombre completo en texto
                nombre = match["entidad_original"].get("nombre", "")
                apellidos = match["entidad_original"].get("apellidos", "")
                if nombre:
                    posiciones = self._buscar_texto_en_original(texto, nombre)
                    for start, end in posiciones:
                        marcas.append((start, end, tipo_marca, nombre))
                if apellidos:
                    posiciones = self._buscar_texto_en_original(texto, apellidos)
                    for start, end in posiciones:
                        marcas.append((start, end, tipo_marca, apellidos))
        
        # Procesar ubicaciones
        for match in matches.get("ubicaciones", []):
            if match["match_type"] in ("EXACTO", "PARCIAL") and match["confidence"] >= self.THRESHOLD_PARCIAL:
                tipo_marca = "@" if match["confidence"] >= self.THRESHOLD_EXACTO else "**"
                
                # Buscar texto completo de ubicación
                texto_ubicacion = match["entidad_original"].get("texto_completo", "")
                if texto_ubicacion:
                    posiciones = self._buscar_texto_en_original(texto, texto_ubicacion)
                    for start, end in posiciones:
                        marcas.append((start, end, tipo_marca, texto_ubicacion))
                else:
                    # Construir desde componentes
                    nombre_via = match["entidad_original"].get("nombre_via", "")
                    if nombre_via:
                        posiciones = self._buscar_texto_en_original(texto, nombre_via)
                        for start, end in posiciones:
                            marcas.append((start, end, tipo_marca, nombre_via))
        
        # Eliminar duplicados y resolver solapamientos
        marcas = self._resolver_solapamientos(marcas)
        
        # Ordenar por posición
        marcas.sort(key=lambda x: x[0])
        
        return marcas
    
    def _buscar_texto_en_original(self, texto: str, buscar: str) -> List[Tuple[int, int]]:
        """Buscar todas las ocurrencias de un texto (case-insensitive)"""
        posiciones = []
        texto_lower = texto.lower()
        buscar_lower = buscar.lower()
        
        start = 0
        while True:
            pos = texto_lower.find(buscar_lower, start)
            if pos == -1:
                break
            posiciones.append((pos, pos + len(buscar)))
            start = pos + 1
        
        return posiciones
    
    def _resolver_solapamientos(self, marcas: List[Tuple[int, int, str, str]]) -> List[Tuple[int, int, str, str]]:
        """
        Resolver solapamientos de marcas.
        Prioridad: @ > **
        Si hay solapamiento, quedarse con el más específico (más corto)
        """
        if not marcas:
            return []
        
        # Ordenar por inicio, luego por longitud (más corto primero)
        marcas_ordenadas = sorted(marcas, key=lambda x: (x[0], x[1] - x[0]))
        
        resultado = []
        for marca in marcas_ordenadas:
            start, end, tipo, texto = marca
            
            # Verificar si solapa con alguna marca ya añadida
            solapa = False
            for r_start, r_end, r_tipo, r_texto in resultado:
                if not (end <= r_start or start >= r_end):  # Hay solapamiento
                    # Prioridad: @ sobre **, o el más corto
                    if r_tipo == "@" and tipo == "**":
                        solapa = True
                        break
                    elif tipo == "@" and r_tipo == "**":
                        # Reemplazar la marca existente
                        resultado.remove((r_start, r_end, r_tipo, r_texto))
                        break
                    elif (end - start) > (r_end - r_start):
                        # La marca existente es más corta, no añadir esta
                        solapa = True
                        break
            
            if not solapa:
                resultado.append(marca)
        
        return resultado
    
    # =========================================================================
    # APLICACIÓN DE MARCAS
    # =========================================================================
    
    def _aplicar_marcas(self, texto: str, marcas: List[Tuple[int, int, str, str]]) -> str:
        """
        Aplicar marcas al texto.
        
        Args:
            texto: Texto original
            marcas: Lista de (start, end, tipo_marca, texto_entidad)
        
        Returns:
            Texto con marcas aplicadas
        """
        if not marcas:
            return texto
        
        # Construir texto marcado de derecha a izquierda para no invalidar posiciones
        resultado = texto
        for start, end, tipo_marca, _ in reversed(marcas):
            texto_marcado = texto[start:end]
            
            if tipo_marca == "@":
                nuevo_texto = f"@{texto_marcado}"
            else:  # tipo_marca == "**"
                nuevo_texto = f"**{texto_marcado}**"
            
            resultado = resultado[:start] + nuevo_texto + resultado[end:]
        
        return resultado
    
    # =========================================================================
    # GENERACIÓN DE EXPLICACIÓN
    # =========================================================================
    
    def _generar_explicacion(self, matches: Dict) -> str:
        """Generar bloque explicativo en catalán"""
        secciones = []
        
        secciones.append("=== INFORMACIÓ DE BASE DE DADES ===\n")
        
        # Explicaciones de vehículos
        for match in matches.get("vehiculos", []):
            if match["match_type"] != "SIN_COINCIDENCIA":
                seccion = self._explicar_vehiculo(match)
                if seccion:
                    secciones.append(seccion)
        
        # Explicaciones de personas
        for match in matches.get("personas", []):
            if match["match_type"] != "SIN_COINCIDENCIA":
                seccion = self._explicar_persona(match)
                if seccion:
                    secciones.append(seccion)
        
        # Explicaciones de ubicaciones
        for match in matches.get("ubicaciones", []):
            if match["match_type"] != "SIN_COINCIDENCIA":
                seccion = self._explicar_ubicacion(match)
                if seccion:
                    secciones.append(seccion)
        
        # Si no hay coincidencias
        if len(secciones) == 1:  # Solo el header
            secciones.append("No s'han trobat coincidències amb la base de dades.\n")
        
        return "\n".join(secciones)
    
    def _explicar_vehiculo(self, match: Dict) -> str:
        """Generar explicación para un vehículo"""
        lineas = []
        
        matricula = match["entidad_original"].get("matricula", "DESCONEGUDA")
        db_record = match.get("db_record", {})
        enrichment = match.get("enrichment", {})
        confidence = match.get("confidence", 0.0)
        match_type = match.get("match_type", "")
        
        # Header
        lineas.append(f"VEHICLE MATRÍCULA {matricula}:")
        
        # Tipo de coincidencia
        if match_type == "EXACTO":
            lineas.append("• Coincidència EXACTA amb registre de base de dades")
        elif match_type == "PARCIAL":
            lineas.append(f"• Coincidència PARCIAL (similitud {confidence*100:.0f}%)")
        
        # Información del vehículo
        if db_record:
            marca = db_record.get("brand", "")
            modelo = db_record.get("model", "")
            if marca or modelo:
                lineas.append(f"• Turisme {marca} {modelo}".strip())
        
        # Titular
        if "titular" in enrichment and enrichment["titular"]:
            titular = enrichment["titular"]
            nombre_completo = f"{titular.get('nombre', '')} {titular.get('apellidos', '')}".strip()
            dni = titular.get('dni', '')
            lineas.append(f"• Titular: {nombre_completo} (DNI {dni})")
            
            # Verificar si conductor coincide con titular
            # (esto requeriría comparar con datos del informe, por ahora lo dejamos genérico)
        
        # Conductores habituales
        if "conductores_habituales" in enrichment and enrichment["conductores_habituales"]:
            conductores = enrichment["conductores_habituales"]
            if conductores:
                lineas.append("• Conductors habituals coneguts:")
                for conductor in conductores[:3]:
                    nombre = f"{conductor.get('nombre', '')} {conductor.get('apellidos', '')}".strip()
                    dni = conductor.get('dni', '')
                    conf = conductor.get('confidence', 0) * 100
                    lineas.append(f"  - {nombre} (DNI {dni}, confiança {conf:.0f}%)")
        
        # Apariciones previas
        apariciones = enrichment.get("apariciones_previas", 0)
        if apariciones > 0:
            lineas.append(f"• Aquest vehicle ha estat identificat en {apariciones} actuació(ns) prèvia(es)")
            
            # Eventos previos
            eventos = enrichment.get("eventos_previos", [])
            if eventos:
                lineas.append(f"• Esdeveniments relacionats: {', '.join(eventos[:5])}")
        
        lineas.append("")  # Línea en blanco
        return "\n".join(lineas)
    
    def _explicar_persona(self, match: Dict) -> str:
        """Generar explicación para una persona"""
        lineas = []
        
        dni = match["entidad_original"].get("dni", "")
        nombre = match["entidad_original"].get("nombre", "")
        apellidos = match["entidad_original"].get("apellidos", "")
        nombre_completo = f"{nombre} {apellidos}".strip()
        
        db_record = match.get("db_record", {})
        enrichment = match.get("enrichment", {})
        confidence = match.get("confidence", 0.0)
        match_type = match.get("match_type", "")
        
        # Header
        if dni:
            lineas.append(f"PERSONA {nombre_completo} ({dni}):")
        else:
            lineas.append(f"PERSONA {nombre_completo}:")
        
        # Tipo de coincidencia
        if match_type == "EXACTO":
            lineas.append("• Coincidència EXACTA amb registre de base de dades")
        elif match_type == "PARCIAL":
            lineas.append(f"• Coincidència PARCIAL (similitud {confidence*100:.0f}%)")
            
            # Si hay diferencias, señalarlas
            if db_record:
                db_nombre = f"{db_record.get('nombre', '')} {db_record.get('apellidos', '')}".strip()
                if db_nombre.lower() != nombre_completo.lower():
                    lineas.append(f"• Nom a la base de dades: {db_nombre}")
        
        # Apariciones previas
        apariciones = enrichment.get("apariciones_previas", 0)
        if apariciones > 0:
            lineas.append(f"• Consta {apariciones} aparició(ns) prèvia(es)")
        else:
            lineas.append("• Primera aparició en el sistema")
        
        # Roles previos
        roles = enrichment.get("roles_previos", [])
        if roles:
            roles_str = ", ".join(roles[:5])
            lineas.append(f"• Rols previs: {roles_str}")
        
        # Domicilio
        if db_record and db_record.get("direccion"):
            lineas.append(f"• Domicili conegut: {db_record['direccion']}")
        
        # Vehículos relacionados
        vehiculos = enrichment.get("vehiculos_relacionados", [])
        if vehiculos:
            lineas.append("• Vehicles relacionats:")
            for vehiculo in vehiculos[:3]:
                plate = vehiculo.get("plate", "")
                marca = vehiculo.get("brand", "")
                modelo = vehiculo.get("model", "")
                relation = vehiculo.get("relation_type", "")
                lineas.append(f"  - {plate} ({marca} {modelo}) - {relation}")
        
        lineas.append("")
        return "\n".join(lineas)
    
    def _explicar_ubicacion(self, match: Dict) -> str:
        """Generar explicación para una ubicación"""
        lineas = []
        
        nombre_via = match["entidad_original"].get("nombre_via", "")
        numero = match["entidad_original"].get("numero", "")
        
        db_record = match.get("db_record")  # Puede ser None
        enrichment = match.get("enrichment", {})
        confidence = match.get("confidence", 0.0)
        match_type = match.get("match_type", "")
        
        # Header
        if db_record and db_record.get("canonical_name"):
            canonical = db_record.get("canonical_name", "").strip()
        else:
            canonical = f"{nombre_via} {numero}".strip()
        lineas.append(f"UBICACIÓ {canonical}:")
        
        # Tipo de coincidencia
        if match_type == "EXACTO":
            lineas.append("• Coincidència EXACTA")
        elif match_type == "PARCIAL":
            lineas.append(f"• Coincidència PARCIAL (similitud {confidence*100:.0f}%)")
        
        # Coordenadas
        if db_record:
            lat = db_record.get("latitude")
            lon = db_record.get("longitude")
            if lat and lon:
                lineas.append(f"• Coordenades: {lat:.4f}, {lon:.4f}")
        
        # Apariciones previas
        apariciones = enrichment.get("apariciones_previas", 0)
        if apariciones > 0:
            lineas.append(f"• Ubicació recurrent: {apariciones} aparició(ns) prèvia(es)")
            
            # Tipos de eventos
            eventos = enrichment.get("eventos_recurrentes", [])
            if eventos:
                eventos_str = ", ".join(eventos[:5])
                lineas.append(f"• Tipologia habitual: {eventos_str}")
            
            lineas.append("• CONSIDERACIÓ: Zona d'actuacions freqüents")
        
        # Alias
        alias = enrichment.get("alias", [])
        if alias:
            lineas.append(f"• Noms alternatius: {', '.join(alias[:3])}")
        
        lineas.append("")
        return "\n".join(lineas)
    
    # =========================================================================
    # GENERACIÓN DE METADATA
    # =========================================================================
    
    def _generar_metadata(self, matches: Dict) -> Dict:
        """Generar metadata del proceso de anotación"""
        metadata = {
            "entidades_exactas": 0,
            "entidades_parciales": 0,
            "entidades_sin_match": 0,
            "warnings": []
        }
        
        # Contar tipos de match
        for tipo_entidad in ["vehiculos", "personas", "ubicaciones"]:
            for match in matches.get(tipo_entidad, []):
                match_type = match.get("match_type", "")
                confidence = match.get("confidence", 0.0)
                
                if match_type == "EXACTO" or (match_type == "PARCIAL" and confidence >= self.THRESHOLD_EXACTO):
                    metadata["entidades_exactas"] += 1
                elif match_type == "PARCIAL" and confidence >= self.THRESHOLD_PARCIAL:
                    metadata["entidades_parciales"] += 1
                else:
                    metadata["entidades_sin_match"] += 1
        
        # Generar warnings
        for match in matches.get("vehiculos", []):
            if match.get("match_type") != "SIN_COINCIDENCIA":
                enrichment = match.get("enrichment", {})
                
                # Warning: titular no coincide con conductor (requeriría info adicional)
                # Por ahora solo warning genérico si hay conductores habituales
                if enrichment.get("conductores_habituales"):
                    metadata["warnings"].append("Vehicle amb conductors habituals coneguts")
        
        for match in matches.get("ubicaciones", []):
            if match.get("match_type") != "SIN_COINCIDENCIA":
                enrichment = match.get("enrichment", {})
                apariciones = enrichment.get("apariciones_previas", 0)
                if apariciones >= 5:
                    metadata["warnings"].append("Ubicació amb alta recurrència d'incidents")
        
        return metadata


# =============================================================================
# EJEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    import json
    
    # Ejemplo de matches (simulado)
    matches_ejemplo = {
        "vehiculos": [
            {
                "entidad_original": {"matricula": "9915GBN", "marca": "Volkswagen", "modelo": "Golf"},
                "match_type": "EXACTO",
                "confidence": 1.0,
                "db_record": {
                    "vehicle_id": 42,
                    "plate": "9915GBN",
                    "brand": "Volkswagen",
                    "model": "Golf",
                    "dni_titular": "12345678A"
                },
                "enrichment": {
                    "titular": {"dni": "12345678A", "nombre": "Pepito", "apellidos": "de los Palotes"},
                    "apariciones_previas": 3,
                    "eventos_previos": ["DRAG-2024-001234"],
                    "conductores_habituales": [
                        {"dni": "98765432B", "nombre": "Maria", "apellidos": "Antonieta", "confidence": 0.85}
                    ]
                }
            }
        ],
        "personas": [
            {
                "entidad_original": {"dni": "43123456X", "nombre": "Joan", "apellidos": "Martí Garcia"},
                "match_type": "PARCIAL",
                "confidence": 0.88,
                "db_record": {"dni": "43123456X", "nombre": "Joan", "apellidos": "Martí García"},
                "enrichment": {
                    "apariciones_previas": 1,
                    "roles_previos": ["denunciant"],
                    "vehiculos_relacionados": []
                }
            }
        ],
        "ubicaciones": [
            {
                "entidad_original": {"tipo_via": "carretera", "nombre_via": "Ribes", "numero": "88"},
                "match_type": "EXACTO",
                "confidence": 1.0,
                "db_record": {
                    "location_id": 15,
                    "canonical_name": "Carretera de Ribes, 88",
                    "latitude": 41.4371,
                    "longitude": 2.2410
                },
                "enrichment": {
                    "apariciones_previas": 7,
                    "eventos_recurrentes": ["robos", "altercats"],
                    "alias": ["Ctra. Ribes 88"]
                }
            }
        ]
    }
    
    texto_ejemplo = """A les 15:30 hores del dia d'avui, he procedit a donar l'alto al vehicle turisme marca Volkswagen model Golf, de color negre, matrícula 9915GBN, a la carretera de Ribes número 88 d'aquesta població. En el moment de la intervenció, el vehicle era conduït per en Joan Martí Garcia, DNI 43123456X."""
    
    annotator = AnnotatorService()
    resultado = annotator.anotar_texto(texto_ejemplo, matches_ejemplo)
    
    print("=== TEXTO ENRIQUECIDO ===")
    print(resultado.texto_enriquecido)
    print("\n")
    print(resultado.explicacion_db)
    print("\n=== METADATA ===")
    print(json.dumps(resultado.metadata, indent=2, ensure_ascii=False))
