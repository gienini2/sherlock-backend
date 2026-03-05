"""
SHERLOCK TIPIFICADOR PENAL V2
==============================

Motor adaptado al catálogo profesional con:
- Indicadores fuertes/débiles
- Banderas transversales
- Niveles de confianza base
- Sensibilidad por delito
"""

import json
import re
import logging
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ResultadoTipificacion:
    """Resultado de tipificación de un delito"""
    id_conducta: str
    familia: str
    descripcion: str
    articulos: List[str]
    confidence: float
    sensibilidad: str
    indicadores_detectados: Dict[str, List[str]]
    banderas_activadas: List[str]
    nota_operativa: Optional[str]


class TipificadorPenalV2:
    """
    Motor de tipificación basado en catálogo profesional.
    """
    
    def __init__(self, catalog_dir: str = None):
        """
        Args:
            catalog_dir: Directorio con los JSON de catálogo
        """
        if catalog_dir is None:
            catalog_dir = Path(__file__).parent / "delitos"
        
        self.catalog_dir = Path(catalog_dir)
        self.delitos = self._cargar_catalogos()
        self.banderas = self._cargar_banderas()
        
        logger.info(f"[TIPIFICADOR V2] Cargados {self._contar_conductas()} delitos")
    
    def _cargar_catalogos(self) -> List[Dict]:
        """Cargar todos los catálogos JSON"""
        delitos = []
        
        # Lista de archivos a cargar
        archivos = [
            "sherlock_cp_v1.json",           # Patrimonio
            "sherlock_cp_t2v1.json",         # Libertad
            "sherlock_cp_t3v1.json",         # Sexuales
            "sherlock_cp_t4v1.json",         # Vida
            "sherlock_cp_t5v1.json",         # Seguridad colectiva
            "sherlock_cp_t6v1.json",         # Orden público
            "sherlock_cp_t7v1.json",         # Funcionarios
            "sherlock_cp_segvial-v1.json"    # Seguridad vial
        ]
        
        for archivo in archivos:
            path = self.catalog_dir / archivo
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    delitos.append(data)
            else:
                logger.warning(f"Catálogo no encontrado: {archivo}")
        
        return delitos
    
    def _cargar_banderas(self) -> Dict:
        """Cargar banderas transversales"""
        path = self.catalog_dir / "sherlock_cp_capastransversales-v1.json"
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"banderas_penales": []}
    
    def _contar_conductas(self) -> int:
        """Contar total de conductas en el catálogo"""
        total = 0
        for catalogo in self.delitos:
            for familia in catalogo.get("familias", []):
                total += len(familia.get("conductas", []))
        return total
    
    def _normalizar_texto(self, texto: str) -> str:
        """Normalizar texto para matching"""
        # Eliminar acentos y convertir a minúsculas
        import unicodedata
        texto = unicodedata.normalize('NFKD', texto)
        texto = texto.encode('ASCII', 'ignore').decode('ASCII')
        return texto.lower()
    
    def _detectar_patterns(self, texto: str, patterns: List[str]) -> List[str]:
        """Detectar patterns lingüísticos en el texto"""
        texto_norm = self._normalizar_texto(texto)
        detectados = []
        
        for pattern in patterns:
            pattern_norm = self._normalizar_texto(pattern)
            # Buscar como palabra completa o subcadena
            if pattern_norm in texto_norm:
                detectados.append(pattern)
        
        return detectados
    
    def _calcular_confidence(
        self,
        conducta: Dict,
        patterns_detectados: List[str],
        indicadores_fuertes: List[str],
        indicadores_debiles: List[str]
    ) -> float:
        """
        Calcular confidence según estructura del catálogo.
        
        Fórmula:
        confidence = nivel_base + bonus_patterns + bonus_indicadores
        """
        # 1. Nivel base del catálogo
        base = conducta.get("nivel_confianza_base", 0.5)
        
        # 2. Bonus por patterns detectados
        total_patterns = len(conducta.get("patterns_linguistics", []))
        if total_patterns > 0:
            ratio_patterns = len(patterns_detectados) / total_patterns
            bonus_patterns = ratio_patterns * 0.3  # Máximo +0.3
        else:
            bonus_patterns = 0
        
        # 3. Bonus por indicadores
        bonus_indicadores = 0
        if indicadores_fuertes:
            bonus_indicadores += len(indicadores_fuertes) * 0.15  # +0.15 por cada fuerte
        if indicadores_debiles:
            bonus_indicadores -= len(indicadores_debiles) * 0.05  # -0.05 por cada débil
        
        # 4. Confidence final (máximo 1.0)
        confidence = min(base + bonus_patterns + bonus_indicadores, 1.0)
        
        return max(confidence, 0.0)  # Mínimo 0.0
    
    def _verificar_condiciones_minimas(
        self,
        texto: str,
        condiciones: List[str]
    ) -> Tuple[bool, List[str]]:
        """
        Verificar que se cumplan condiciones mínimas.
        
        Returns:
            (cumple, lista_de_condiciones_detectadas)
        """
        texto_norm = self._normalizar_texto(texto)
        detectadas = []
        
        for condicion in condiciones:
            condicion_norm = self._normalizar_texto(condicion)
            if condicion_norm in texto_norm:
                detectadas.append(condicion)
        
        # Debe cumplir TODAS las condiciones mínimas
        cumple = len(detectadas) == len(condiciones)
        
        return cumple, detectadas
    
    def _detectar_banderas(self, texto: str) -> List[str]:
        """Detectar banderas transversales en el texto"""
        banderas_detectadas = []
        
        # Keywords por bandera (simplificado)
        keywords_banderas = {
            "menor_implicado": ["menor", "nen", "nena", "adolescent"],
            "violencia_domestica": ["domestic", "parella", "convivent"],
            "violencia_genero": ["dona", "exparella", "violencia masclista"],
            "funcionario_publico": ["agent", "policia", "funcionari"],
            "uso_arma": ["arma", "pistola", "ganivet", "navalla"],
            "resultado_lesivo": ["lesio", "ferit", "contusio"],
            "resultado_mortal": ["mort", "defuncio", "cadaver"]
        }
        
        texto_norm = self._normalizar_texto(texto)
        
        for bandera, keywords in keywords_banderas.items():
            for keyword in keywords:
                if keyword in texto_norm:
                    banderas_detectadas.append(bandera)
                    break
        
        return banderas_detectadas
    
    def analizar(
        self,
        texto: str,
        umbral_confidence: float = 0.4
    ) -> List[ResultadoTipificacion]:
        """
        Analiza texto y devuelve delitos aplicables.
        
        Args:
            texto: Texto policial en formato DRAG
            umbral_confidence: Mínimo confidence para incluir delito
            
        Returns:
            Lista de ResultadoTipificacion ordenados por confidence
        """
        resultados = []
        
        # Detectar banderas transversales
        banderas_detectadas = self._detectar_banderas(texto)
        
        # Iterar por todos los catálogos
        for catalogo in self.delitos:
            sensibilidad = catalogo.get("sensibilidad", "media")
            
            for familia in catalogo.get("familias", []):
                familia_nombre = familia.get("familia", "")
                
                for conducta in familia.get("conductas", []):
                    # 1. Detectar patterns lingüísticos
                    patterns = conducta.get("patterns_linguistics", [])
                    patterns_detectados = self._detectar_patterns(texto, patterns)
                    
                    if not patterns_detectados:
                        continue  # Sin patterns, skip
                    
                    # 2. Verificar condiciones mínimas
                    condiciones = conducta.get("condiciones_minimas", [])
                    cumple, _ = self._verificar_condiciones_minimas(texto, condiciones)
                    
                    if not cumple:
                        continue  # No cumple condiciones mínimas
                    
                    # 3. Detectar indicadores fuertes/débiles
                    ind_fuertes = conducta.get("indicadores_fuertes", [])
                    ind_debiles = conducta.get("indicadores_debiles", [])
                    
                    fuertes_detectados = self._detectar_patterns(texto, ind_fuertes)
                    debiles_detectados = self._detectar_patterns(texto, ind_debiles)
                    
                    # 4. Calcular confidence
                    confidence = self._calcular_confidence(
                        conducta,
                        patterns_detectados,
                        fuertes_detectados,
                        debiles_detectados
                    )
                    
                    if confidence < umbral_confidence:
                        continue  # Confidence muy bajo
                    
                    # 5. Crear resultado
                    resultado = ResultadoTipificacion(
                        id_conducta=conducta.get("id_conducta", ""),
                        familia=familia_nombre,
                        descripcion=conducta.get("descripcion", ""),
                        articulos=conducta.get("articulos_orientativos", []),
                        confidence=confidence,
                        sensibilidad=sensibilidad,
                        indicadores_detectados={
                            "patterns": patterns_detectados,
                            "fuertes": fuertes_detectados,
                            "debiles": debiles_detectados
                        },
                        banderas_activadas=banderas_detectadas,
                        nota_operativa=conducta.get("nota_operativa")
                    )
                    
                    resultados.append(resultado)
        
        # Ordenar por confidence descendente
        resultados.sort(key=lambda x: x.confidence, reverse=True)
        
        logger.info(f"[TIPIFICADOR V2] Detectados {len(resultados)} delitos potenciales")
        
        return resultados
    
    def formato_json(self, resultados: List[ResultadoTipificacion]) -> Dict:
        """Convertir resultados a JSON para API"""
        return {
            "delitos": [
                {
                    "id_conducta": r.id_conducta,
                    "familia": r.familia,
                    "descripcion": r.descripcion,
                    "articulos": r.articulos,
                    "confidence": round(r.confidence, 3),
                    "sensibilidad": r.sensibilidad,
                    "indicadores": r.indicadores_detectados,
                    "banderas": r.banderas_activadas,
                    "nota_operativa": r.nota_operativa
                }
                for r in resultados
            ],
            "num_delitos": len(resultados),
            "banderas_transversales": list(set(
                bandera
                for r in resultados
                for bandera in r.banderas_activadas
            ))
        }


# ============================================================================
# FUNCIONES DE CONVENIENCIA
# ============================================================================

def tipificar_texto(texto: str) -> Dict:
    """
    Función de conveniencia para tipificar un texto.
    
    Returns:
        {
            "delitos": [...],
            "num_delitos": int,
            "banderas_transversales": [...]
        }
    """
    tipificador = TipificadorPenalV2()
    resultados = tipificador.analizar(texto)
    return tipificador.formato_json(resultados)


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    # Ejemplo: Hurto
    texto_hurto = """
    A les 15:30 hores, he procedit a identificar Joan Martí Garcia,
    qui s'ha apoderat d'un telèfon mòbil de la botiga sense consentiment.
    El valor aproximat és de 600 EUR.
    """
    
    print("EJEMPLO 1: Hurto")
    print("=" * 70)
    resultado = tipificar_texto(texto_hurto)
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
    
    # Ejemplo: Amenazas con menor implicado
    texto_amenazas = """
    El denunciant manifesta que el seu exconvivent l'ha amenaçat
    de matar-lo davant del seu fill menor. Context de violència
    domèstica amb reiteració d'amenaces.
    """
    
    print("\n\nEJEMPLO 2: Amenazas + Banderas")
    print("=" * 70)
    resultado = tipificar_texto(texto_amenazas)
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
