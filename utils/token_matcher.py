"""
TOKEN MATCHER - Fuzzy matching de personas basado en tokens
============================================================

Estrategia:
1. Normalizar texto (unidecode + uppercase)
2. Tokenizar por espacios
3. Comparar tokens con rapidfuzz
4. Calcular confianza combinada

Uso exclusivo en matcher_service.py para búsqueda de personas por nombre.
"""

import logging
from typing import List, Dict, Tuple, Optional
from rapidfuzz import fuzz
from unidecode import unidecode

logger = logging.getLogger(__name__)

class TokenMatcher:
    """
    Matcher basado en tokens para nombres de personas.
    """
    
    def __init__(self, umbral_global: float = 0.70):
        self.umbral_global = umbral_global
        
    def normalizar(self, texto: str) -> str:
        """Normaliza texto: unidecode + uppercase + limpia espacios"""
        if not texto:
            return ""
        return unidecode(texto).upper().strip()
    
    def tokenizar(self, texto: str) -> List[str]:
        """Divide texto en tokens (palabras)"""
        return self.normalizar(texto).split()
    
    def buscar_persona_fuzzy_tokens(
        self, 
        nombre: str, 
        apellidos: str, 
        candidatos: List[Dict], 
        umbral: float = 0.70
    ) -> List[Tuple[Dict, float, Dict]]:
        """
        Busca persona por tokens (nombre + apellidos)
        
        Args:
            nombre: Nombre a buscar
            apellidos: Apellidos a buscar
            candidatos: Lista de dicts con 'nombre', 'apellidos', 'dni'
            umbral: Umbral mínimo de confianza
            
        Returns:
            Lista de (candidato, confianza, detalles)
        """
        if not candidatos:
            return []
            
        # Construir texto completo de búsqueda
        texto_busqueda = f"{nombre} {apellidos}".strip()
        if not texto_busqueda:
            return []
            
        tokens_busqueda = self.tokenizar(texto_busqueda)
        if not tokens_busqueda:
            return []
            
        resultados = []
        
        for cand in candidatos:
            try:
                # Construir texto del candidato
                texto_cand = f"{cand.get('nombre','')} {cand.get('apellidos','')}".strip()
                if not texto_cand:
                    continue
                    
                tokens_cand = self.tokenizar(texto_cand)
                
                # Calcular matching de tokens
                match_info = self._calcular_match_tokens(
                    tokens_busqueda, 
                    tokens_cand,
                    cand
                )
                
                if match_info and match_info['confianza'] >= umbral:
                    resultados.append((
                        cand,
                        match_info['confianza'],
                        match_info['detalles']
                    ))
                    
            except Exception as e:
                logger.error(f"Error procesando candidato: {e}")
                continue
        
        # Ordenar por confianza descendente
        resultados.sort(key=lambda x: x[1], reverse=True)
        return resultados[:5]  # Top 5
    
    def _calcular_match_tokens(
        self, 
        tokens_a: List[str], 
        tokens_b: List[str],
        candidato: Dict
    ) -> Optional[Dict]:
        """
        Calcula similitud entre dos listas de tokens.
        
        Returns:
            Dict con confianza y detalles, o None si no hay match
        """
        if not tokens_a or not tokens_b:
            return None
            
        # Matriz de similitud entre tokens
        n_a, n_b = len(tokens_a), len(tokens_b)
        matriz = [[0.0] * n_b for _ in range(n_a)]
        
        for i, ta in enumerate(tokens_a):
            for j, tb in enumerate(tokens_b):
                matriz[i][j] = fuzz.ratio(ta, tb) / 100.0
        
        # Estrategia: asignar cada token de A al mejor token de B
        confianza_total = 0.0
        asignaciones = []
        usados_b = set()
        
        for i in range(n_a):
            mejor_j = -1
            mejor_valor = 0.0
            for j in range(n_b):
                if j not in usados_b and matriz[i][j] > mejor_valor:
                    mejor_valor = matriz[i][j]
                    mejor_j = j
            
            if mejor_j != -1:
                confianza_total += mejor_valor
                asignaciones.append((i, mejor_j, mejor_valor))
                usados_b.add(mejor_j)
        
        # Tokens no asignados penalizan
        tokens_no_asignados = n_a - len(asignaciones)
        penalizacion = tokens_no_asignados * 0.2
        confianza = (confianza_total / max(n_a, 1)) - penalizacion
        confianza = max(0.0, min(1.0, confianza))
        
        return {
            "confianza": confianza,
            "detalles": {
                "tokens_a": tokens_a,
                "tokens_b": tokens_b,
                "asignaciones": asignaciones,
                "penalizacion": penalizacion
            }
        }
    
    def buscar_por_dni_exacto(self, dni: str, candidatos: List[Dict]) -> Optional[Dict]:
        """Busca coincidencia exacta por DNI"""
        dni_norm = self.normalizar(dni)
        for cand in candidatos:
            if self.normalizar(cand.get('dni', '')) == dni_norm:
                return cand
        return None
