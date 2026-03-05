"""
TOKEN MATCHER - Búsqueda inteligente de personas por tokens
============================================================

Mejora el matching de personas dividiendo nombres/apellidos en tokens
y calculando similitud de forma más flexible.
"""

import re
from typing import List, Tuple, Dict, Optional
from rapidfuzz import fuzz
from unidecode import unidecode


class TokenMatcher:
    """
    Matcher avanzado que divide nombres en tokens y calcula
    similitud de forma más flexible que string completo.
    """
    
    def __init__(self):
        # Stopwords comunes en nombres catalanes/españoles
        self.stopwords = {
            'de', 'del', 'dels', 'la', 'el', 'els', 'les',
            'i', 'y', 'da', 'das', 'dos', 'mc', 'mac'
        }
    
    def _normalizar(self, texto: str) -> str:
        """Normalizar texto: sin acentos, minúsculas"""
        if not texto:
            return ""
        texto = unidecode(texto)
        return texto.upper().strip()
    
    def _tokenizar(self, texto: str) -> List[str]:
        """
        Dividir texto en tokens significativos.
        
        Ejemplo:
            "Maria Del Carmen" → ["MARIA", "CARMEN"]  (elimina "DEL")
            "Gangas Alvear" → ["GANGAS", "ALVEAR"]
        """
        if not texto:
            return []
        
        texto_norm = self._normalizar(texto)
        
        # Separar por espacios, guiones, comas
        tokens = re.split(r'[\s\-,]+', texto_norm)
        
        # Filtrar tokens vacíos y stopwords
        tokens = [
            t for t in tokens
            if t and t.lower() not in self.stopwords and len(t) > 1
        ]
        
        return tokens
    
    def _similitud_tokens(
        self,
        tokens_busqueda: List[str],
        tokens_bd: List[str]
    ) -> float:
        """
        Calcular similitud entre dos listas de tokens.
        
        Estrategia:
        1. Coincidencias exactas = máxima puntuación
        2. Coincidencias parciales = puntuación proporcional
        3. Penalizar tokens no encontrados
        """
        if not tokens_busqueda or not tokens_bd:
            return 0.0
        
        total_score = 0.0
        tokens_bd_usados = set()
        
        for token_busq in tokens_busqueda:
            mejor_match = 0.0
            mejor_idx = -1
            
            # Buscar el mejor match para este token
            for idx, token_bd in enumerate(tokens_bd):
                if idx in tokens_bd_usados:
                    continue
                
                # Similitud entre este par de tokens
                sim = fuzz.ratio(token_busq, token_bd) / 100.0
                
                if sim > mejor_match:
                    mejor_match = sim
                    mejor_idx = idx
            
            # Marcar token de BD como usado
            if mejor_idx >= 0:
                tokens_bd_usados.add(mejor_idx)
            
            total_score += mejor_match
        
        # Normalizar por número de tokens buscados
        score_medio = total_score / len(tokens_busqueda)
        
        # Penalizar si hay muchos tokens en BD que no se usan
        tokens_no_usados = len(tokens_bd) - len(tokens_bd_usados)
        penalizacion = tokens_no_usados * 0.1
        
        return max(score_medio - penalizacion, 0.0)
    
    def match_persona(
        self,
        nombre_busqueda: str,
        apellidos_busqueda: str,
        nombre_bd: str,
        apellidos_bd: str
    ) -> Tuple[float, Dict]:
        """
        Calcular similitud entre persona buscada y persona en BD.
        
        Returns:
            (confidence, detalles)
        """
        # Tokenizar
        tokens_nombre_busq = self._tokenizar(nombre_busqueda)
        tokens_apellidos_busq = self._tokenizar(apellidos_busqueda)
        
        tokens_nombre_bd = self._tokenizar(nombre_bd)
        tokens_apellidos_bd = self._tokenizar(apellidos_bd)
        
        # Similitud de nombres
        if tokens_nombre_busq and tokens_nombre_bd:
            sim_nombre = self._similitud_tokens(tokens_nombre_busq, tokens_nombre_bd)
        else:
            sim_nombre = 0.0
        
        # Similitud de apellidos
        if tokens_apellidos_busq and tokens_apellidos_bd:
            sim_apellidos = self._similitud_tokens(tokens_apellidos_busq, tokens_apellidos_bd)
        else:
            sim_apellidos = 0.0
        
        # Confidence ponderado: nombre 40%, apellidos 60%
        confidence = (sim_nombre * 0.4) + (sim_apellidos * 0.6)
        
        detalles = {
            "similitud_nombre": round(sim_nombre, 3),
            "similitud_apellidos": round(sim_apellidos, 3),
            "tokens_nombre_busqueda": tokens_nombre_busq,
            "tokens_apellidos_busqueda": tokens_apellidos_busq,
            "tokens_nombre_bd": tokens_nombre_bd,
            "tokens_apellidos_bd": tokens_apellidos_bd
        }
        
        return confidence, detalles
    
    def buscar_persona_fuzzy_tokens(
        self,
        nombre: str,
        apellidos: str,
        candidatos: List[Dict],
        umbral: float = 0.70
    ) -> List[Tuple[Dict, float, Dict]]:
        """
        Buscar persona usando matching por tokens.
        
        Args:
            nombre: Nombre a buscar
            apellidos: Apellidos a buscar
            candidatos: Lista de personas de BD con formato:
                [{"dni": ..., "nombre": ..., "apellidos": ...}, ...]
            umbral: Confidence mínimo (default 70%)
            
        Returns:
            Lista de (persona, confidence, detalles) ordenada por confidence
        """
        resultados = []
        
        for candidato in candidatos:
            confidence, detalles = self.match_persona(
                nombre,
                apellidos,
                candidato.get("nombre", ""),
                candidato.get("apellidos", "")
            )
            
            if confidence >= umbral:
                resultados.append((candidato, confidence, detalles))
        
        # Ordenar por confidence descendente
        resultados.sort(key=lambda x: x[1], reverse=True)
        
        return resultados


# ============================================================================
# TESTING Y EJEMPLOS
# ============================================================================

def test_token_matcher():
    """Pruebas del matcher por tokens"""
    
    matcher = TokenMatcher()
    
    print("=" * 70)
    print("TEST TOKEN MATCHER")
    print("=" * 70)
    
    # Caso 1: Luz Estrella Gangas
    print("\n1. Caso: Luz Estrella Gangas")
    print("-" * 70)
    
    confidence, detalles = matcher.match_persona(
        "Luz Estrella",
        "Gangas",
        "Luz Estrella",
        "Gangas Alvear"
    )
    
    print(f"Búsqueda: 'Luz Estrella' + 'Gangas'")
    print(f"BD:       'Luz Estrella' + 'Gangas Alvear'")
    print(f"\nTokens búsqueda nombre: {detalles['tokens_nombre_busqueda']}")
    print(f"Tokens BD nombre:       {detalles['tokens_nombre_bd']}")
    print(f"Similitud nombre:       {detalles['similitud_nombre']*100:.1f}%")
    print()
    print(f"Tokens búsqueda apellidos: {detalles['tokens_apellidos_busqueda']}")
    print(f"Tokens BD apellidos:       {detalles['tokens_apellidos_bd']}")
    print(f"Similitud apellidos:       {detalles['similitud_apellidos']*100:.1f}%")
    print()
    print(f"CONFIDENCE FINAL: {confidence:.3f} ({confidence*100:.1f}%)")
    
    if confidence >= 0.85:
        print("✅ MATCH PARCIAL (≥85%)")
    elif confidence >= 0.70:
        print("⚠️  CANDIDATO (70-85%)")
    else:
        print("❌ NO MATCH (<70%)")
    
    # Caso 2: Maria Del Carmen Rodriguez
    print("\n\n2. Caso: Maria Del Carmen Rodriguez")
    print("-" * 70)
    
    confidence, detalles = matcher.match_persona(
        "Maria Carmen",
        "Rodriguez",
        "Maria Del Carmen",
        "Rodriguez Lopez"
    )
    
    print(f"Búsqueda: 'Maria Carmen' + 'Rodriguez'")
    print(f"BD:       'Maria Del Carmen' + 'Rodriguez Lopez'")
    print(f"\nTokens búsqueda: {detalles['tokens_nombre_busqueda']} + {detalles['tokens_apellidos_busqueda']}")
    print(f"Tokens BD:       {detalles['tokens_nombre_bd']} + {detalles['tokens_apellidos_bd']}")
    print(f"\nCONFIDENCE FINAL: {confidence:.3f} ({confidence*100:.1f}%)")
    
    # Caso 3: Joan Marti vs Joan Marti Garcia
    print("\n\n3. Caso: Joan Marti vs Joan Marti Garcia")
    print("-" * 70)
    
    confidence, detalles = matcher.match_persona(
        "Joan",
        "Marti",
        "Joan",
        "Marti Garcia"
    )
    
    print(f"Búsqueda: 'Joan' + 'Marti'")
    print(f"BD:       'Joan' + 'Marti Garcia'")
    print(f"\nCONFIDENCE FINAL: {confidence:.3f} ({confidence*100:.1f}%)")
    
    # Caso 4: Buscar en lista de candidatos
    print("\n\n4. Búsqueda en lista de candidatos")
    print("-" * 70)
    
    candidatos = [
        {"dni": "12345678A", "nombre": "Joan", "apellidos": "Marti Garcia"},
        {"dni": "87654321B", "nombre": "Joan", "apellidos": "Martinez Lopez"},
        {"dni": "11111111C", "nombre": "Jose", "apellidos": "Marti Ruiz"},
        {"dni": "22222222D", "nombre": "Juan", "apellidos": "Martin Garcia"}
    ]
    
    resultados = matcher.buscar_persona_fuzzy_tokens(
        "Joan",
        "Marti",
        candidatos,
        umbral=0.70
    )
    
    print(f"Búsqueda: 'Joan Marti'")
    print(f"Candidatos encontrados: {len(resultados)}\n")
    
    for i, (persona, conf, det) in enumerate(resultados, 1):
        print(f"{i}. {persona['nombre']} {persona['apellidos']} ({persona['dni']})")
        print(f"   Confidence: {conf:.3f} ({conf*100:.1f}%)")
        print(f"   Similitud nombre: {det['similitud_nombre']*100:.1f}%, apellidos: {det['similitud_apellidos']*100:.1f}%")
        print()


if __name__ == "__main__":
    test_token_matcher()
