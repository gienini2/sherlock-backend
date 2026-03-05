# resolver/db_adapter.py

import sqlite3
from typing import Optional, List

class HermanoMayorDB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row

    def get_vehicle_exact(self, plate: str) -> Optional[dict]:
        cur = self.conn.execute(
            "SELECT * FROM vehicles WHERE matricula = ?", (plate,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_vehicle_similar(self, plate: str) -> List[dict]:
        cur = self.conn.execute(
            "SELECT * FROM vehicles WHERE matricula LIKE ?", (f"%{plate[:4]}%",)
        )
        return [dict(r) for r in cur.fetchall()]

    def get_vehicle_history_count(self, plate: str) -> int:
        cur = self.conn.execute(
            "SELECT COUNT(*) as c FROM historial WHERE matricula = ?", (plate,)
        )
        return cur.fetchone()["c"]
    
    def get_vehicle_history(self, plate: str) -> List[dict]:
        """
        Obtiene historial completo de un vehículo.
        
        Returns:
            Lista de registros ordenados por fecha DESC
        """
        cur = self.conn.execute(
            """
            SELECT 
                fecha,
                tipo_actuacion,
                agente_tip,
                ubicacion
            FROM historial
            WHERE matricula = ?
            ORDER BY fecha DESC
            """,
            (plate,)
        )
        return [dict(r) for r in cur.fetchall()]

    def get_person_history(self, dni: str) -> List[dict]:
        """
        Obtiene historial completo de una persona.
        
        Returns:
            Lista de registros ordenados por fecha DESC
        """
        cur = self.conn.execute(
            """
            SELECT 
                fecha,
                tipo_actuacion,
                rol,
                ubicacion
            FROM historial_personas
            WHERE dni = ?
            ORDER BY fecha DESC
            """,
            (dni,)
        )
        return [dict(r) for r in cur.fetchall()]
