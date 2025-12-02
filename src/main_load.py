# src/main_load.py
"""
Punto de entrada para la fase de Load del ETL.

Uso:
    Desde la raíz del proyecto:

        python -m src.main_load

Responsabilidad:
- Conectarse a la base de datos PostgreSQL usando DB_URI.
- Eliminar todas las tablas existentes del esquema public (drop_all_tables).
- Crear las tablas del modelo musical (artists, albums, tracks, etc.).
- Cargar los datos desde songs_integrated.json en dichas tablas.
"""

from __future__ import annotations

import logging

from .utils_io import setup_logging
from .utils_db import get_postgres_engine, drop_all_tables
from .load_schema import load_schema


def main() -> None:
    setup_logging()
    logger = logging.getLogger("main_load")

    logger.info("=== INICIO LOAD ===")

    try:
        engine = get_postgres_engine()

        # 1. BORRAR TODAS LAS TABLAS DE LA BASE DE DATOS (schema public)
        drop_all_tables(engine)

        # 2. Crear tablas y cargar datos desde songs_integrated.json
        load_schema(engine)

    except Exception as e:
        logger.exception(f"Error en LOAD: {e}")

    logger.info("=== FIN LOAD ===")


if __name__ == "__main__":
    main()
