# src/main_load.py

from __future__ import annotations

import logging

from .utils_io import setup_logging
from .utils_db import get_postgres_engine, drop_all_tables, create_star_schema_tables
from .load_schema import load_schema


def main() -> None:
    setup_logging()
    logger = logging.getLogger("main_load")

    logger.info("=== INICIO LOAD ===")

    try:
        engine = get_postgres_engine()

        # 1. BORRAR TODAS LAS TABLAS DE LA BASE DE DATOS
        drop_all_tables(engine)

        # 2. Crear el modelo en estrella
        create_star_schema_tables(engine)

        # 3. Cargar datos
        load_schema(engine)

    except Exception as e:
        logger.exception(f"Error en LOAD: {e}")

    logger.info("=== FIN LOAD ===")


if __name__ == "__main__":
    main()
