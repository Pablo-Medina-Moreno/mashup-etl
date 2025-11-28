# src/main_extract.py
"""
Punto de entrada para la fase de Extract del ETL.

Ejemplo:
    python -m src.main_extract
"""

from __future__ import annotations

import logging

from .utils_io import setup_logging, ensure_directories
from .extract_spotify import extract_spotify
from .extract_spotify_youtube import extract_spotify_youtube
from .extract_track_data_final import extract_track_data_final


def main() -> None:
    """
    Orquesta la ejecución completa de la fase de extracción.
    Ejecuta TODOS los Extract:
        1. Spotify Tracks (Kaggle)
        2. Spotify–YouTube Dataset
        3. Spotify Global Music (track_data_final)
    """
    setup_logging()
    logger = logging.getLogger("main_extract")

    logger.info("=== INICIO EXTRACT GLOBAL ===")

    # Asegurar directorios base (input/raw)
    ensure_directories()

    # 1. Spotify Tracks
    try:
        extract_spotify()
    except Exception as e:
        logger.exception(f"Error en extracción de Spotify Tracks: {e}")

    # 2. Spotify–YouTube
    try:
        extract_spotify_youtube()
    except Exception as e:
        logger.exception(f"Error en extracción de Spotify–YouTube: {e}")

    # 3. Spotify Global (track_data_final)
    try:
        extract_track_data_final()
    except Exception as e:
        logger.exception(f"Error en extracción de track_data_final: {e}")

    logger.info("=== FIN EXTRACT GLOBAL ===")


if __name__ == "__main__":
    main()
