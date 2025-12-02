# src/main_transform.py
"""
Punto de entrada para la fase de Transform del ETL.

Uso:
    Desde la raíz del proyecto:

        python -m src.main_transform

Responsabilidad:
- Orquestar la fase de TRANSFORM para todos los datasets.
- Cada transform_*:
    * Lee el JSON "raw" desde data/raw.
    * Aplica la lógica de negocio (renombrados, URLs, parseos, etc.).
    * Construye objetos anidados {track, album, artists}.
    * Guarda el resultado limpio en data/processed.
- Finalmente, ejecuta la transformación integrada para obtener
  un único dataset maestro de canciones.
"""

from __future__ import annotations

import logging

from .utils_io import setup_logging
from .transform_spotify import transform_spotify
from .transform_spotify_youtube import transform_spotify_youtube
from .transform_track_data_final import transform_track_data_final
from .transform_integrated import transform_integrated


def main() -> None:
    """
    Orquesta la ejecución completa de la fase de Transform.
    Ejecuta:
        1. Transform individual de Spotify Tracks.
        2. Transform individual de Spotify–YouTube.
        3. Transform individual de track_data_final.
        4. Integración final en un único dataset maestro.
    """
    setup_logging()
    logger = logging.getLogger("main_transform")

    logger.info("=== INICIO TRANSFORM GLOBAL ===")

    # 1. Spotify Tracks
    try:
        transform_spotify()
    except Exception as e:
        logger.exception(f"Error en TRANSFORM de Spotify Tracks: {e}")

    # 2. Spotify–YouTube
    try:
        transform_spotify_youtube()
    except Exception as e:
        logger.exception(f"Error en TRANSFORM de Spotify–YouTube: {e}")

    # 3. track_data_final (Spotify Global)
    try:
        transform_track_data_final()
    except Exception as e:
        logger.exception(f"Error en TRANSFORM de track_data_final: {e}")

    # 4. Integración final
    try:
        transform_integrated()
    except Exception as e:
        logger.exception(f"Error en TRANSFORM integrado: {e}")

    logger.info("=== FIN TRANSFORM GLOBAL ===")


if __name__ == "__main__":
    main()
