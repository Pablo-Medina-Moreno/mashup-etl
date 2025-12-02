# src/extract_spotify.py
"""
Extracción del dataset de Spotify Tracks (Spotify Kaggle).

Responsabilidad de la fase EXTRACT en este módulo:
- Leer el CSV original desde data/input.
- Normalizar nombres de columnas (snake_case, minúsculas, etc.).
- Hacer un profiling ligero.
- Guardar el resultado en JSON "raw" en data/raw.

Toda la lógica de negocio (renombrado a track_/album_,
generación de track_spotify_url, etc.) se hace en TRANSFORM.
"""

from __future__ import annotations

import logging

import pandas as pd

from config import SPOTIFY_TRACKS_CSV_PATH, SPOTIFY_TRACKS_RAW_JSON
from .utils_io import (
    read_csv_with_logging,
    write_json_with_logging,
    normalize_column_names,
    basic_profiling,
)

logger = logging.getLogger("extract_spotify")


def extract_spotify() -> None:
    """
    Ejecuta la fase de EXTRACT para el dataset de Spotify Tracks.
    """
    dataset_name = "Spotify Tracks"

    logger.info("=== INICIO EXTRACT: %s ===", dataset_name)

    # 1. Leer CSV de entrada
    df: pd.DataFrame = read_csv_with_logging(SPOTIFY_TRACKS_CSV_PATH, dataset_name)

    # 2. Normalizar nombres de columnas
    df = normalize_column_names(df)

    # 3. Profiling ligero
    basic_profiling(df, dataset_name)

    # 4. Guardar como JSON "raw" (una fila por línea)
    write_json_with_logging(df, SPOTIFY_TRACKS_RAW_JSON, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
