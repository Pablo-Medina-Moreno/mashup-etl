# src/extract_track_data_final.py
"""
Extracción del dataset Spotify Global (track_data_final.csv).

Responsabilidad de la fase EXTRACT en este módulo:
- Leer el CSV original desde data/input.
- Normalizar nombres de columnas (snake_case, minúsculas, etc.).
- Hacer un profiling ligero.
- Guardar el resultado en JSON "raw" en data/raw.

Toda la lógica de negocio (prefijos track_/album_/artist_,
generación de URLs de Spotify, etc.) se hace en la fase TRANSFORM.
"""

from __future__ import annotations

import logging

import pandas as pd

from config import TRACK_DATA_FINAL_CSV_PATH, TRACK_DATA_FINAL_RAW_JSON
from .utils_io import (
    read_csv_with_logging,
    write_json_with_logging,
    normalize_column_names,
    basic_profiling,
)

logger = logging.getLogger("extract_track_data_final")


def extract_track_data_final() -> None:
    """
    Ejecuta la fase de EXTRACT para el dataset track_data_final.
    """
    dataset_name = "Spotify Global (track_data_final)"

    logger.info("=== INICIO EXTRACT: %s ===", dataset_name)

    # 1. Leer CSV
    df: pd.DataFrame = read_csv_with_logging(TRACK_DATA_FINAL_CSV_PATH, dataset_name)

    # 2. Normalizar nombres de columnas (lowercase, snake_case, etc.)
    df = normalize_column_names(df)

    # 3. Profiling ligero
    basic_profiling(df, dataset_name)

    # 4. Guardar en JSON (data/raw)
    write_json_with_logging(df, TRACK_DATA_FINAL_RAW_JSON, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
