"""
Extracción del dataset de Spotify Tracks (Spotify Kaggle).

Responsabilidad:
- Leer el CSV original desde data/input
- Normalizar nombres de columnas
- Asegurar que track_id existe y es string
- Hacer profiling ligero
- Guardar en Parquet en data/raw
"""

from __future__ import annotations

import logging

import pandas as pd

from config import SPOTIFY_TRACKS_CSV_PATH, SPOTIFY_TRACKS_RAW_PARQUET
from .utils_io import (
    read_csv_with_logging,
    write_parquet_with_logging,
    normalize_column_names,
    basic_profiling,
)


logger = logging.getLogger("extract_spotify")


def _postprocess_spotify_tracks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Operaciones ligeras específicas para spotify_tracks en EXTRACT.

    No se hace limpieza profunda (eso irá en TRANSFORM), solo:
    - asegurar que track_id existe y es string.
    """
    if "track_id" not in df.columns:
        logger.warning("El dataset Spotify Tracks no contiene columna 'track_id'.")
    else:
        df["track_id"] = df["track_id"].astype(str)

    return df


def extract_spotify() -> None:
    """
    Ejecuta la fase de EXTRACT para el dataset de Spotify Tracks.
    """
    dataset_name = "Spotify Tracks"

    logger.info("=== INICIO EXTRACT: %s ===", dataset_name)

    df = read_csv_with_logging(SPOTIFY_TRACKS_CSV_PATH, dataset_name)
    df = normalize_column_names(df)
    df = _postprocess_spotify_tracks(df)

    basic_profiling(df, dataset_name)
    write_parquet_with_logging(df, SPOTIFY_TRACKS_RAW_PARQUET, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
