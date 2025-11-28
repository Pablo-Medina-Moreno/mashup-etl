"""
Transform del dataset de Spotify Tracks (Spotify Kaggle).

Responsabilidad:
- Leer Parquet RAW desde data/raw
- Limpiar columnas inútiles (ej. índices tipo 'unnamed:_0')
- Normalizar algunos nombres de columnas (artists -> artist_name, popularity -> spotify_popularity)
- Asegurar tipos básicos y deduplicar por track_id
- Guardar dataset limpio en data/processed
"""

from __future__ import annotations

import logging

import pandas as pd

from config import SPOTIFY_TRACKS_RAW_PARQUET, SPOTIFY_TRACKS_PROCESSED_PARQUET
from .utils_io import basic_profiling, write_parquet_with_logging

logger = logging.getLogger("transform_spotify")


def _clean_spotify_tracks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones de limpieza al dataset de Spotify Tracks.
    """
    # 1. Eliminar columnas índice tipo 'unnamed:_0'
    unnamed_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
    if unnamed_cols:
        logger.info("Eliminando columnas índice innecesarias: %s", unnamed_cols)
        df = df.drop(columns=unnamed_cols)

    # 2. Asegurar que track_id existe y es string
    if "track_id" not in df.columns:
        logger.warning(
            "El dataset Spotify Tracks no tiene 'track_id'. "
            "Esto dificultará la integración posterior."
        )
    else:
        df["track_id"] = df["track_id"].astype(str)
        missing_track_id = df["track_id"].isna().sum()
        if missing_track_id > 0:
            logger.warning("Filas con track_id nulo en Spotify Tracks: %s", missing_track_id)
            df = df[df["track_id"].notna()]

    # 3. Normalizar nombres de columnas clave para facilitar integración
    rename_map = {}
    if "artists" in df.columns:
        rename_map["artists"] = "artist_name"
    if "popularity" in df.columns:
        # Evitar conflicto con track_popularity del dataset global
        rename_map["popularity"] = "spotify_popularity"

    if rename_map:
        logger.info("Renombrando columnas Spotify Tracks: %s", rename_map)
        df = df.rename(columns=rename_map)

    # 4. Deduplicar por track_id (si existe)
    if "track_id" in df.columns:
        before = df.shape[0]
        df = df.drop_duplicates(subset=["track_id"])
        after = df.shape[0]
        if after < before:
            logger.info(
                "Eliminadas %s filas duplicadas por track_id en Spotify Tracks.",
                before - after,
            )

    return df


def transform_spotify() -> None:
    """
    Ejecuta la fase de TRANSFORM para el dataset de Spotify Tracks.
    """
    dataset_name = "Spotify Tracks (TRANSFORM)"

    logger.info("=== INICIO TRANSFORM: %s ===", dataset_name)

    # Leer el RAW Parquet
    logger.info("Leyendo RAW Parquet desde: %s", SPOTIFY_TRACKS_RAW_PARQUET)
    df = pd.read_parquet(SPOTIFY_TRACKS_RAW_PARQUET)

    # Limpiar y transformar
    df = _clean_spotify_tracks(df)

    # Profiling post-transform
    basic_profiling(df, dataset_name)

    # Guardar como PROCESSED
    write_parquet_with_logging(df, SPOTIFY_TRACKS_PROCESSED_PARQUET, dataset_name)

    logger.info("=== FIN TRANSFORM: %s ===", dataset_name)
