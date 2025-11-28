"""
Transform del dataset Spotify Global Music (track_data_final.csv).

Responsabilidad:
- Leer Parquet RAW desde data/raw
- Limpiar columnas índice si las hubiera
- Asegurar tipos básicos (track_id string, fecha como string o datetime)
- Renombrar track_duration_ms -> duration_ms para alinearlo con otros datasets
- Deduplicar por track_id
- Guardar dataset limpio en data/processed
"""

from __future__ import annotations

import logging

import pandas as pd

from config import TRACK_DATA_FINAL_RAW_PARQUET, TRACK_DATA_FINAL_PROCESSED_PARQUET
from .utils_io import basic_profiling, write_parquet_with_logging

logger = logging.getLogger("transform_track_data_final")


def _clean_track_data_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza ligera y normalización de track_data_final.
    """
    # 1. Eliminar columnas tipo 'unnamed:_0' si apareciesen
    unnamed_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
    if unnamed_cols:
        logger.info("Eliminando columnas índice innecesarias: %s", unnamed_cols)
        df = df.drop(columns=unnamed_cols)

    # 2. Asegurar track_id como string
    if "track_id" in df.columns:
        df["track_id"] = df["track_id"].astype(str)
        missing = df["track_id"].isna().sum()
        if missing > 0:
            logger.warning("Filas con track_id nulo en track_data_final: %s", missing)
            df = df[df["track_id"].notna()]
    else:
        logger.warning(
            "El dataset track_data_final no contiene 'track_id'. "
            "La integración se verá limitada."
        )

    # 3. Renombrar track_duration_ms -> duration_ms para alinearlo con Spotify Tracks
    if "track_duration_ms" in df.columns:
        logger.info("Renombrando 'track_duration_ms' -> 'duration_ms'.")
        df = df.rename(columns={"track_duration_ms": "duration_ms"})

    # 4. Asegurar que album_release_date es string
    if "album_release_date" in df.columns:
        df["album_release_date"] = df["album_release_date"].astype(str)

    # 5. Deduplicar por track_id
    if "track_id" in df.columns:
        before = df.shape[0]
        df = df.drop_duplicates(subset=["track_id"])
        after = df.shape[0]
        if after < before:
            logger.info(
                "Eliminadas %s filas duplicadas por track_id en track_data_final.",
                before - after,
            )

    return df


def transform_track_data_final() -> None:
    """
    Ejecuta la fase de TRANSFORM para track_data_final.
    """
    dataset_name = "Spotify Global (track_data_final) (TRANSFORM)"

    logger.info("=== INICIO TRANSFORM: %s ===", dataset_name)

    logger.info("Leyendo RAW Parquet desde: %s", TRACK_DATA_FINAL_RAW_PARQUET)
    df = pd.read_parquet(TRACK_DATA_FINAL_RAW_PARQUET)

    df = _clean_track_data_final(df)

    basic_profiling(df, dataset_name)

    write_parquet_with_logging(df, TRACK_DATA_FINAL_PROCESSED_PARQUET, dataset_name)

    logger.info("=== FIN TRANSFORM: %s ===", dataset_name)
