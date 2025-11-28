"""
Extracción del dataset Spotify Global Music (track_data_final.csv).

Responsabilidad:
- Leer el CSV original desde data/input
- Normalizar nombres de columnas
- Asegurar track_id como string
- Asegurar album_release_date como string (fecha se parseará en TRANSFORM)
- Hacer profiling ligero
- Guardar en Parquet en data/raw
"""

from __future__ import annotations

import logging

import pandas as pd

from config import TRACK_DATA_FINAL_CSV_PATH, TRACK_DATA_FINAL_RAW_PARQUET
from .utils_io import (
    read_csv_with_logging,
    write_parquet_with_logging,
    normalize_column_names,
    basic_profiling,
)


logger = logging.getLogger("extract_track_data_final")


def _postprocess_track_data_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocesado ligero para track_data_final en EXTRACT.
    """
    if "track_id" in df.columns:
        df["track_id"] = df["track_id"].astype(str)
    else:
        logger.warning(
            "El dataset track_data_final no contiene columna 'track_id'. "
            "Esto complicará la integración posterior."
        )

    if "album_release_date" in df.columns:
        df["album_release_date"] = df["album_release_date"].astype(str)

    return df


def extract_track_data_final() -> None:
    """
    Ejecuta la fase de EXTRACT para el dataset track_data_final.
    """
    dataset_name = "Spotify Global (track_data_final)"

    logger.info("=== INICIO EXTRACT: %s ===", dataset_name)

    df = read_csv_with_logging(TRACK_DATA_FINAL_CSV_PATH, dataset_name)
    df = normalize_column_names(df)
    df = _postprocess_track_data_final(df)

    basic_profiling(df, dataset_name)
    write_parquet_with_logging(df, TRACK_DATA_FINAL_RAW_PARQUET, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
