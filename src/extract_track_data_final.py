# src/extract_track_data_final.py
"""
Extracción del dataset Spotify Global (track_data_final.csv).

Responsabilidad:
- Leer el CSV original desde data/input
- Normalizar nombres de columnas
- Asegurar track_id como string
- Asegurar album_id como string
- Renombrar columnas para que:
    * Todas las columnas de track empiecen por track_
    * Todas las columnas de álbum empiecen por album_
    * Todas las columnas de artista empiecen por artist_
- Generar album_spotify_url = https://open.spotify.com/album/<album_id>
- Generar track_spotify_url = https://open.spotify.com/track/<track_id> (si existe)
- Asegurar album_release_date como string (fecha se parseará en TRANSFORM)
- Hacer profiling ligero
- Guardar en JSON en data/raw
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


def _postprocess_track_data_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocesado ligero para track_data_final en EXTRACT.
    Aquí SOLO renombramos columnas y generamos URLs, sin duplicar campos.
    """

    # ---------------------------------------------------------------------
    # 1. Renombrado de columnas para prefijos consistentes
    # ---------------------------------------------------------------------
    rename_map: dict[str, str] = {}

    # Asegurar prefijo track_ para columnas de track
    # (en este dataset ya suelen venir bien, pero explicit hay que renombrarla)
    if "explicit" in df.columns and "track_explicit" not in df.columns:
        rename_map["explicit"] = "track_explicit"

    # Posibles variantes raras de nombres de duración de track
    if "trackduration_ms" in df.columns and "track_duration_ms" not in df.columns:
        rename_map["trackduration_ms"] = "track_duration_ms"
    if "track_duration" in df.columns and "track_duration_ms" not in df.columns:
        rename_map["track_duration"] = "track_duration_ms"

    # Asegurar que las columnas de artista y álbum tienen prefijo correcto
    # (en tu dataset ya vienen como artist_* y album_*, pero por si acaso)
    # Aquí podrías añadir más mapeos si tuvieras variantes sin prefijo.
    # Por ejemplo:
    # if "artist" in df.columns and "artist_name" not in df.columns:
    #     rename_map["artist"] = "artist_name"

    # Aplicar renombrado (SIN duplicar columnas)
    if rename_map:
        df = df.rename(columns=rename_map)

    # ---------------------------------------------------------------------
    # 2. Tipos básicos y generación de URLs
    # ---------------------------------------------------------------------

    # Track ID
    if "track_id" in df.columns:
        df["track_id"] = df["track_id"].astype(str)
    else:
        logger.warning(
            "El dataset track_data_final no contiene columna 'track_id'. "
            "Esto complicará la integración posterior."
        )

    # Track Spotify URL (si tenemos track_id)
    if "track_id" in df.columns:
        df["track_spotify_url"] = "https://open.spotify.com/track/" + df["track_id"]
    else:
        # si no hay track_id no generamos la columna
        if "track_spotify_url" in df.columns:
            df = df.drop(columns=["track_spotify_url"])

    # Álbum: asegurar album_id y album_spotify_url
    if "album_id" in df.columns:
        df["album_id"] = df["album_id"].astype(str)
        df["album_spotify_url"] = "https://open.spotify.com/album/" + df["album_id"]
    else:
        logger.warning(
            "El dataset track_data_final no contiene columna 'album_id'. "
            "No se podrá generar album_spotify_url."
        )
        if "album_spotify_url" in df.columns:
            df = df.drop(columns=["album_spotify_url"])

    # Asegurar fecha de álbum como string (se parseará en TRANSFORM)
    if "album_release_date" in df.columns:
        df["album_release_date"] = df["album_release_date"].astype(str)

    # Las columnas de artista en este dataset ya vienen con prefijo artist_:
    # artist_name, artist_popularity, artist_followers, artist_genres
    # No hace falta tocarlas aquí.

    return df


def extract_track_data_final() -> None:
    """
    Ejecuta la fase de EXTRACT para el dataset track_data_final.
    """
    dataset_name = "Spotify Global (track_data_final)"

    logger.info("=== INICIO EXTRACT: %s ===", dataset_name)

    # 1. Leer CSV
    df = read_csv_with_logging(TRACK_DATA_FINAL_CSV_PATH, dataset_name)

    # 2. Normalizar nombres de columnas (lowercase, snake_case, etc.)
    df = normalize_column_names(df)

    # 3. Postprocesado ligero: renombrar columnas, tipos básicos, URLs
    df = _postprocess_track_data_final(df)

    # 4. Profiling ligero
    basic_profiling(df, dataset_name)

    # 5. Guardar en JSON (data/raw)
    write_json_with_logging(df, TRACK_DATA_FINAL_RAW_JSON, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
