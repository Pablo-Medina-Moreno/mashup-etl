# src/transform_spotify_youtube.py
"""
Transform del dataset Spotify–YouTube.

Responsabilidad:
- Leer JSON RAW desde data/raw
- Limpiar columnas índice (unnamed:_*)
- Asegurar track_id como string y descartar filas sin track_id
- Construir, para cada fila, un objeto:
    {
      "track": {...},
      "album": {...},
      "artists": [...]
    }
- Guardar en JSON en data/processed (una fila/objeto por línea)
"""

from __future__ import annotations

import logging

import pandas as pd

from config import SPOTIFY_YOUTUBE_RAW_JSON, SPOTIFY_YOUTUBE_PROCESSED_JSON
from .utils_io import basic_profiling, write_json_with_logging

logger = logging.getLogger("transform_spotify_youtube")


def _clean_spotify_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza ligera: solo columnas 'unnamed' y asegurar track_id string.
    """
    # 1. Eliminar columnas índice tipo 'unnamed:_0'
    unnamed_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
    if unnamed_cols:
        logger.info("Eliminando columnas índice innecesarias: %s", unnamed_cols)
        df = df.drop(columns=unnamed_cols)

    # 2. Asegurar track_id como string y filtrar nulos
    if "track_id" not in df.columns:
        logger.warning(
            "El dataset Spotify–YouTube no tiene 'track_id'. "
            "No se podrá integrar bien con el resto."
        )
        return df.iloc[0:0]  # df vacío

    df["track_id"] = df["track_id"].astype(str)
    before = len(df)
    df = df[df["track_id"].notna() & (df["track_id"] != "")]
    after = len(df)
    if after < before:
        logger.info(
            "Filtradas %s filas con track_id nulo o vacío en Spotify–YouTube.",
            before - after,
        )

    return df


def _row_to_nested_object(row: pd.Series) -> dict:
    """
    Dado un row de Spotify–YouTube (limpio), construye:
    {
      "track": {...},
      "album": {...},
      "artists": [...]
    }
    """
    # --- TRACK ---
    track_obj = {
        "track_id": row.get("track_id"),
        "track_name": row.get("track_name"),
        "track_duration_ms": row.get("track_duration_ms"),
        "track_danceability": row.get("track_danceability"),
        "track_energy": row.get("track_energy"),
        "track_key": row.get("track_key"),
        "track_loudness": row.get("track_loudness"),
        "track_speechiness": row.get("track_speechiness"),
        "track_acousticness": row.get("track_acousticness"),
        "track_instrumentalness": row.get("track_instrumentalness"),
        "track_liveness": row.get("track_liveness"),
        "track_valence": row.get("track_valence"),
        "track_tempo": row.get("track_tempo"),
        "track_spotify_url": row.get("track_spotify_url"),
        "track_spotify_streams": row.get("track_spotify_streams"),
        "track_youtube_url": row.get("track_youtube_url"),
        "track_youtube_title": row.get("track_youtube_title"),
        "track_youtube_channel": row.get("track_youtube_channel"),
        "track_youtube_views": row.get("track_youtube_views"),
        "track_youtube_likes": row.get("track_youtube_likes"),
        "track_youtube_comments": row.get("track_youtube_comments"),
        "track_youtube_description": row.get("track_youtube_description"),
        "track_youtube_licensed": row.get("track_youtube_licensed"),
        "track_youtube_official_video": row.get("track_youtube_official_video"),
    }

    # --- ALBUM ---
    album_obj = {
        "album_id": row.get("album_id"),  # suele ser None en este dataset
        "album_name": row.get("album_name"),
        "album_type": row.get("album_type"),
        "album_spotify_url": row.get("album_spotify_url"),  # por si se añade en otro paso
        "album_artist_owner_id": row.get("album_artist_owner_id"),
    }

    # --- ARTISTS ---
    artist_obj = {
        "artist_id": row.get("artist_id"),
        "artist_name": row.get("artist_name"),
        "artist_spotify_url": row.get("artist_spotify_url"),
        "artist_genres": row.get("artist_genres"),
    }
    artists_list = [artist_obj]

    return {
        "track": track_obj,
        "album": album_obj,
        "artists": artists_list,
    }


def transform_spotify_youtube() -> None:
    """
    Ejecuta la fase de TRANSFORM para el dataset Spotify–YouTube.
    """
    dataset_name = "Spotify–YouTube (TRANSFORM)"

    logger.info("=== INICIO TRANSFORM: %s ===", dataset_name)
    logger.info("Leyendo RAW JSON desde: %s", SPOTIFY_YOUTUBE_RAW_JSON)

    df = pd.read_json(SPOTIFY_YOUTUBE_RAW_JSON, lines=True)
    df = _clean_spotify_youtube(df)

    # Construir objetos anidados por fila
    nested_records = df.apply(_row_to_nested_object, axis=1).tolist()
    nested_df = pd.DataFrame(nested_records)

    basic_profiling(nested_df, dataset_name)
    write_json_with_logging(nested_df, SPOTIFY_YOUTUBE_PROCESSED_JSON, dataset_name)

    logger.info("=== FIN TRANSFORM: %s ===", dataset_name)
