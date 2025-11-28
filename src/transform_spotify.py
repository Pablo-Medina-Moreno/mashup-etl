# src/transform_spotify.py
"""
Transform del dataset de Spotify Tracks (Spotify Kaggle).

Responsabilidad:
- Leer JSON RAW desde data/raw
- Limpiar columnas inútiles (ej. índices tipo 'unnamed:_0')
- Asegurar track_id como string y descartar filas sin track_id
- Parsear artistas múltiples:
    - track_artists_raw (string original)
    - track_artists_list (lista de artistas)
- Construir, para cada fila, un objeto:
    {
      "track": {...},
      "album": {...},
      "artists": [...]
    }
- Guardar dataset transformado en data/processed
"""

from __future__ import annotations

import logging
from typing import List

import pandas as pd

from config import SPOTIFY_TRACKS_RAW_JSON, SPOTIFY_TRACKS_PROCESSED_JSON
from .utils_io import basic_profiling, write_json_with_logging

logger = logging.getLogger("transform_spotify")


def _split_artists(value) -> List[str]:
    """
    Recibe el valor de 'track_artists_raw' (string tipo
    'Jason Mraz;Colbie Caillat') y devuelve una lista limpia.
    """
    if pd.isna(value):
        return []
    text = str(value)
    parts = text.split(";")
    return [p.strip() for p in parts if p.strip()]


def _clean_spotify_tracks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza ligera del dataset Spotify Tracks.
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
        return df.iloc[0:0]

    df["track_id"] = df["track_id"].astype(str)
    before = len(df)
    df = df[df["track_id"].notna() & (df["track_id"] != "")]
    after = len(df)
    if after < before:
        logger.info(
            "Filtradas %s filas con track_id nulo o vacío en Spotify Tracks.",
            before - after,
        )

    # 3. Manejo de artistas múltiples
    if "track_artists_raw" in df.columns:
        logger.info("Procesando 'track_artists_raw' para manejar artistas múltiples.")
        df["track_artists_list"] = df["track_artists_raw"].apply(_split_artists)
    else:
        logger.info("No se encontró 'track_artists_raw'; inicializando listas vacías.")
        df["track_artists_list"] = [[] for _ in range(len(df))]

    return df


def _row_to_nested_object(row: pd.Series) -> dict:
    """
    Dado un row de Spotify Tracks, construye:
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
        "track_explicit": row.get("track_explicit"),
        "track_popularity": row.get("track_popularity"),
        "track_spotify_popularity": row.get("track_spotify_popularity"),
        "track_danceability": row.get("track_danceability"),
        "track_energy": row.get("track_energy"),
        "track_key": row.get("track_key"),
        "track_loudness": row.get("track_loudness"),
        "track_mode": row.get("track_mode"),
        "track_speechiness": row.get("track_speechiness"),
        "track_acousticness": row.get("track_acousticness"),
        "track_instrumentalness": row.get("track_instrumentalness"),
        "track_liveness": row.get("track_liveness"),
        "track_valence": row.get("track_valence"),
        "track_tempo": row.get("track_tempo"),
        "track_time_signature": row.get("track_time_signature"),
        "track_genre": row.get("track_genre"),
        "track_spotify_url": row.get("track_spotify_url"),
    }

    # --- ALBUM ---
    album_obj = {
        "album_id": row.get("album_id"),  # este dataset no la trae: será None
        "album_name": row.get("album_name"),
        "album_type": row.get("album_type"),
        "album_spotify_url": row.get("album_spotify_url"),
        "album_artist_owner_id": row.get("album_artist_owner_id"),
    }

    # --- ARTISTS ---
    artists_list_raw = row.get("track_artists_list") or []
    artists_objs = [
        {
            "artist_id": None,
            "artist_name": name,
            "artist_spotify_url": None,
            "artist_genres": None,
        }
        for name in artists_list_raw
    ]

    return {
        "track": track_obj,
        "album": album_obj,
        "artists": artists_objs,
    }


def transform_spotify() -> None:
    """
    Ejecuta la fase de TRANSFORM para el dataset de Spotify Tracks.
    """
    dataset_name = "Spotify Tracks (TRANSFORM)"

    logger.info("=== INICIO TRANSFORM: %s ===", dataset_name)
    logger.info("Leyendo RAW JSON desde: %s", SPOTIFY_TRACKS_RAW_JSON)

    df = pd.read_json(SPOTIFY_TRACKS_RAW_JSON, lines=True)
    df = _clean_spotify_tracks(df)

    nested_records = df.apply(_row_to_nested_object, axis=1).tolist()
    nested_df = pd.DataFrame(nested_records)

    basic_profiling(nested_df, dataset_name)
    write_json_with_logging(nested_df, SPOTIFY_TRACKS_PROCESSED_JSON, dataset_name)

    logger.info("=== FIN TRANSFORM: %s ===", dataset_name)
