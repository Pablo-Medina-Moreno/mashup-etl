# src/extract_spotify.py
"""
Extracción del dataset de Spotify Tracks (Spotify Kaggle).

Responsabilidad:
- Leer el CSV original desde data/input
- Normalizar nombres de columnas
- Asegurar que track_id existe y es string
- Renombrar columnas para que:
    * Todas las columnas de track empiecen por track_
    * Las columnas de álbum empiecen por album_
- Mantener la lista cruda de artistas como track_artists_raw
- Generar track_spotify_url = https://open.spotify.com/track/<track_id>
- Hacer profiling ligero
- Guardar en JSON en data/raw
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


def _postprocess_spotify_tracks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Operaciones ligeras específicas para spotify_tracks en EXTRACT.

    - Asegurar que track_id existe y es string.
    - Renombrar columnas con prefijo track_/album_ para audio features,
      popularidad y metadatos de track.
    - Generar track_spotify_url.
    """

    # ----------------------------------------------------------
    # 1. Renombrado de columnas (sin duplicar)
    # ----------------------------------------------------------
    rename_map: dict[str, str] = {}

    # Si la columna del id del track tiene otro nombre raro, aquí se podría mapear.
    # En el dataset estándar ya viene como track_id tras normalize_column_names.

    # Track name: a veces viene como "track"
    if "track" in df.columns and "track_name" not in df.columns:
        rename_map["track"] = "track_name"

    # Métricas básicas de track
    if "duration_ms" in df.columns:
        rename_map["duration_ms"] = "track_duration_ms"
    if "explicit" in df.columns:
        rename_map["explicit"] = "track_explicit"
    if "popularity" in df.columns:
        rename_map["popularity"] = "track_popularity"

    # Audio features -> track_
    audio_cols = [
        "danceability",
        "energy",
        "key",
        "loudness",
        "mode",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo",
        "time_signature",
    ]
    for col in audio_cols:
        if col in df.columns:
            rename_map[col] = f"track_{col}"

    # Género ya viene como track_genre, lo dejamos tal cual

    # Álbum: en este dataset solo tenemos el nombre de álbum
    # ya se llama album_name tras normalize_column_names, no hay que tocarlo.

    # Lista cruda de artistas del track (puede contener ';')
    if "artists" in df.columns:
        rename_map["artists"] = "track_artists_raw"

    # Aplicar el renombrado de una vez
    if rename_map:
        df = df.rename(columns=rename_map)

    # ----------------------------------------------------------
    # 2. Tipos y track_spotify_url
    # ----------------------------------------------------------
    if "track_id" not in df.columns:
        logger.warning("El dataset Spotify Tracks no contiene columna 'track_id'.")
    else:
        df["track_id"] = df["track_id"].astype(str)
        df["track_spotify_url"] = "https://open.spotify.com/track/" + df["track_id"]

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
    write_json_with_logging(df, SPOTIFY_TRACKS_RAW_JSON, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
