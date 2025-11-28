# src/extract_spotify_youtube.py
"""
Extracción del dataset Spotify–YouTube.

Responsabilidad:
- Leer el CSV original desde data/input
- Normalizar nombres de columnas
- Extraer track_id desde la URI de Spotify (columna uri: spotify:track:<id>)
- Extraer artist_id desde la URL de Spotify (url_spotify con /artist/<id>)
- Renombrar columnas con prefijos track_/album_/artist_:
    - track_id, track_name, track_duration_ms, track_tempo, track_danceability, ...
    - track_spotify_uri (a partir de uri)
    - track_spotify_url (https://open.spotify.com/track/<track_id>)
    - track_youtube_url, track_youtube_title, track_youtube_channel, ...
    - track_spotify_streams
    - album_name, album_type (a nivel de track)
    - artist_name, artist_spotify_url, artist_id, artist_genres (array)
- Hacer profiling ligero
- Guardar en JSON en data/raw
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from config import SPOTIFY_YOUTUBE_CSV_PATH, SPOTIFY_YOUTUBE_RAW_JSON
from .utils_io import (
    read_csv_with_logging,
    write_json_with_logging,
    normalize_column_names,
    basic_profiling,
)

logger = logging.getLogger("extract_spotify_youtube")


def _postprocess_spotify_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocesado para el dataset Spotify-YouTube en EXTRACT.

    - track_id desde 'uri' (spotify:track:<id>)
    - artist_id desde 'url_spotify' (https://open.spotify.com/artist/<id>)
    - Renombrar columnas con prefijos track_/album_/artist_ y generar URLs.
    """

    # ==========================================================
    # 1. Extraer track_id desde la columna 'uri'
    #    (spotify:track:0d28khcov6AiegSCpG5TuT)
    # ==========================================================
    uri_col = "uri"
    if uri_col not in df.columns:
        logger.warning(
            "No se encontró columna '%s' en Spotify-YouTube. "
            "No se podrá extraer track_id.",
            uri_col,
        )
    else:
        logger.info("Extrayendo track_id desde columna '%s'...", uri_col)
        df[uri_col] = df[uri_col].astype(str)

        df["track_id"] = df[uri_col].str.split(":").str[-1]

        # Validación ligera de longitud típica de IDs de Spotify (22 chars)
        mask_valid = df["track_id"].str.len() == 22
        invalid_count = (~mask_valid).sum()
        if invalid_count > 0:
            logger.warning(
                "Se han encontrado %s track_id con longitud distinta de 22. "
                "Se dejan tal cual en EXTRACT; se revisarán en TRANSFORM.",
                invalid_count,
            )

    # Asegurar track_id como string (por si no había uri)
    if "track_id" in df.columns:
        df["track_id"] = df["track_id"].astype(str)

    # ==========================================================
    # 2. Renombrar columnas a prefijos track_/album_/artist_
    #    (sin duplicar columnas)
    # ==========================================================
    rename_map: dict[str, str] = {}

    # Artista
    if "artist" in df.columns:
        rename_map["artist"] = "artist_name"
    if "url_spotify" in df.columns:
        # Aquí Url_spotify es la URL del artista
        rename_map["url_spotify"] = "artist_spotify_url"

    # Track y álbum
    if "track" in df.columns:
        rename_map["track"] = "track_name"
    if "album" in df.columns:
        rename_map["album"] = "album_name"
    # album_type ya está bien tras normalize_column_names

    # Audio features -> track_
    audio_cols = [
        "danceability",
        "energy",
        "key",
        "loudness",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo",
    ]
    for col in audio_cols:
        if col in df.columns:
            if col == "tempo":
                rename_map[col] = "track_tempo"
            else:
                rename_map[col] = f"track_{col}"

    # Duración de la canción
    if "duration_ms" in df.columns:
        rename_map["duration_ms"] = "track_duration_ms"

    # URI de Spotify de la canción: la renombramos a track_spotify_uri
    if "uri" in df.columns:
        rename_map["uri"] = "track_spotify_uri"

    # Streams de Spotify
    if "stream" in df.columns:
        rename_map["stream"] = "track_spotify_streams"

    # Campos de YouTube -> track_youtube_*
    youtube_rename = {
        "url_youtube": "track_youtube_url",
        "title": "track_youtube_title",
        "channel": "track_youtube_channel",
        "views": "track_youtube_views",
        "likes": "track_youtube_likes",
        "comments": "track_youtube_comments",
        "description": "track_youtube_description",
        "licensed": "track_youtube_licensed",
        "official_video": "track_youtube_official_video",
    }
    for src, dst in youtube_rename.items():
        if src in df.columns:
            rename_map[src] = dst

    # Aplicar renombrado
    if rename_map:
        df = df.rename(columns=rename_map)

    # ==========================================================
    # 3. Generar track_spotify_url y extraer artist_id
    # ==========================================================

    # track_spotify_url canónica (https://open.spotify.com/track/<track_id>)
    if "track_id" in df.columns:
        df["track_spotify_url"] = "https://open.spotify.com/track/" + df["track_id"]

    # artist_id desde artist_spotify_url
    if "artist_spotify_url" not in df.columns:
        logger.warning(
            "No se encontró columna 'artist_spotify_url'; "
            "no se podrá extraer artist_id."
        )
        df["artist_id"] = None
    else:
        logger.info("Extrayendo artist_id desde 'artist_spotify_url'...")
        df["artist_spotify_url"] = df["artist_spotify_url"].astype(str)

        # Buscar /artist/<id> en la URL
        df["artist_id"] = df["artist_spotify_url"].str.extract(
            r"artist/([^/?]+)", expand=False
        )

        invalid_artist_ids = df["artist_id"].isna().sum()
        logger.info(
            "artist_id extraído. Filas con artist_id nulo (sin /artist/ en artist_spotify_url o vacío): %s",
            invalid_artist_ids,
        )

    # ==========================================================
    # 4. Campos adicionales:album_artist_owner_id
    # ==========================================================

    df["album_artist_owner_id"] = df["artist_id"]

    return df


def extract_spotify_youtube() -> None:
    """
    Ejecuta la fase de EXTRACT para el dataset Spotify–YouTube.
    """
    dataset_name = "Spotify-YouTube"

    logger.info("=== INICIO EXTRACT: %s ===", dataset_name)

    df = read_csv_with_logging(SPOTIFY_YOUTUBE_CSV_PATH, dataset_name)
    df = normalize_column_names(df)
    df = _postprocess_spotify_youtube(df)

    basic_profiling(df, dataset_name)
    write_json_with_logging(df, SPOTIFY_YOUTUBE_RAW_JSON, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
