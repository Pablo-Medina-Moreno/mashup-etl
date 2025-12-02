# src/transform_spotify_youtube.py
"""
Transform del dataset Spotify–YouTube.

Responsabilidad:
- Leer JSON RAW desde data/raw.
- Postprocesar las columnas específicas del dataset:
    - Extraer track_id desde la URI de Spotify (columna `uri`: spotify:track:<id>).
    - Extraer artist_id desde la URL de Spotify (`url_spotify` / `artist_spotify_url`).
    - Renombrar columnas con prefijos track_/album_/artist_.
    - Generar URLs canónicas de Spotify y campos derivados.
- Limpiar columnas índice (unnamed:_*).
- Asegurar track_id como string y descartar filas sin track_id.
- Construir, para cada fila, un objeto:
    {
      "track": {...},
      "album": {...},
      "artists": [...]
    }
- Guardar en JSON en data/processed (una fila/objeto por línea).
"""

from __future__ import annotations

import logging

import pandas as pd

from config import SPOTIFY_YOUTUBE_RAW_JSON, SPOTIFY_YOUTUBE_PROCESSED_JSON
from .utils_io import basic_profiling, write_json_with_logging

logger = logging.getLogger("transform_spotify_youtube")


def _postprocess_spotify_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocesado específico para el dataset Spotify–YouTube en TRANSFORM.

    - Extraer track_id desde 'uri' (spotify:track:<id>).
    - Renombrar columnas con prefijos track_/album_/artist_.
    - Generar track_spotify_url.
    - Extraer artist_id desde 'artist_spotify_url'.
    - Derivar album_artist_owner_id.
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
                "Se dejan tal cual en TRANSFORM; se podrían revisar en pasos posteriores.",
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
        # Aquí url_spotify es la URL del artista
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
    # 4. Campos adicionales: album_artist_owner_id
    # ==========================================================

    df["album_artist_owner_id"] = df.get("artist_id")

    return df


def _clean_spotify_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza ligera: eliminar columnas 'unnamed' y asegurar track_id string.

    Se asume que, si era posible, track_id ya ha sido generado
    previamente en _postprocess_spotify_youtube.
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
    Dado un row de Spotify–YouTube (ya limpio y postprocesado), construye:
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

    # 1. Leer RAW JSON
    df = pd.read_json(SPOTIFY_YOUTUBE_RAW_JSON, lines=True)

    # 2. Postprocesado específico del dataset (renombrados, IDs, URLs, etc.)
    df = _postprocess_spotify_youtube(df)

    # 3. Limpieza genérica (unnamed, filtrado por track_id)
    df = _clean_spotify_youtube(df)

    # 4. Construir objetos anidados por fila
    nested_records = df.apply(_row_to_nested_object, axis=1).tolist()
    nested_df = pd.DataFrame(nested_records)

    # 5. Profiling y guardado
    basic_profiling(nested_df, dataset_name)
    write_json_with_logging(nested_df, SPOTIFY_YOUTUBE_PROCESSED_JSON, dataset_name)

    logger.info("=== FIN TRANSFORM: %s ===", dataset_name)
