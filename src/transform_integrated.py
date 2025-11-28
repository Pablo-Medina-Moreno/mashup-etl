"""
Transform de integración: une los tres datasets ya transformados
en un único dataset maestro de canciones.

Nueva prioridad de fuentes (para la existencia de canciones y datos base):

    1) spotify_youtube                 -> garantiza que no perdemos temas que solo están en este dataset
    2) spotify_tracks (Kaggle grande)  -> verdad principal para metadatos y audio features
    3) track_data_final (Spotify Global) -> complementa con metadatos extra

Idea:
    - spotify_youtube aporta sobre todo:
        spotify_url, youtube_url, youtube_* y spotify_streams.
      Además, usando un outer join final nos aseguramos de que cualquier track_id
      que aparezca solo en spotify_youtube también esté en el integrado.
    - spotify_tracks sigue siendo la fuente principal de:
        track_name, artist_name, album_name, duration_ms, explicit, track_genre, spotify_popularity
    - track_data_final aporta campos adicionales (album_release_date, artist_popularity, etc.)
      pero NO sobreescribe lo que viene de spotify_tracks.
"""

from __future__ import annotations

import logging
from typing import List

import pandas as pd

from config import (
    SPOTIFY_TRACKS_PROCESSED_PARQUET,
    SPOTIFY_YOUTUBE_PROCESSED_PARQUET,
    TRACK_DATA_FINAL_PROCESSED_PARQUET,
    SONGS_INTEGRATED_PARQUET,
)
from .utils_io import basic_profiling, write_parquet_with_logging

logger = logging.getLogger("transform_integrated")


def _coalesce_columns(df: pd.DataFrame, columns: List[str], new_name: str) -> pd.DataFrame:
    """
    Crea una nueva columna new_name combinando por prioridad
    las columnas de 'columns' (primera no nula gana).

    'columns' debe estar ordenada por prioridad: primero la fuente
    con mayor prioridad, luego las siguientes (por ejemplo:
    ['track_name_sp', 'track_name_global']).
    """
    cols_present = [c for c in columns if c in df.columns]
    if not cols_present:
        return df

    # Empezamos con una copia de la primera columna (máxima prioridad)
    result = df[cols_present[0]].copy()

    # Para cada columna siguiente, rellenamos solo donde result es nulo
    for col in cols_present[1:]:
        mask = result.isna()
        result[mask] = df[col][mask]

    df[new_name] = result
    return df


def transform_integrated() -> None:
    """
    Ejecuta la integración de los tres datasets ya limpios con prioridad:

        1) spotify_youtube (para no perder temas y añadir métricas)
        2) spotify_tracks (metadatos y audio features)
        3) track_data_final (metadatos extra)
    """
    dataset_name = "Songs Integrated"

    logger.info("=== INICIO TRANSFORM INTEGRATED: %s ===", dataset_name)

    # 1. Cargar datasets procesados individuales
    logger.info("Leyendo PROCESSED Spotify Tracks desde: %s", SPOTIFY_TRACKS_PROCESSED_PARQUET)
    df_sp = pd.read_parquet(SPOTIFY_TRACKS_PROCESSED_PARQUET)

    logger.info("Leyendo PROCESSED Spotify–YouTube desde: %s", SPOTIFY_YOUTUBE_PROCESSED_PARQUET)
    df_yt = pd.read_parquet(SPOTIFY_YOUTUBE_PROCESSED_PARQUET)

    logger.info("Leyendo PROCESSED track_data_final desde: %s", TRACK_DATA_FINAL_PROCESSED_PARQUET)
    df_global = pd.read_parquet(TRACK_DATA_FINAL_PROCESSED_PARQUET)

    # 2. Mergeo Spotify Tracks + Global (outer join por track_id)
    #    PRIORIDAD en metadatos base: SPOTIFY_TRACKS > GLOBAL
    logger.info("Uniendo Spotify Tracks (Kaggle) con track_data_final (GLOBAL).")
    df_merged = df_sp.merge(
        df_global,
        on="track_id",
        how="outer",
        suffixes=("_sp", "_global"),
    )

    # 3. Coalesce de columnas clave según prioridad (SPOTIFY > GLOBAL)
    # track_name
    df_merged = _coalesce_columns(
        df_merged,
        ["track_name_sp", "track_name_global"],
        "track_name",
    )
    # artist_name
    df_merged = _coalesce_columns(
        df_merged,
        ["artist_name_sp", "artist_name_global"],
        "artist_name",
    )
    # album_name
    df_merged = _coalesce_columns(
        df_merged,
        ["album_name_sp", "album_name_global"],
        "album_name",
    )
    # duration_ms
    df_merged = _coalesce_columns(
        df_merged,
        ["duration_ms_sp", "duration_ms_global"],
        "duration_ms",
    )
    # explicit
    df_merged = _coalesce_columns(
        df_merged,
        ["explicit_sp", "explicit_global"],
        "explicit",
    )
    # track_genre (solo existe en Spotify Tracks normalmente, pero por si acaso)
    df_merged = _coalesce_columns(
        df_merged,
        ["track_genre", "track_genres"],  # si hubiese alguna variante
        "track_genre",
    )

    # 4. Mantener popularidades como columnas separadas (útil) o unificarlas
    #    Aquí creamos una columna 'popularity' priorizando spotify_popularity
    if "spotify_popularity" in df_merged.columns or "track_popularity" in df_merged.columns:
        df_merged = _coalesce_columns(
            df_merged,
            ["spotify_popularity", "track_popularity"],
            "popularity",
        )

    # 5. Eliminar las columnas originales duplicadas para no ensuciar el schema
    drop_cols = [
        col
        for col in df_merged.columns
        if col.endswith("_sp") or col.endswith("_global")
    ]
    logger.info("Eliminando columnas duplicadas de origen: %s", drop_cols)
    df_merged = df_merged.drop(columns=drop_cols)

    # 6. Merge con Spotify–YouTube para añadir métricas de YouTube
    #    PRIORIDAD: spotify_youtube manda en el sentido de que todo track_id
    #    presente ahí se conserva (usamos outer join), y sus columnas
    #    (spotify_url, youtube_*) nunca se pisan porque son únicas.
    logger.info("Uniendo con Spotify–YouTube (métricas de YouTube y URLs).")
    df_final = df_merged.merge(df_yt, on="track_id", how="outer")

    # 7. Profiling del dataset integrado
    basic_profiling(df_final, dataset_name)

    # 8. Guardar el dataset integrado
    write_parquet_with_logging(df_final, SONGS_INTEGRATED_PARQUET, dataset_name)

    logger.info("=== FIN TRANSFORM INTEGRATED: %s ===", dataset_name)
