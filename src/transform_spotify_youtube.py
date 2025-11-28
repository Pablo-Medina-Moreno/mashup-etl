"""
Transform del dataset Spotify–YouTube.

Responsabilidad:
- Leer Parquet RAW desde data/raw
- Limpiar columnas índice y texto basura
- Filtrar track_id inválidos (longitud distinta de 22 o nulos)
- Renombrar columnas relevantes (urls y métricas de YouTube)
- Quedarse con una fila por track_id (la de más views, si es posible)
- Guardar dataset limpio en data/processed
"""

from __future__ import annotations

import logging

import pandas as pd

from config import SPOTIFY_YOUTUBE_RAW_PARQUET, SPOTIFY_YOUTUBE_PROCESSED_PARQUET
from .utils_io import basic_profiling, write_parquet_with_logging

logger = logging.getLogger("transform_spotify_youtube")


def _clean_spotify_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones de limpieza al dataset Spotify–YouTube.
    """
    # 1. Eliminar columnas índice tipo 'unnamed:_0'
    unnamed_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
    if unnamed_cols:
        logger.info("Eliminando columnas índice innecesarias: %s", unnamed_cols)
        df = df.drop(columns=unnamed_cols)

    # 2. Asegurar track_id válido (lo extrajimos en la fase EXTRACT)
    if "track_id" not in df.columns:
        logger.warning(
            "El dataset Spotify–YouTube no tiene 'track_id'. "
            "No se podrá integrar bien con el resto."
        )
        return df

    df["track_id"] = df["track_id"].astype(str)
    # Filtrar nulos
    before = df.shape[0]
    df = df[df["track_id"].notna()]
    # Filtrar por longitud típica de Spotify
    df = df[df["track_id"].str.len() == 22]
    after = df.shape[0]
    if after < before:
        logger.info(
            "Filtradas %s filas con track_id nulo o longitud != 22 en Spotify–YouTube.",
            before - after,
        )

    # 3. Renombrar columnas relevantes para YouTube y Spotify URL
    rename_map = {}
    if "url_spotify" in df.columns:
        rename_map["url_spotify"] = "spotify_url"
    if "url_youtube" in df.columns:
        rename_map["url_youtube"] = "youtube_url"
    if "title" in df.columns:
        rename_map["title"] = "youtube_title"
    if "channel" in df.columns:
        rename_map["channel"] = "youtube_channel"
    if "views" in df.columns:
        rename_map["views"] = "youtube_views"
    if "likes" in df.columns:
        rename_map["likes"] = "youtube_likes"
    if "comments" in df.columns:
        rename_map["comments"] = "youtube_comments"
    if "description" in df.columns:
        rename_map["description"] = "youtube_description"
    if "licensed" in df.columns:
        rename_map["licensed"] = "youtube_licensed"
    if "official_video" in df.columns:
        rename_map["official_video"] = "youtube_official_video"
    if "stream" in df.columns:
        rename_map["stream"] = "spotify_streams"

    if rename_map:
        logger.info("Renombrando columnas Spotify–YouTube: %s", rename_map)
        df = df.rename(columns=rename_map)

    # 4. Nos quedamos solo con las columnas que aportan algo nuevo
    #    (para no duplicar cosas que ya tenemos mejor en Spotify/Global)
    keep_cols = ["track_id"]
    for col in [
        "spotify_url",
        "youtube_url",
        "youtube_title",
        "youtube_channel",
        "youtube_views",
        "youtube_likes",
        "youtube_comments",
        "youtube_description",
        "youtube_licensed",
        "youtube_official_video",
        "spotify_streams",
    ]:
        if col in df.columns:
            keep_cols.append(col)

    logger.info("Columnas finales que se conservarán en Spotify–YouTube: %s", keep_cols)
    df = df[keep_cols]

    # 5. Si hay varios vídeos por track_id, nos quedamos con el de más views
    if "youtube_views" in df.columns:
        logger.info("Agrupando por track_id y quedándonos con la fila de mayor youtube_views.")
        # Aseguramos numérico
        df["youtube_views"] = pd.to_numeric(df["youtube_views"], errors="coerce")
        df = (
            df.sort_values("youtube_views", ascending=False)
              .drop_duplicates(subset=["track_id"], keep="first")
        )
    else:
        # Simplemente deduplicamos por track_id
        before = df.shape[0]
        df = df.drop_duplicates(subset=["track_id"])
        after = df.shape[0]
        if after < before:
            logger.info(
                "Eliminadas %s filas duplicadas por track_id en Spotify–YouTube.",
                before - after,
            )

    return df


def transform_spotify_youtube() -> None:
    """
    Ejecuta la fase de TRANSFORM para el dataset Spotify–YouTube.
    """
    dataset_name = "Spotify–YouTube (TRANSFORM)"

    logger.info("=== INICIO TRANSFORM: %s ===", dataset_name)

    logger.info("Leyendo RAW Parquet desde: %s", SPOTIFY_YOUTUBE_RAW_PARQUET)
    df = pd.read_parquet(SPOTIFY_YOUTUBE_RAW_PARQUET)

    df = _clean_spotify_youtube(df)

    basic_profiling(df, dataset_name)

    write_parquet_with_logging(df, SPOTIFY_YOUTUBE_PROCESSED_PARQUET, dataset_name)

    logger.info("=== FIN TRANSFORM: %s ===", dataset_name)
