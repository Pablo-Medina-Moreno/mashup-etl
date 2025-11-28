"""
Extracción del dataset Spotify–YouTube.

Responsabilidad:
- Leer el CSV original desde data/input
- Normalizar nombres de columnas
- Extraer track_id desde la URI de Spotify (columna uri)
- Extraer artist_id desde la URL de Spotify (artist o track si viene como /artist/...)
- Hacer profiling ligero
- Guardar en Parquet en data/raw
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from config import SPOTIFY_YOUTUBE_CSV_PATH, SPOTIFY_YOUTUBE_RAW_PARQUET
from .utils_io import (
    read_csv_with_logging,
    write_parquet_with_logging,
    normalize_column_names,
    basic_profiling,
)


logger = logging.getLogger("extract_spotify_youtube")


def _extract_spotify_artist_id(url: str) -> str | None:
    """
    Extrae el ID de un artista desde un URL de Spotify.

    Ejemplos válidos:
        https://open.spotify.com/artist/3AA28KZvwAUcZuOKwyblJQ
        https://open.spotify.com/artist/3AA28KZvwAUcZuOKwyblJQ?si=algo

    Si el URL no contiene /artist/<id>, devuelve None.
    """
    if not isinstance(url, str):
        return None

    match = re.search(r"spotify\.com/artist/([A-Za-z0-9]+)", url)
    if match:
        return match.group(1)
    return None


def _postprocess_spotify_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocesado para el dataset Spotify-YouTube en EXTRACT.

    Objetivo principal:
    - extraer track_id de Spotify desde la columna URI (spotify:track:<id>)
      y crear una columna explícita 'track_id'.
    - extraer artist_id de Spotify desde la URL (si está disponible)
      y crear una columna explícita 'artist_id'.
    """
    # ======================
    #  TRACK_ID desde URI
    # ======================
    uri_col_candidates = ["uri", "spotify_uri"]
    uri_col = None

    for col in uri_col_candidates:
        if col in df.columns:
            uri_col = col
            break

    if uri_col is None:
        logger.warning(
            "No se encontró columna URI en Spotify-YouTube (buscado: %s). "
            "No se podrá extraer track_id.",
            uri_col_candidates,
        )
    else:
        logger.info("Usando columna '%s' para extraer track_id de Spotify.", uri_col)

        df["track_id"] = (
            df[uri_col]
            .astype(str)
            .str.split(":")
            .str[-1]
        )

        # Validación ligera de longitud de track_id (22 chars típico de Spotify)
        mask_valid = df["track_id"].str.len() == 22
        invalid_count = (~mask_valid).sum()

        if invalid_count > 0:
            logger.warning(
                "Se han encontrado %s track_id no válidos (longitud != 22). "
                "Se dejan tal cual en EXTRACT; se revisarán en TRANSFORM.",
                invalid_count,
            )

    # ======================
    #  ARTIST_ID desde URL
    # ======================
    # Intentamos encontrar una columna que contenga el URL de Spotify.
    # En tu dataset original comentaste que viene en Spotify Youtube Dataset.csv.
    # Probables nombres (ya normalizados): 'url_spotify', 'artist_url', 'artist_spotify_url'
    artist_url_col = None
    artist_url_candidates = ["artist_url", "artist_spotify_url", "url_spotify"]

    for col in artist_url_candidates:
        if col in df.columns:
            artist_url_col = col
            break

    if artist_url_col is None:
        logger.warning(
            "No se encontró columna de URL de artista en Spotify-YouTube "
            "(buscado: %s). No se podrá extraer artist_id.",
            artist_url_candidates,
        )
    else:
        logger.info("Usando columna '%s' para extraer artist_id de Spotify.", artist_url_col)
        df["artist_id"] = df[artist_url_col].apply(_extract_spotify_artist_id)
        invalid_artist_ids = df["artist_id"].isna().sum()
        logger.info(
            "artist_id extraído. Filas con artist_id nulo (sin /artist/ en URL o vacío): %s",
            invalid_artist_ids,
        )

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
    write_parquet_with_logging(df, SPOTIFY_YOUTUBE_RAW_PARQUET, dataset_name)

    logger.info("=== FIN EXTRACT: %s ===", dataset_name)
