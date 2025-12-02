# src/transform_track_data_final.py
"""
Transform del dataset Spotify Global Music (track_data_final.csv).

Responsabilidad:
- Leer JSON RAW desde data/raw.
- Normalizar columnas específicas del dataset:
    * Asegurar prefijos track_/album_/artist_ cuando aplique.
    * Asegurar tipos básicos (track_id y album_id como string).
    * Generar album_spotify_url y track_spotify_url.
    * Asegurar album_release_date como string.
- Eliminar columnas índice si las hubiera.
- Asegurar track_id válido y filtrar filas sin ID.
- Parsear artist_genres (string con lista) a lista real.
- Construir, para cada fila, un objeto:
    {
      "track": {...},
      "album": {...},
      "artists": [...]
    }
- Guardar dataset limpio en data/processed.
"""

from __future__ import annotations

import logging
import ast

import pandas as pd

from config import TRACK_DATA_FINAL_RAW_JSON, TRACK_DATA_FINAL_PROCESSED_JSON
from .utils_io import basic_profiling, write_json_with_logging

logger = logging.getLogger("transform_track_data_final")


def _postprocess_track_data_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Postprocesado específico para track_data_final en TRANSFORM.

    - Renombrar columnas para prefijos consistentes (track_/album_).
    - Asegurar track_id y album_id como string (si existen).
    - Generar track_spotify_url y album_spotify_url.
    - Asegurar album_release_date como string (parseo de fecha se hará después).
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

    # Aquí podrías añadir otros mapeos si hubiera columnas sin prefijo
    # para artista o álbum, pero en este dataset ya suelen venir como
    # artist_* y album_*.

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

    # Asegurar fecha de álbum como string (se parseará en otros pasos si hace falta)
    if "album_release_date" in df.columns:
        df["album_release_date"] = df["album_release_date"].astype(str)

    # Las columnas de artista en este dataset ya vienen con prefijo artist_:
    # artist_name, artist_popularity, artist_followers, artist_genres
    # No hace falta tocarlas aquí.

    return df


def _parse_genres(value):
    """
    Convierte el campo artist_genres desde un string tipo
    "['brazilian bass', 'electronic']" a una lista real.

    Devuelve siempre una lista (posiblemente vacía).
    """
    if pd.isna(value):
        return []

    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]

    text = str(value).strip()
    if not text:
        return []

    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
        elif parsed is None:
            return []
        else:
            return [str(parsed).strip()]
    except (ValueError, SyntaxError):
        if "," in text:
            return [t.strip() for t in text.split(",") if t.strip()]
        return [text]


def _clean_track_data_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza ligera y normalización de track_data_final.

    - Elimina columnas índice tipo 'unnamed:_0'.
    - Valida y filtra por track_id no nulo.
    - Parsea artist_genres a una lista real.
    """
    # 1. Eliminar columnas tipo 'unnamed:_0' si apareciesen
    unnamed_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
    if unnamed_cols:
        logger.info("Eliminando columnas índice innecesarias: %s", unnamed_cols)
        df = df.drop(columns=unnamed_cols)

    # 2. Asegurar track_id como string y filtrar nulos
    if "track_id" in df.columns:
        df["track_id"] = df["track_id"].astype(str)
        before = len(df)
        df = df[df["track_id"].notna() & (df["track_id"] != "")]
        after = len(df)
        if after < before:
            logger.info(
                "Filtradas %s filas con track_id nulo o vacío en track_data_final.",
                before - after,
            )
    else:
        logger.warning(
            "El dataset track_data_final no contiene 'track_id'. "
            "La integración se verá limitada."
        )
        return df.iloc[0:0]

    # 3. Parsear artist_genres a lista real
    if "artist_genres" in df.columns:
        logger.info("Parseando columna 'artist_genres' (string -> lista).")
        df["artist_genres"] = df["artist_genres"].apply(_parse_genres)

    return df


def _row_to_nested_object(row: pd.Series) -> dict:
    """
    Dado un row de track_data_final, construye:
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
        "track_number": row.get("track_number"),
        "track_popularity": row.get("track_popularity"),
        "track_duration_ms": row.get("track_duration_ms"),
        "track_explicit": row.get("track_explicit"),
        "track_spotify_url": row.get("track_spotify_url"),
    }

    # --- ALBUM ---
    album_obj = {
        "album_id": row.get("album_id"),
        "album_name": row.get("album_name"),
        "album_release_date": row.get("album_release_date"),
        "album_total_tracks": row.get("album_total_tracks"),
        "album_type": row.get("album_type"),
        "album_spotify_url": row.get("album_spotify_url"),
        "album_artist_owner_id": row.get("album_artist_owner_id"),
    }

    # --- ARTIST (único en este dataset) ---
    artist_obj = {
        "artist_id": row.get("artist_id"),  # aún será None, se rellenará luego si procede
        "artist_name": row.get("artist_name"),
        "artist_popularity": row.get("artist_popularity"),
        "artist_followers": row.get("artist_followers"),
        "artist_genres": row.get("artist_genres"),
        "artist_spotify_url": row.get("artist_spotify_url"),
    }

    return {
        "track": track_obj,
        "album": album_obj,
        "artists": [artist_obj],
    }


def transform_track_data_final() -> None:
    """
    Ejecuta la fase de TRANSFORM para track_data_final.
    """
    dataset_name = "Spotify Global (track_data_final) (TRANSFORM)"

    logger.info("=== INICIO TRANSFORM: %s ===", dataset_name)
    logger.info("Leyendo RAW JSON desde: %s", TRACK_DATA_FINAL_RAW_JSON)

    # 1. Leer RAW JSON
    df = pd.read_json(TRACK_DATA_FINAL_RAW_JSON, lines=True)

    # 2. Postprocesado específico (renombrados, IDs, URLs, fechas)
    df = _postprocess_track_data_final(df)

    # 3. Limpieza y normalización (unnamed, track_id, artist_genres)
    df = _clean_track_data_final(df)

    # 4. Construir objetos anidados
    nested_records = df.apply(_row_to_nested_object, axis=1).tolist()
    nested_df = pd.DataFrame(nested_records)

    # 5. Profiling y guardado
    basic_profiling(nested_df, dataset_name)
    write_json_with_logging(nested_df, TRACK_DATA_FINAL_PROCESSED_JSON, dataset_name)

    logger.info("=== FIN TRANSFORM: %s ===", dataset_name)
