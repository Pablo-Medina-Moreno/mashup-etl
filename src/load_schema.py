# src/load_schema.py
"""
Lógica de carga (LOAD) del modelo en estrella en PostgreSQL
a partir de songs_integrated.parquet.
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy.engine import Engine

from config import SONGS_INTEGRATED_PARQUET
from .utils_db import create_star_schema_tables, truncate_star_schema_tables

logger = logging.getLogger(__name__)


def build_dim_artists(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye el DataFrame de la dimensión de artistas a partir del
    dataset integrado.

    Columnas usadas:
        - artist_name
        - artist_popularity
        - artist_followers
        - artist_genres
    """
    cols = ["artist_name", "artist_popularity", "artist_followers", "artist_genres"]
    df_artists = df[cols].dropna(subset=["artist_name"]).drop_duplicates(subset=["artist_name"])
    df_artists = df_artists.reset_index(drop=True)
    logger.info("dim_artists: %s filas", df_artists.shape[0])
    return df_artists


def build_dim_albums(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye el DataFrame de la dimensión de álbumes.

    Columnas usadas:
        - album_id
        - album_name
        - album_type
        - album_release_date
        - album_total_tracks
    """
    cols = ["album_id", "album_name", "album_type", "album_release_date", "album_total_tracks"]

    # 🔴 ANTES: solo dropna en album_id
    # df_albums = df[cols].dropna(subset=["album_id"]).drop_duplicates(subset=["album_id"])

    # ✅ AHORA: exigimos también album_name no nulo
    df_albums = (
        df[cols]
        .dropna(subset=["album_id", "album_name"])
        .drop_duplicates(subset=["album_id"])
    )

    # Parseo de fecha
    df_albums["album_release_date"] = pd.to_datetime(
        df_albums["album_release_date"], errors="coerce"
    ).dt.date

    df_albums = df_albums.reset_index(drop=True)
    logger.info("dim_albums: %s filas", df_albums.shape[0])
    return df_albums



def build_fact_tracks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye el DataFrame de la tabla de hechos fact_tracks
    usando solo las columnas que se quieren cargar a BD.

    Columnas usadas (todas existen en songs_integrated):
        track_id, artist_name, album_id, track_name,
        track_genre, track_number,
        spotify_popularity, track_popularity, popularity,
        danceability, energy, key, loudness, mode, liveness, valence, tempo,
        spotify_url, youtube_url, youtube_title, youtube_channel
    """
    cols = [
        "track_id",
        "artist_name",
        "album_id",
        "track_name",
        "track_genre",
        "track_number",
        "spotify_popularity",
        "track_popularity",
        "popularity",
        "danceability",
        "energy",
        "key",
        "loudness",
        "mode",
        "liveness",
        "valence",
        "tempo",
        "spotify_url",
        "youtube_url",
        "youtube_title",
        "youtube_channel",
    ]

    df_fact = df[cols].dropna(subset=["track_id", "track_name"])
    df_fact = df_fact.drop_duplicates(subset=["track_id"])
    df_fact = df_fact.reset_index(drop=True)
    logger.info("fact_tracks: %s filas", df_fact.shape[0])
    return df_fact


def load_schema(engine: Engine) -> None:
    """
    Orquesta el proceso de LOAD:

        1. Leer songs_integrated.parquet
        2. Crear tablas si no existen
        3. Truncar tablas (opcional, aquí Sí lo hacemos para recarga completa)
        4. Construir dataframes de dim_artists, dim_albums y fact_tracks
        5. Insertar en PostgreSQL en el orden correcto
    """
    logger.info("Leyendo songs_integrated desde: %s", SONGS_INTEGRATED_PARQUET)
    df = pd.read_parquet(SONGS_INTEGRATED_PARQUET)

    # Crear tablas si no existen
    create_star_schema_tables(engine)

    # Truncar contenido (recarga completa)
    truncate_star_schema_tables(engine)

    # Construir dataframes para cada tabla del modelo en estrella
    df_artists = build_dim_artists(df)
    df_albums = build_dim_albums(df)
    df_fact = build_fact_tracks(df)

    # Cargar a PostgreSQL usando pandas.to_sql
    # NOTA: asumimos que el esquema coincide; if_exists='append' para no tocar estructura.
    logger.info("Insertando dim_artists...")
    df_artists.to_sql("dim_artists", con=engine, if_exists="append", index=False)

    logger.info("Insertando dim_albums...")
    df_albums.to_sql("dim_albums", con=engine, if_exists="append", index=False)

    logger.info("Insertando fact_tracks...")
    df_fact.to_sql("fact_tracks", con=engine, if_exists="append", index=False)

    logger.info("Carga a PostgreSQL completada.")
