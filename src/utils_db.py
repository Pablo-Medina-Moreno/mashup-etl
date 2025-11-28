# src/utils_db.py

from __future__ import annotations

import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from config import DB_URI

logger = logging.getLogger(__name__)


def get_postgres_engine() -> Engine:
    """Crea conexión usando DB_URI del config."""
    safe_uri = DB_URI.replace(DB_URI.split(':')[2].split('@')[0], "*****")
    logger.info(f"Usando DB_URI: {safe_uri}")

    return create_engine(DB_URI, echo=False, future=True)


def drop_all_tables(engine: Engine) -> None:
    """
    Elimina TODAS las tablas del schema 'public' del PostgreSQL.
    Equivalente a reiniciar la base de datos sin borrarla.
    """
    logger.warning("⚠️ Eliminando TODAS las tablas del esquema public…")

    get_tables = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
    """)

    with engine.begin() as conn:
        result = conn.execute(get_tables)
        tables = [row[0] for row in result]

        if not tables:
            logger.info("No hay tablas para eliminar.")
            return

        # Drop en cascada
        for table in tables:
            logger.warning(f"DROP TABLE {table} CASCADE;")
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))

    logger.warning("🔥 Todas las tablas eliminadas correctamente.")


def create_star_schema_tables(engine: Engine) -> None:
    """Crea las tablas dim_artists, dim_albums y fact_tracks."""
    logger.info("Creando tablas del modelo en estrella…")

    create_dim_artists = text("""
        CREATE TABLE IF NOT EXISTS dim_artists (
            artist_name TEXT PRIMARY KEY,
            artist_popularity NUMERIC,
            artist_followers NUMERIC,
            artist_genres TEXT
        );
    """)

    create_dim_albums = text("""
        CREATE TABLE IF NOT EXISTS dim_albums (
            album_id TEXT PRIMARY KEY,
            album_name TEXT NOT NULL,
            album_type TEXT,
            album_release_date DATE,
            album_total_tracks INTEGER
        );
    """)

    create_fact_tracks = text("""
        CREATE TABLE IF NOT EXISTS fact_tracks (
            track_id TEXT PRIMARY KEY,
            artist_name TEXT REFERENCES dim_artists(artist_name),
            album_id TEXT REFERENCES dim_albums(album_id),

            track_name   TEXT NOT NULL,
            track_genre  TEXT,
            track_number INTEGER,

            spotify_popularity NUMERIC,
            track_popularity   NUMERIC,
            popularity         NUMERIC,

            danceability NUMERIC,
            energy       NUMERIC,
            key          INTEGER,
            loudness     NUMERIC,
            mode         INTEGER,
            liveness     NUMERIC,
            valence      NUMERIC,
            tempo        NUMERIC,

            spotify_url    TEXT,
            youtube_url    TEXT,
            youtube_title  TEXT,
            youtube_channel TEXT
        );
    """)

    with engine.begin() as conn:
        conn.execute(create_dim_artists)
        conn.execute(create_dim_albums)
        conn.execute(create_fact_tracks)

    logger.info("Modelo en estrella creado.")



def truncate_star_schema_tables(engine: Engine) -> None:
    """
    Elimina el contenido de las tablas del esquema en estrella.
    """
    logger.info("Truncando tablas fact_tracks, dim_artists y dim_albums...")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_tracks RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_artists RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_albums RESTART IDENTITY CASCADE;"))

    logger.info("Truncado completado.")
