from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import DB_URI

logger = logging.getLogger(__name__)


def get_postgres_engine() -> Engine:
    """Crea conexión usando DB_URI del config."""
    try:
        # ofuscamos solo la parte de password para el log
        parts = DB_URI.split("@")
        left = parts[0]
        right = "@".join(parts[1:])

        # left: postgresql+psycopg2://user:password
        creds_parts = left.split(":")
        if len(creds_parts) >= 3:
            creds_parts[2] = "*****"
            safe_left = ":".join(creds_parts)
        else:
            safe_left = left

        safe_uri = safe_left + "@" + right
    except Exception:
        safe_uri = "<oculto>"

    logger.info(f"Usando DB_URI: {safe_uri}")

    return create_engine(DB_URI, echo=False, future=True)


def drop_all_tables(engine: Engine) -> None:
    """
    Elimina TODAS las tablas del schema 'public' del PostgreSQL.
    Equivalente a reiniciar la base de datos sin borrarla.
    """
    logger.warning("⚠️ Eliminando TODAS las tablas del esquema public…")

    get_tables = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE';
        """
    )

    with engine.begin() as conn:
        result = conn.execute(get_tables)
        tables = [row[0] for row in result]

        if not tables:
            logger.info("No hay tablas para eliminar.")
            return

        for table in tables:
            logger.warning(f"DROP TABLE {table} CASCADE;")
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))

    logger.warning("🔥 Todas las tablas eliminadas correctamente.")


def create_music_schema_tables(engine: Engine) -> None:
    """
    Crea las tablas del modelo relacional musical:

        - artists
        - artist_genres
        - albums
        - tracks
        - track_artists
    """
    logger.info("Creando tablas del modelo musical…")

    create_artists = text(
        """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id TEXT PRIMARY KEY,
            artist_name TEXT NOT NULL,
            artist_spotify_url TEXT,
            artist_popularity NUMERIC,
            artist_followers NUMERIC
        );
        """
    )

    create_artist_genres = text(
        """
        CREATE TABLE IF NOT EXISTS artist_genres (
            artist_id TEXT NOT NULL REFERENCES artists(artist_id) ON DELETE CASCADE,
            genre     TEXT NOT NULL,
            PRIMARY KEY (artist_id, genre)
        );
        """
    )

    create_albums = text(
        """
        CREATE TABLE IF NOT EXISTS albums (
            album_id           TEXT PRIMARY KEY,
            artist_id          TEXT NOT NULL REFERENCES artists(artist_id) ON DELETE CASCADE,
            album_name         TEXT NOT NULL,
            album_release_date DATE,
            album_total_tracks INTEGER,
            album_type         TEXT,
            album_spotify_url  TEXT
        );
        """
    )

    # Todas las columnas de canción llevan prefijo track_
    create_tracks = text(
        """
        CREATE TABLE IF NOT EXISTS tracks (
            track_id   TEXT PRIMARY KEY,
            album_id   TEXT REFERENCES albums(album_id) ON DELETE SET NULL,

            track_name        TEXT NOT NULL,
            track_duration_ms INTEGER,
            track_explicit    BOOLEAN,
            track_number      INTEGER,
            track_genre       TEXT,

            track_spotify_popularity NUMERIC,
            track_popularity         NUMERIC,
            track_spotify_streams    NUMERIC,

            track_danceability     NUMERIC,
            track_energy           NUMERIC,
            track_key              INTEGER,
            track_loudness         NUMERIC,
            track_mode             INTEGER,
            track_speechiness      NUMERIC,
            track_acousticness     NUMERIC,
            track_instrumentalness NUMERIC,
            track_liveness         NUMERIC,
            track_valence          NUMERIC,
            track_tempo            NUMERIC,
            track_time_signature   INTEGER,

            track_youtube_url           TEXT,
            track_youtube_title         TEXT,
            track_youtube_channel       TEXT,
            track_youtube_views         NUMERIC,
            track_youtube_likes         NUMERIC,
            track_youtube_comments      NUMERIC,
            track_youtube_description   TEXT,
            track_youtube_licensed      BOOLEAN,
            track_youtube_official_video BOOLEAN,

            track_spotify_url TEXT
        );
        """
    )

    create_track_artists = text(
        """
        CREATE TABLE IF NOT EXISTS track_artists (
            track_id  TEXT NOT NULL REFERENCES tracks(track_id) ON DELETE CASCADE,
            artist_id TEXT NOT NULL REFERENCES artists(artist_id) ON DELETE CASCADE,
            PRIMARY KEY (track_id, artist_id)
        );
        """
    )

    with engine.begin() as conn:
        conn.execute(create_artists)
        conn.execute(create_artist_genres)
        conn.execute(create_albums)
        conn.execute(create_tracks)
        conn.execute(create_track_artists)

    logger.info("Tablas del modelo musical creadas.")


def truncate_music_schema_tables(engine: Engine) -> None:
    """
    TRUNCATE ordenado de todas las tablas del modelo musical.
    Útil si quieres recargar sin hacer drop_all_tables.
    """
    logger.info("Truncando tablas del modelo musical...")

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                TRUNCATE TABLE
                    track_artists,
                    artist_genres,
                    tracks,
                    albums,
                    artists
                RESTART IDENTITY CASCADE;
                """
            )
        )

    logger.info("Truncado de tablas musicales completado.")
