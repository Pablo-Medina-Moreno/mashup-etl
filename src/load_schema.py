# src/load_schema.py
"""
Lógica de carga (LOAD) del modelo relacional musical en PostgreSQL
a partir de songs_integrated.json.

Tablas:

    - artists
    - artist_genres
    - albums
    - tracks
    - track_artists
"""

from __future__ import annotations

import logging
from typing import List, Dict, Any

import pandas as pd
from sqlalchemy.engine import Engine

from config import SONGS_INTEGRATED_JSON
from .utils_db import create_music_schema_tables, truncate_music_schema_tables

logger = logging.getLogger(__name__)


# ==========================
#  HELPERS
# ==========================


def _ensure_list(value) -> List[str]:
    """Convierte un valor a lista de strings limpia, si es posible."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []

    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]

    text = str(value).strip()
    if not text:
        return []

    if "," in text:
        return [t.strip() for t in text.split(",") if t.strip()]

    return [text]


def _coerce_bool(series: pd.Series) -> pd.Series:
    """
    Convierte una serie a booleano de forma robusta:
        - 1, 1.0, '1', 'true', 'True', 't', 'yes', 'y'  -> True
        - 0, 0.0, '0', 'false', 'False', 'f', 'no', 'n' -> False
        - cualquier otra cosa -> NaN (se enviará como NULL a la BD)
    """
    if series.dtype == bool:
        return series

    s = series.copy()
    s_str = s.astype(str).str.strip().str.lower()

    true_vals = {"1", "true", "t", "yes", "y"}
    false_vals = {"0", "false", "f", "no", "n"}

    result = pd.Series(pd.NA, index=s.index, dtype="boolean")
    result[s_str.isin(true_vals)] = True
    result[s_str.isin(false_vals)] = False

    return result


def _clean_nullable_str(series: pd.Series) -> pd.Series:
    """
    Limpia una serie de strings:
      - strip
      - convierte "", "None", "none", "NaN", "nan", "<NA>" en NaN
    """
    s = series.astype("string")
    s = s.str.strip()
    mask_bad = s.isin(["", "None", "none", "NaN", "nan", "<NA>"])
    s = s.mask(mask_bad)
    return s


def _first_not_na(s: pd.Series) -> Any:
    """Agregador: devuelve el primer valor no nulo de la serie."""
    non_na = s.dropna()
    return non_na.iloc[0] if not non_na.empty else pd.NA


# ==========================
#  BUILDERS DE DATAFRAMES
# ==========================


def build_artists(df_artists: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla artists usando artist_id como PRIMARY KEY.

    df_artists: dataframe con columnas de artistas (una fila por artista)
                proviniente del campo "artists" de songs_integrated.
    """
    cols = [
        "artist_id",
        "artist_name",
        "artist_spotify_url",
        "artist_popularity",
        "artist_followers",
    ]

    for col in cols:
        if col not in df_artists.columns:
            df_artists[col] = pd.NA

    df_art = df_artists[cols].dropna(subset=["artist_id"]).copy()

    # Limpiamos strings
    df_art["artist_id"] = _clean_nullable_str(df_art["artist_id"])
    df_art["artist_name"] = _clean_nullable_str(df_art["artist_name"])
    df_art["artist_spotify_url"] = _clean_nullable_str(df_art["artist_spotify_url"])

    df_art["artist_popularity"] = pd.to_numeric(
        df_art["artist_popularity"], errors="coerce"
    )
    df_art["artist_followers"] = pd.to_numeric(
        df_art["artist_followers"], errors="coerce"
    )

    # Agregamos por artist_id
    agg = (
        df_art.groupby("artist_id", as_index=False)
        .agg(
            {
                "artist_name": _first_not_na,
                "artist_spotify_url": _first_not_na,
                "artist_popularity": "max",
                "artist_followers": "max",
            }
        )
        .reset_index(drop=True)
    )

    # Normalización final
    agg["artist_id"] = agg["artist_id"].astype("string").str.strip()
    agg["artist_name"] = _clean_nullable_str(agg["artist_name"])
    agg["artist_spotify_url"] = _clean_nullable_str(agg["artist_spotify_url"])

    # Asegurar que artist_name nunca es NULL (NOT NULL en la tabla)
    mask_no_name = agg["artist_name"].isna()
    if mask_no_name.any():
        logger.info(
            "artists: %s artistas sin nombre; usando artist_id como nombre.",
            mask_no_name.sum(),
        )
        agg.loc[mask_no_name, "artist_name"] = agg.loc[mask_no_name, "artist_id"]

    logger.info("artists: %s filas", agg.shape[0])
    return agg


def build_artist_genres(df_artists: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla artist_genres (1:N) a partir de df_artists:

        - artist_id
        - genre
    """
    if "artist_id" not in df_artists.columns or "artist_genres" not in df_artists.columns:
        logger.info("No se pueden construir artist_genres: faltan columnas.")
        return pd.DataFrame(columns=["artist_id", "genre"])

    tmp = df_artists[["artist_id", "artist_genres"]].copy()
    tmp = tmp.dropna(subset=["artist_id", "artist_genres"])

    tmp["artist_genres"] = tmp["artist_genres"].apply(_ensure_list)

    tmp = tmp.explode("artist_genres")
    tmp = tmp.rename(columns={"artist_genres": "genre"})
    tmp = tmp.dropna(subset=["genre"])

    tmp["artist_id"] = tmp["artist_id"].astype(str).str.strip()
    tmp["genre"] = tmp["genre"].astype(str).str.strip()

    df_genres = tmp.drop_duplicates(subset=["artist_id", "genre"]).reset_index(drop=True)

    logger.info("artist_genres: %s filas", df_genres.shape[0])
    return df_genres


def build_albums(df_albums: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla albums:

        - album_id (PK)
        - artist_id (FK hacia artists)  <- viene de album_artist_owner_id
        - album_name
        - album_release_date
        - album_total_tracks
        - album_type
        - album_spotify_url
    """
    # Asegurarnos de que tenemos artist_id (owner) con el nombre correcto
    if "artist_id" not in df_albums.columns and "album_artist_owner_id" in df_albums.columns:
        df_albums = df_albums.rename(columns={"album_artist_owner_id": "artist_id"})

    cols = [
        "album_id",
        "artist_id",
        "album_name",
        "album_release_date",
        "album_total_tracks",
        "album_type",
        "album_spotify_url",
    ]

    for col in cols:
        if col not in df_albums.columns:
            df_albums[col] = pd.NA

    df_alb = df_albums[cols].copy()

    for col in ["album_id", "artist_id", "album_name", "album_type", "album_spotify_url"]:
        df_alb[col] = _clean_nullable_str(df_alb[col])

    # Parseo de fecha
    df_alb["album_release_date"] = pd.to_datetime(
        df_alb["album_release_date"], errors="coerce"
    ).dt.date

    df_alb["album_total_tracks"] = pd.to_numeric(
        df_alb["album_total_tracks"], errors="coerce"
    )

    # Exigimos album_id y artist_id no nulos para respetar PK/FK
    df_alb = df_alb.dropna(subset=["album_id", "artist_id"])

    # Rellenamos album_name nulo con placeholder para cumplir NOT NULL
    df_alb.loc[df_alb["album_name"].isna(), "album_name"] = "Unknown Album"

    df_alb = df_alb.drop_duplicates(subset=["album_id"]).reset_index(drop=True)

    logger.info("albums: %s filas", df_alb.shape[0])
    return df_alb


def build_tracks(df_tracks: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla tracks.

    df_tracks: dataframe con columnas provenientes de "track" + "album_id":

        - track_id (PK)
        - album_id (FK)
        - track_name
        - track_duration_ms
        - track_explicit
        - track_number
        - track_genre
        - track_spotify_popularity
        - track_popularity
        - track_spotify_streams
        - audio features (track_*)
        - YouTube (track_youtube_*)
        - track_spotify_url
    """
    cols = [
        "track_id",
        "album_id",
        "track_name",
        "track_duration_ms",
        "track_explicit",
        "track_number",
        "track_genre",
        "track_spotify_popularity",
        "track_popularity",
        "track_spotify_streams",
        "track_danceability",
        "track_energy",
        "track_key",
        "track_loudness",
        "track_mode",
        "track_speechiness",
        "track_acousticness",
        "track_instrumentalness",
        "track_liveness",
        "track_valence",
        "track_tempo",
        "track_time_signature",
        "track_youtube_url",
        "track_youtube_title",
        "track_youtube_channel",
        "track_youtube_views",
        "track_youtube_likes",
        "track_youtube_comments",
        "track_youtube_description",
        "track_youtube_licensed",
        "track_youtube_official_video",
        "track_spotify_url",
    ]

    for col in cols:
        if col not in df_tracks.columns:
            df_tracks[col] = pd.NA

    df_tr = df_tracks[cols].copy()

    # Limpiamos IDs
    df_tr["track_id"] = df_tr["track_id"].astype("string").str.strip()
    bad_ids = df_tr["track_id"].isna() | df_tr["track_id"].isin(["", "<NA>"])
    if bad_ids.any():
        df_tr = df_tr[~bad_ids].copy()

    # Limpieza de strings (excepto track_name que tratamos aparte)
    for col in [
        "album_id",
        "track_genre",
        "track_youtube_url",
        "track_youtube_title",
        "track_youtube_channel",
        "track_youtube_description",
        "track_spotify_url",
    ]:
        if col in df_tr.columns:
            df_tr[col] = _clean_nullable_str(df_tr[col])

    # track_name: puede venir vacío → lo limpiamos y rellenamos
    df_tr["track_name"] = _clean_nullable_str(df_tr["track_name"])
    mask_no_name = df_tr["track_name"].isna()
    if mask_no_name.any():
        logger.info(
            "tracks: %s filas sin nombre; usando track_id como nombre.",
            mask_no_name.sum(),
        )
        df_tr.loc[mask_no_name, "track_name"] = df_tr.loc[mask_no_name, "track_id"]

    # Booleans
    df_tr["track_explicit"] = _coerce_bool(df_tr["track_explicit"])
    df_tr["track_youtube_licensed"] = _coerce_bool(df_tr["track_youtube_licensed"])
    df_tr["track_youtube_official_video"] = _coerce_bool(
        df_tr["track_youtube_official_video"]
    )

    # Numéricos
    numeric_cols = [
        "track_duration_ms",
        "track_number",
        "track_spotify_popularity",
        "track_popularity",
        "track_spotify_streams",
        "track_danceability",
        "track_energy",
        "track_key",
        "track_loudness",
        "track_mode",
        "track_speechiness",
        "track_acousticness",
        "track_instrumentalness",
        "track_liveness",
        "track_valence",
        "track_tempo",
        "track_time_signature",
        "track_youtube_views",
        "track_youtube_likes",
        "track_youtube_comments",
    ]
    for col in numeric_cols:
        if col in df_tr.columns:
            df_tr[col] = pd.to_numeric(df_tr[col], errors="coerce")

    # Generar track_spotify_url si está vacío o es nulo
    mask_empty_url = df_tr["track_spotify_url"].isna() | (
        df_tr["track_spotify_url"].astype(str).isin(
            ["", "<NA>", "nan", "NaN", "None", "none"]
        )
    )
    df_tr.loc[mask_empty_url, "track_spotify_url"] = (
        "https://open.spotify.com/track/"
        + df_tr.loc[mask_empty_url, "track_id"].astype(str)
    )

    # Quitamos duplicados por track_id
    df_tr = df_tr.drop_duplicates(subset=["track_id"]).reset_index(drop=True)

    logger.info("tracks: %s filas", df_tr.shape[0])
    return df_tr


def build_track_artists(
    df_pairs: pd.DataFrame,
    df_tracks: pd.DataFrame,
    df_artists: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construye tabla track_artists (N:N) a partir de:

        df_pairs:    columnas ["track_id", "artist_id"] (una fila por relación)
        df_tracks:   para validar track_id existentes
        df_artists:  para validar artist_id existentes
    """
    if df_pairs.empty:
        logger.info("track_artists: no se generó ninguna relación (df_pairs vacío).")
        return pd.DataFrame(columns=["track_id", "artist_id"])

    df_ta = df_pairs[["track_id", "artist_id"]].copy()
    df_ta["track_id"] = _clean_nullable_str(df_ta["track_id"])
    df_ta["artist_id"] = _clean_nullable_str(df_ta["artist_id"])

    df_ta = df_ta.dropna(subset=["track_id", "artist_id"])

    # Conjuntos válidos de tracks y artists para respetar FKs
    valid_tracks = (
        df_tracks[["track_id"]]
        .dropna(subset=["track_id"])
        .assign(track_id=lambda d: d["track_id"].astype(str).str.strip())["track_id"]
        .unique()
    )
    valid_tracks_set = set(valid_tracks)

    valid_artists = (
        df_artists[["artist_id"]]
        .dropna(subset=["artist_id"])
        .assign(artist_id=lambda d: d["artist_id"].astype(str).str.strip())["artist_id"]
        .unique()
    )
    valid_artists_set = set(valid_artists)

    before = df_ta.shape[0]

    df_ta = (
        df_ta[
            df_ta["track_id"].isin(valid_tracks_set)
            & df_ta["artist_id"].isin(valid_artists_set)
        ]
        .drop_duplicates(subset=["track_id", "artist_id"])
        .reset_index(drop=True)
    )

    after = df_ta.shape[0]
    logger.info(
        "track_artists: %s filas (filtradas %s por FK inválida)",
        after,
        before - after,
    )

    return df_ta


# ==========================
#  LOAD ORQUESTADOR
# ==========================


def load_schema(engine: Engine) -> None:
    """
    Orquesta el proceso de LOAD:

        1. Leer songs_integrated.json (JSON-lines)
        2. Desanidar track/album/artists en dataframes planos
        3. Crear tablas del modelo musical (si no existen)
        4. Truncar las tablas (recarga completa)
        5. Construir DataFrames para cada tabla
        6. Insertar en PostgreSQL en el orden correcto
    """
    logger.info("Leyendo songs_integrated desde: %s", SONGS_INTEGRATED_JSON)
    df_raw = pd.read_json(SONGS_INTEGRATED_JSON, lines=True)

    # ------------------------------------------------------------------
    # 1) Desanidar: construir dfs planos a partir de {track, album, artists}
    # ------------------------------------------------------------------
    records: List[Dict[str, Any]] = df_raw.to_dict(orient="records")

    track_rows: List[Dict[str, Any]] = []
    album_rows: List[Dict[str, Any]] = []
    artist_rows: List[Dict[str, Any]] = []
    track_artist_pairs: List[Dict[str, str]] = []

    for rec in records:
        tid = rec.get("track_id")
        track = rec.get("track") or {}
        album = rec.get("album") or {}
        artists = rec.get("artists") or []

        # Aseguramos que track lleva su track_id y album_id
        track_row = dict(track)
        track_row["track_id"] = tid
        track_row["album_id"] = album.get("album_id")
        track_rows.append(track_row)

        # ÁLBUM
        album_row = dict(album)
        album_rows.append(album_row)

        # ARTISTAS
        for art in artists:
            artist_rows.append(dict(art))
            aid = art.get("artist_id")
            if tid and aid:
                track_artist_pairs.append(
                    {"track_id": str(tid), "artist_id": str(aid)}
                )

    df_tracks_flat = pd.DataFrame(track_rows)
    df_albums_flat = pd.DataFrame(album_rows)
    df_artists_flat = pd.DataFrame(artist_rows)
    df_track_artists_flat = pd.DataFrame(track_artist_pairs)

    logger.info(
        "Desanidado: %s tracks, %s albums, %s artists, %s track-artist pairs",
        df_tracks_flat.shape[0],
        df_albums_flat.shape[0],
        df_artists_flat.shape[0],
        df_track_artists_flat.shape[0],
    )

    # 2) Crear tablas si no existen
    create_music_schema_tables(engine)

    # 3) Truncar contenido (recarga completa)
    truncate_music_schema_tables(engine)

    # 4) Construir dataframes para cada tabla
    df_artists = build_artists(df_artists_flat)
    df_artist_genres = build_artist_genres(df_artists_flat)
    df_albums = build_albums(df_albums_flat)
    df_tracks = build_tracks(df_tracks_flat)
    df_track_artists = build_track_artists(df_track_artists_flat, df_tracks, df_artists)

    # Alinear album_id de tracks con albums para no violar la FK
    if not df_albums.empty and "album_id" in df_tracks.columns:
        valid_album_ids = (
            df_albums["album_id"]
            .astype(str)
            .str.strip()
            .unique()
        )
        valid_album_ids_set = set(valid_album_ids)

        df_tracks["album_id"] = df_tracks["album_id"].astype("string").str.strip()

        mask_invalid = ~df_tracks["album_id"].isin(valid_album_ids_set)
        mask_invalid |= df_tracks["album_id"].isin(
            ["", "None", "none", "NaN", "nan", "<NA>"]
        )

        if mask_invalid.any():
            logger.info(
                "tracks: %s filas con album_id sin álbum válido; se ponen a NULL.",
                mask_invalid.sum(),
            )
            df_tracks.loc[mask_invalid, "album_id"] = pd.NA

    # 5) Cargar a PostgreSQL usando pandas.to_sql en orden de FKs
    logger.info("Insertando artists...")
    if not df_artists.empty:
        df_artists.to_sql("artists", con=engine, if_exists="append", index=False)

    logger.info("Insertando artist_genres...")
    if not df_artist_genres.empty:
        df_artist_genres.to_sql("artist_genres", con=engine, if_exists="append", index=False)

    logger.info("Insertando albums...")
    if not df_albums.empty:
        df_albums.to_sql("albums", con=engine, if_exists="append", index=False)

    logger.info("Insertando tracks...")
    if not df_tracks.empty:
        df_tracks.to_sql("tracks", con=engine, if_exists="append", index=False)

    logger.info("Insertando track_artists...")
    if not df_track_artists.empty:
        df_track_artists.to_sql("track_artists", con=engine, if_exists="append", index=False)

    logger.info("Carga del modelo musical completada.")
