# src/transform_integrated.py
"""
Transform de integración: une los tres datasets ya transformados
en un único dataset maestro de canciones, completando al máximo
la información de tracks, álbumes y artistas.

Prioridad de fuentes a nivel de TRACK (para completar campos):

    1) Spotify Tracks (Kaggle)            -> verdad principal para metadatos y audio features
    2) track_data_final (Spotify Global)  -> complementa con álbum y artista (album_*, artist_*)
    3) Spotify–YouTube                    -> añade URLs, métricas de YouTube, streams y
                                            audio features en tracks que solo estén aquí.

Además:

- Construye una “base de conocimiento” de artistas y álbumes:
    * Artistas: por nombre normalizado y por artist_id (real o sintético)
    * Álbumes: por (album_name_normalizado, owner) y por album_id
- Completa artist_id y album_id donde falten.
- Genera ids sintéticos cuando solo hay nombre:
    * synth:artist:<nombre_normalizado>
    * synth:album:<nombre_album_normalizado>::owner:<owner_id_o_nombre>
- Propaga información rica (popularidad, followers, géneros, urls, etc.) a
  todos los objetos artistas y álbumes que compartan id o nombre.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd

from config import (
    SPOTIFY_TRACKS_PROCESSED_JSON,
    SPOTIFY_YOUTUBE_PROCESSED_JSON,
    TRACK_DATA_FINAL_PROCESSED_JSON,
    SONGS_INTEGRATED_JSON,
)
from .utils_io import basic_profiling, write_json_with_logging

logger = logging.getLogger("transform_integrated")


# --------------------------------------------------------------------
# Helpers generales
# --------------------------------------------------------------------
def _normalize_name(name: Optional[str]) -> Optional[str]:
    """Normaliza nombres (artistas, álbumes): lower, strip, colapsar espacios."""
    if name is None:
        return None
    s = str(name).strip()
    if not s:
        return None
    # colapsar espacios múltiples
    s = " ".join(s.split()).lower()
    return s or None


def _merge_info(base: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge "suave": para cada clave, si base[k] es None / vacío y new[k] no lo es,
    actualiza base[k]. No pisa valores ya existentes.
    """
    for k, v in new.items():
        if v is None:
            continue
        if k not in base or base[k] in (None, "", [], {}):
            base[k] = v
    return base


# --------------------------------------------------------------------
# Fase 1: construir base de conocimiento de artistas y álbumes
# --------------------------------------------------------------------
def _build_knowledge(
    all_records: List[Dict[str, Any]],
):
    """
    Recorre todos los objetos {track, album, artists} de los tres datasets
    y construye estructuras globales para artistas y álbumes.
    """

    # Artistas
    artist_name_to_id: Dict[str, str] = {}          # nombre_norm -> artist_id
    artist_name_to_info: Dict[str, Dict[str, Any]] = {}  # nombre_norm -> info agregada
    artist_id_to_info: Dict[str, Dict[str, Any]] = {}     # artist_id -> info agregada
    artist_names_without_id: set[str] = set()

    # Álbumes
    # Clave de álbum: (nombre_album_norm, owner_norm_o_id)
    album_key_to_id: Dict[Tuple[Optional[str], Optional[str]], str] = {}
    album_key_to_info: Dict[Tuple[Optional[str], Optional[str]], Dict[str, Any]] = {}
    album_id_to_info: Dict[str, Dict[str, Any]] = {}
    album_keys_without_id: set[Tuple[Optional[str], Optional[str]]] = set()

    for rec in all_records:
        track = rec.get("track") or {}
        album = rec.get("album") or {}
        artists = rec.get("artists") or []

        # ----- ARTISTAS -----
        for artist in artists:
            name = artist.get("artist_name")
            norm = _normalize_name(name)
            aid = artist.get("artist_id")

            # Agregar info por nombre
            if norm:
                info_by_name = artist_name_to_info.setdefault(norm, {})
                _merge_info(info_by_name, artist)

            if aid:
                # Info por id
                info_by_id = artist_id_to_info.setdefault(aid, {})
                _merge_info(info_by_id, artist)

                # Mapear nombre -> id (primera vez que lo veamos)
                if norm and norm not in artist_name_to_id:
                    artist_name_to_id[norm] = aid
            else:
                # Sin id pero con nombre: candidato a id sintético
                if norm:
                    artist_names_without_id.add(norm)

        # ----- ÁLBUMES -----
        album_name = album.get("album_name")
        if album_name:
            album_name_norm = _normalize_name(album_name)

            owner_id = album.get("album_artist_owner_id")
            owner_norm = None

            if not owner_id and artists:
                owner_norm = _normalize_name(artists[0].get("artist_name"))

            key = (album_name_norm, owner_id or owner_norm)

            aid_album = album.get("album_id")

            # Agregar info por clave
            info_by_key = album_key_to_info.setdefault(key, {})
            _merge_info(info_by_key, album)

            if aid_album:
                info_by_id = album_id_to_info.setdefault(aid_album, {})
                _merge_info(info_by_id, album)
                if key not in album_key_to_id:
                    album_key_to_id[key] = aid_album
            else:
                album_keys_without_id.add(key)

    # ----------------------------------------------------------------
    # Fase 2: generar IDs sintéticos y consolidar información
    # ----------------------------------------------------------------

    # ARTISTAS: generar ids sintéticos para nombres sin id
    for norm in artist_names_without_id:
        if norm not in artist_name_to_id:
            synth_id = f"synth:artist:{norm}"
            artist_name_to_id[norm] = synth_id
            info_by_name = artist_name_to_info.get(norm, {})
            artist_id_to_info[synth_id] = dict(info_by_name) if info_by_name else {}

    # Fusionar info por nombre en info por id (real o sintético)
    for norm, aid in artist_name_to_id.items():
        info_name = artist_name_to_info.get(norm)
        if info_name:
            info_id = artist_id_to_info.setdefault(aid, {})
            _merge_info(info_id, info_name)

    # ÁLBUMES: generar ids sintéticos para claves sin id
    for key in album_keys_without_id:
        if key not in album_key_to_id:
            album_name_norm, owner_part = key
            owner_label = owner_part if owner_part else "none"
            synth_id = f"synth:album:{album_name_norm}::owner:{owner_label}"
            album_key_to_id[key] = synth_id
            info_by_key = album_key_to_info.get(key, {})
            album_id_to_info[synth_id] = dict(info_by_key) if info_by_key else {}

    # Fusionar info por clave en info por id (real o sintético)
    for key, aid in album_key_to_id.items():
        info_key = album_key_to_info.get(key)
        if info_key:
            info_id = album_id_to_info.setdefault(aid, {})
            _merge_info(info_id, info_key)

    return (
        artist_name_to_id,
        artist_id_to_info,
        album_key_to_id,
        album_id_to_info,
    )


# --------------------------------------------------------------------
# Fase 3: aplicar IDs y enriquecer cada objeto individual
# --------------------------------------------------------------------
def _apply_ids_and_enrich(
    records: List[Dict[str, Any]],
    artist_name_to_id: Dict[str, str],
    artist_id_to_info: Dict[str, Dict[str, Any]],
    album_key_to_id: Dict[Tuple[Optional[str], Optional[str]], str],
    album_id_to_info: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    A cada objeto {track, album, artists} le rellena artist_id / album_id
    y enriquece la info usando la base de conocimiento global.
    """
    enriched: List[Dict[str, Any]] = []

    for rec in records:
        track = rec.get("track") or {}
        album = rec.get("album") or {}
        artists = rec.get("artists") or []

        # ----- ARTISTAS -----
        for artist in artists:
            name = artist.get("artist_name")
            norm = _normalize_name(name)
            aid = artist.get("artist_id")

            if not aid and norm:
                aid = artist_name_to_id.get(norm)
                if not aid:
                    # Último recurso: generar uno en caliente (no debería pasar)
                    aid = f"synth:artist:{norm}"
                    artist_name_to_id[norm] = aid
                artist["artist_id"] = aid

            if aid:
                info = artist_id_to_info.get(aid)
                if info:
                    _merge_info(artist, info)

        # ----- ÁLBUM -----
        album_name = album.get("album_name")
        album_name_norm = _normalize_name(album_name) if album_name else None

        # Owner del álbum: si falta, usar primer artista (y su id si la tenemos)
        if not album.get("album_artist_owner_id"):
            if artists:
                owner_id_candidate = artists[0].get("artist_id")
                if owner_id_candidate:
                    album["album_artist_owner_id"] = owner_id_candidate

        owner_part = album.get("album_artist_owner_id")

        key = None
        if album_name_norm:
            key = (album_name_norm, owner_part)

        # Rellenar album_id a partir de la clave
        aid_album = album.get("album_id")
        if key and not aid_album:
            album_id_val = album_key_to_id.get(key)
            if not album_id_val:
                owner_label = owner_part if owner_part else "none"
                album_id_val = f"synth:album:{album_name_norm}::owner:{owner_label}"
                album_key_to_id[key] = album_id_val
            album["album_id"] = album_id_val
            aid_album = album_id_val

        # Enriquecer álbum a partir de album_id
        if aid_album:
            info_album = album_id_to_info.get(aid_album)
            if info_album:
                _merge_info(album, info_album)

        enriched.append(
            {
                "track": track,
                "album": album,
                "artists": artists,
            }
        )

    return enriched


# --------------------------------------------------------------------
# Fase 4: merge final por track_id con prioridad de datasets
# --------------------------------------------------------------------
def _merge_artist_lists(
    base_list: List[Dict[str, Any]],
    new_list: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Une dos listas de artistas, mergeando por artist_id si existe,
    y si no por nombre normalizado.
    """
    result_map: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def key_for(a: Dict[str, Any]) -> Tuple[str, str]:
        aid = a.get("artist_id")
        if aid:
            return ("id", str(aid))
        norm = _normalize_name(a.get("artist_name"))
        return ("name", norm or "")

    # Base primero (mayor prioridad)
    for a in base_list:
        k = key_for(a)
        result_map[k] = dict(a)

    # Nuevos artistas (completan)
    for a in new_list:
        k = key_for(a)
        if k in result_map:
            _merge_info(result_map[k], a)
        else:
            result_map[k] = dict(a)

    return list(result_map.values())


def _merge_nested(
    base: Dict[str, Any],
    new: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merge de dos objetos {track, album, artists} respetando prioridad
    de 'base' sobre 'new' (no se pisan valores ya existentes).
    """
    base_track = base.get("track") or {}
    base_album = base.get("album") or {}
    base_artists = base.get("artists") or []

    new_track = new.get("track") or {}
    new_album = new.get("album") or {}
    new_artists = new.get("artists") or []

    _merge_info(base_track, new_track)
    _merge_info(base_album, new_album)
    merged_artists = _merge_artist_lists(base_artists, new_artists)

    base["track"] = base_track
    base["album"] = base_album
    base["artists"] = merged_artists

    return base


# --------------------------------------------------------------------
# Función principal
# --------------------------------------------------------------------
def transform_integrated() -> None:
    """
    Ejecuta la integración de los tres datasets ya limpios y transformados
    (nested {track, album, artists}) en un único dataset maestro por track_id.
    """
    dataset_name = "Songs Integrated"

    logger.info("=== INICIO TRANSFORM INTEGRATED: %s ===", dataset_name)

    # 1. Cargar datasets procesados individuales (JSON-lines) como listas de dicts
    logger.info("Leyendo PROCESSED Spotify Tracks desde: %s", SPOTIFY_TRACKS_PROCESSED_JSON)
    df_sp = pd.read_json(SPOTIFY_TRACKS_PROCESSED_JSON, lines=True)
    sp_records: List[Dict[str, Any]] = df_sp.to_dict(orient="records")

    logger.info("Leyendo PROCESSED Spotify–YouTube desde: %s", SPOTIFY_YOUTUBE_PROCESSED_JSON)
    df_yt = pd.read_json(SPOTIFY_YOUTUBE_PROCESSED_JSON, lines=True)
    yt_records: List[Dict[str, Any]] = df_yt.to_dict(orient="records")

    logger.info("Leyendo PROCESSED track_data_final desde: %s", TRACK_DATA_FINAL_PROCESSED_JSON)
    df_global = pd.read_json(TRACK_DATA_FINAL_PROCESSED_JSON, lines=True)
    global_records: List[Dict[str, Any]] = df_global.to_dict(orient="records")

    # 2. Construir base de conocimiento global (artistas + álbumes)
    logger.info("Construyendo base de conocimiento global de artistas y álbumes...")
    all_records = sp_records + global_records + yt_records

    (
        artist_name_to_id,
        artist_id_to_info,
        album_key_to_id,
        album_id_to_info,
    ) = _build_knowledge(all_records)

    logger.info(
        "Base de conocimiento: %s artistas, %s álbumes.",
        len(artist_name_to_id),
        len(album_key_to_id),
    )

    # 3. Aplicar IDs y enriquecer cada dataset individual
    logger.info("Aplicando IDs y enriqueciendo Spotify Tracks...")
    sp_enriched = _apply_ids_and_enrich(
        sp_records,
        artist_name_to_id,
        artist_id_to_info,
        album_key_to_id,
        album_id_to_info,
    )

    logger.info("Aplicando IDs y enriqueciendo track_data_final...")
    global_enriched = _apply_ids_and_enrich(
        global_records,
        artist_name_to_id,
        artist_id_to_info,
        album_key_to_id,
        album_id_to_info,
    )

    logger.info("Aplicando IDs y enriqueciendo Spotify–YouTube...")
    yt_enriched = _apply_ids_and_enrich(
        yt_records,
        artist_name_to_id,
        artist_id_to_info,
        album_key_to_id,
        album_id_to_info,
    )

    # 4. Merge final por track_id con prioridad:
    #    1) Spotify Tracks (Kaggle)
    #    2) track_data_final (Global)
    #    3) Spotify–YouTube
    logger.info("Integrando registros por track_id con prioridad de datasets...")
    integrated: Dict[str, Dict[str, Any]] = {}

    def _integrate(records: List[Dict[str, Any]]):
        for rec in records:
            track = rec.get("track") or {}
            tid = track.get("track_id")
            if not tid:
                continue
            tid = str(tid)
            if tid not in integrated:
                # Clonar la estructura
                integrated[tid] = {
                    "track": dict(track),
                    "album": dict(rec.get("album") or {}),
                    "artists": [dict(a) for a in (rec.get("artists") or [])],
                }
            else:
                integrated[tid] = _merge_nested(integrated[tid], rec)

    # Orden según prioridad
    _integrate(sp_enriched)
    _integrate(global_enriched)
    _integrate(yt_enriched)

    logger.info("Tracks integrados: %s", len(integrated))

    # 5. Preparar salida: una fila por track_id con forma:
    #    { "track_id": ..., "track": {...}, "album": {...}, "artists": [...] }
    output_records: List[Dict[str, Any]] = []
    for tid, obj in integrated.items():
        record = {"track_id": tid}
        record.update(obj)
        output_records.append(record)

    df_out = pd.DataFrame(output_records)

    basic_profiling(df_out, dataset_name)
    write_json_with_logging(df_out, SONGS_INTEGRATED_JSON, dataset_name)

    logger.info("=== FIN TRANSFORM INTEGRATED: %s ===", dataset_name)
