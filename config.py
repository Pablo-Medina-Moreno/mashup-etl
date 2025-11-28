# src/config.py
"""
Archivo de configuración GLOBAL del proyecto.

Centraliza TODAS las rutas del ETL (Extract, Transform, Load)
y las variables de conexión a base de datos obtenidas desde `.env`.
"""

from pathlib import Path
from dotenv import load_dotenv
import os

# Cargar variables desde .env
load_dotenv()

# ==========================
#  VARIABLES DE BASE DE DATOS (LOAD)
# ==========================

DB_URI = os.getenv("DB_URI")

if DB_URI is None:
    raise RuntimeError("DB_URI no está definido en el entorno .env")


# ==========================
#  RUTA RAÍZ DEL PROYECTO
# ==========================

PROJECT_ROOT = Path(__file__).resolve().parent


# ==========================
#  DIRECTORIOS DE DATOS
# ==========================

DATA_DIR = PROJECT_ROOT / "data"

INPUT_DIR = DATA_DIR / "input"          # datos CSV originales
RAW_DIR = DATA_DIR / "raw"              # salida EXTRACT en JSON
PROCESSED_DIR = DATA_DIR / "processed"  # salida TRANSFORM en JSON


# ==========================
#  FICHEROS DE ENTRADA (CSV)
# ==========================

SPOTIFY_TRACKS_CSV_NAME = "spotify_tracks.csv"
SPOTIFY_TRACKS_CSV_PATH = INPUT_DIR / SPOTIFY_TRACKS_CSV_NAME

SPOTIFY_YOUTUBE_CSV_NAME = "spotify_youtube_dataset.csv"
SPOTIFY_YOUTUBE_CSV_PATH = INPUT_DIR / SPOTIFY_YOUTUBE_CSV_NAME

TRACK_DATA_FINAL_CSV_NAME = "track_data_final.csv"
TRACK_DATA_FINAL_CSV_PATH = INPUT_DIR / TRACK_DATA_FINAL_CSV_NAME


# ==========================
#  SALIDA EXTRACT (RAW JSON)
# ==========================

SPOTIFY_TRACKS_RAW_JSON = RAW_DIR / "spotify_tracks_raw.json"
SPOTIFY_YOUTUBE_RAW_JSON = RAW_DIR / "spotify_youtube_raw.json"
TRACK_DATA_FINAL_RAW_JSON = RAW_DIR / "track_data_final_raw.json"


# ==========================
#  SALIDA TRANSFORM INDIVIDUAL (JSON)
# ==========================

SPOTIFY_TRACKS_PROCESSED_JSON = PROCESSED_DIR / "spotify_tracks_clean.json"
SPOTIFY_YOUTUBE_PROCESSED_JSON = PROCESSED_DIR / "spotify_youtube_clean.json"
TRACK_DATA_FINAL_PROCESSED_JSON = PROCESSED_DIR / "track_data_final_clean.json"


# ==========================
#  SALIDA TRANSFORM INTEGRADO (JSON)
# ==========================

SONGS_INTEGRATED_JSON = PROCESSED_DIR / "songs_integrated.json"
