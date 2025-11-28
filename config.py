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

# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = int(os.getenv("DB_PORT", 5432))
# DB_NAME = os.getenv("DB_NAME")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")

DB_URI = os.getenv("DB_URI")

if DB_URI is None:
    raise RuntimeError(
        "DB_URI no está definido en el entorno .env"
    )

from pathlib import Path

# ==========================
#  RUTA RAÍZ DEL PROYECTO
# ==========================

PROJECT_ROOT = Path(__file__).resolve().parent


# ==========================
#  DIRECTORIOS DE DATOS
# ==========================

# Directorio principal de datos
DATA_DIR = PROJECT_ROOT / "data"

# Subcarpetas del pipeline
INPUT_DIR = DATA_DIR / "input"          # datos originales descargados (CSV)
RAW_DIR = DATA_DIR / "raw"              # salida de EXTRACT (Parquet, lo más fiel posible al origen)
PROCESSED_DIR = DATA_DIR / "processed"  # salida de TRANSFORM (Parquet, ya limpio / modelado)


# ==========================
#  FICHEROS DE ENTRADA (CSV)
# ==========================
# Ajustados a tus 3 fuentes de Kaggle

# 1) Spotify Tracks Dataset
SPOTIFY_TRACKS_CSV_NAME = "spotify_tracks.csv"         
SPOTIFY_TRACKS_CSV_PATH = INPUT_DIR / SPOTIFY_TRACKS_CSV_NAME

# 2) Spotify–YouTube Dataset
SPOTIFY_YOUTUBE_CSV_NAME = "spotify_youtube_dataset.csv"
SPOTIFY_YOUTUBE_CSV_PATH = INPUT_DIR / SPOTIFY_YOUTUBE_CSV_NAME

# 3) Spotify Global Music (track_data_final.csv)
TRACK_DATA_FINAL_CSV_NAME = "track_data_final.csv"
TRACK_DATA_FINAL_CSV_PATH = INPUT_DIR / TRACK_DATA_FINAL_CSV_NAME


# ==========================
#  SALIDA EXTRACT (RAW)
# ==========================
# Mismo contenido lógico que el CSV, pero en formato columnar (Parquet)

SPOTIFY_TRACKS_RAW_PARQUET = RAW_DIR / "spotify_tracks_raw.parquet"
SPOTIFY_YOUTUBE_RAW_PARQUET = RAW_DIR / "spotify_youtube_raw.parquet"
TRACK_DATA_FINAL_RAW_PARQUET = RAW_DIR / "track_data_final_raw.parquet"


# ==========================
#  SALIDA TRANSFORM INDIVIDUAL
# ==========================
# Cada dataset ya limpio / normalizado individualmente

SPOTIFY_TRACKS_PROCESSED_PARQUET = PROCESSED_DIR / "spotify_tracks_clean.parquet"
SPOTIFY_YOUTUBE_PROCESSED_PARQUET = PROCESSED_DIR / "spotify_youtube_clean.parquet"
TRACK_DATA_FINAL_PROCESSED_PARQUET = PROCESSED_DIR / "track_data_final_clean.parquet"


# ==========================
#  SALIDA TRANSFORM INTEGRADO
# ==========================
# Dataset maestro con todos los datos integrados por track_id

SONGS_INTEGRATED_PARQUET = PROCESSED_DIR / "songs_integrated.parquet"
