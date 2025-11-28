# src/utils_io.py
"""
Utilidades de logging, I/O y helpers genéricos para el ETL.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config import INPUT_DIR, RAW_DIR

# ==========================
#  LOGGING
# ==========================


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configura el logger raíz para el proyecto.

    Se llama una vez al inicio del script principal
    (main_extract.py o main_transform.py).
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


# ==========================
#  DIRECTORIOS
# ==========================


def ensure_directories() -> None:
    """
    Asegura que las carpetas necesarias existan.

    Por ahora: data/input y data/raw.
    """
    logger = logging.getLogger("utils_io.ensure_directories")

    for directory in [INPUT_DIR, RAW_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info("Directorio asegurado: %s", directory)


# ==========================
#  NORMALIZACIÓN Y PROFILING
# ==========================


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los nombres de columnas a snake_case, minúsculas,
    sin espacios ni caracteres raros.
    """
    def _normalize(col: str) -> str:
        col = col.strip()
        col = col.replace(" ", "_")
        col = col.replace("-", "_")
        col = col.replace("/", "_")
        return col.lower()

    return df.rename(columns={c: _normalize(c) for c in df.columns})


def basic_profiling(df: pd.DataFrame, dataset_name: str) -> None:
    """
    Saca por log un pequeño profiling del DataFrame para trazabilidad.
    """
    logger = logging.getLogger(f"profiling.{dataset_name}")

    logger.info("=== Profiling básico para %s ===", dataset_name)
    logger.info("Filas: %s, Columnas: %s", df.shape[0], df.shape[1])
    logger.info("Primeras columnas: %s", list(df.columns[:10]))

    null_counts = df.iloc[:, :10].isna().sum()
    logger.info("Nulos (primeras columnas):\n%s", null_counts)


# ==========================
#  I/O: CSV / JSON
# ==========================


def read_csv_with_logging(path: Path, dataset_name: str) -> pd.DataFrame:
    """
    Lee un CSV y controla errores comunes, sacando logs informativos.
    """
    logger = logging.getLogger(f"io.read_csv.{dataset_name}")
    logger.info("Leyendo CSV de %s desde: %s", dataset_name, path)

    if not path.exists():
        logger.error("El fichero %s no existe: %s", dataset_name, path)
        raise FileNotFoundError(f"No se encuentra el fichero de entrada: {path}")

    try:
        df = pd.read_csv(path)
        logger.info(
            "CSV de %s leído correctamente. Filas: %s, Columnas: %s",
            dataset_name,
            df.shape[0],
            df.shape[1],
        )
        return df
    except Exception as exc:
        logger.exception("Error leyendo CSV de %s: %s", dataset_name, exc)
        raise


def write_json_with_logging(df: pd.DataFrame, path: Path, dataset_name: str) -> None:
    """
    Escribe un DataFrame en formato JSON (una fila por línea),
    con logs y control de errores.
    """
    logger = logging.getLogger(f"io.write_json.{dataset_name}")
    logger.info("Guardando %s en formato JSON: %s", dataset_name, path)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_json(path, orient="records", lines=True, force_ascii=False)
        logger.info(
            "JSON de %s guardado correctamente. Filas: %s, Columnas: %s",
            dataset_name,
            df.shape[0],
            df.shape[1],
        )
    except Exception as exc:
        logger.exception("Error guardando JSON de %s: %s", dataset_name, exc)
        raise
