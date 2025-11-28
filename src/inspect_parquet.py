# src/inspect_parquet.py
"""
Herramienta para inspeccionar archivos Parquet del ETL.

Ejemplo:
    python -m src.inspect_parquet songs_integrated
    python -m src.inspect_parquet spotify_tracks_clean
    python -m src.inspect_parquet track_data_final_clean
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from config import PROCESSED_DIR


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )


def list_available_parquets():
    print("\nParquets disponibles en data/processed:\n")
    files = list(PROCESSED_DIR.glob("*.parquet"))
    for f in files:
        print(f"  - {f.name}")
    print()


def inspect_parquet(name: str):
    """
    Inspecciona un parquet por nombre (sin extensión).
    """
    path = PROCESSED_DIR / f"{name}.parquet"

    if not path.exists():
        print(f"\n❌ No existe: {path}")
        print("\nParquets disponibles:")
        list_available_parquets()
        return

    print(f"\n📄 Leyendo parquet: {path}")

    # Mostrar schema (PyArrow)
    table = pq.read_table(path)
    print("\n=== Esquema del parquet ===")
    print(table.schema)

    # Convertir a pandas para mostrar datos
    df = pd.read_parquet(path)

    print("\n=== Primeras filas ===")
    print(df.head(10))

    print("\n=== Columnas ===")
    print(df.columns.tolist())

    print("\n=== Info Dataset ===")
    print(f"Filas: {df.shape[0]}")
    print(f"Columnas: {df.shape[1]}\n")


def main():
    setup_logging()

    if len(sys.argv) < 2:
        print("\nUso correcto:")
        print("    python -m src.inspect_parquet <nombre_sin_extension>")
        print("\nEjemplos:")
        print("    python -m src.inspect_parquet songs_integrated")
        print("    python -m src.inspect_parquet spotify_tracks_clean")
        print()
        list_available_parquets()
        return

    parquet_name = sys.argv[1]
    inspect_parquet(parquet_name)


if __name__ == "__main__":
    main()
