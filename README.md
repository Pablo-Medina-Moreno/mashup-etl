# mashup-recommender

# Mashup Recommender – ETL (Fase Extract)

## Estructura básica

- `data/input/`  
  Coloca aquí los ficheros originales descargados de Kaggle:
  - `spotify_tracks.csv`
  - `apple_music.csv`

- `data/raw/`  
  Aquí se generarán los ficheros Parquet "raw":
  - `spotify_tracks.parquet`
  - `apple_music.parquet`

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate   # en Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m src.main_extract
python -m src.main_transform
python -m src.main_load
```

---

## 🔟 Qué estamos haciendo exactamente (en términos de ETL)

Solo para dejarlo claro conceptualmente:

- **E (Extract)**:  
  - Tomamos los **datasets originales** (Spotify y Apple Music) tal y como los hemos descargado.
  - Los leemos con pandas de forma robusta, comprobando que existen, número de filas, etc.
  - Hacemos una **validación ligera**: columnas importantes, nulos básicos.

- **Resultado de esta fase**:  
  - Tenemos una “**zona RAW**” (`data/raw/`) en un formato optimizado (Parquet), sobre el que será más cómodo y eficiente trabajar en las siguientes fases (**Transform** & **Load al Data Warehouse**).

A partir de aquí, el siguiente paso natural será la **T (Transform)**:  
- Homogeneizar columnas (nombres, tipos).  
- Empezar a definir el modelo común (track, artista, tonalidad, etc.).  
- Preparar las tablas para el futuro Data Warehouse.

---

Si quieres, en el siguiente mensaje podemos:

- Diseñar y codificar la **fase Transform** sobre estos Parquet (limpieza + normalización de tonos/BPM),  
o  
- Añadir ya el **extract de alguna API** (por ejemplo, Spotify Web API para popularidad actual) y dejar otro módulo `extract_spotify_api.py`.
::contentReference[oaicite:0]{index=0}
