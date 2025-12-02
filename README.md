# 🎵 ETL Musical

## 📌 Descripción del proyecto

Este repositorio implementa un **pipeline ETL completo** en Python que integra tres datasets musicales:

1. **Spotify Tracks (Kaggle)**
2. **Spotify–YouTube Dataset**
3. **Spotify Global Music (track_data_final.csv)**

El objetivo es construir un **modelo relacional normalizado en PostgreSQL**, consolidando información sobre:

- Canciones (tracks)  
- Álbumes  
- Artistas  
- Géneros de artistas  
- Relaciones Track–Artista (N:N)

El proyecto sigue la arquitectura clásica:

➡️ **EXTRACT → TRANSFORM → INTEGRATE → LOAD**

---

# 📁 Estructura del proyecto
```
mashup-recommender/
│
├── data/
│ ├── input/ ← CSV originales
│ ├── raw/ ← Se genera en EXTRACT
│ └── processed/ ← Se genera en TRANSFORM
│
├── src/
│ ├── extract_spotify.py
│ ├── extract_spotify_youtube.py
│ ├── extract_track_data_final.py
│ ├── transform_spotify.py
│ ├── transform_spotify_youtube.py
│ ├── transform_track_data_final.py
│ ├── transform_integrated.py
│ ├── load_schema.py
│ ├── main_extract.py
│ ├── main_transform.py
│ ├── main_load.py
│ ├── utils_io.py
│ └── utils_db.py
│
├── config.py
├── requirements.txt
└── README.md
```

---

# 🔧 Instalación y ejecución del pipeline completo

## 1. Descomprimir el proyecto

Descargar el `.zip` y extraerlo. La estructura debe quedar así:

```
mashup-recommender/
│
├── data/
│ └── input/ ← CSV originales
│
├── src/
├── config.py
├── requirements.txt
└── README.md
```
--

## 2. Crear entorno virtual e instalar dependencias

### En Windows

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### En Linux / MacOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
# 3. Ejecutar el pipeline ETL completo

Ejecuta los siguientes comandos **en este orden** desde la raíz del proyecto (donde está `src/` y `config.py`):

---

## 🟦 1. EXTRACT  
Lee los CSV de `data/input/`, normaliza columnas y genera los JSON RAW en `data/raw/`.

```bash
python -m src.main_extract
```
Salida generada:

```
data/raw/
   spotify_tracks_raw.json
   spotify_youtube_raw.json
   track_data_final_raw.json
```
## 🟩 2. TRANSFORM

Procesa y limpia cada dataset, los convierte en formato anidado (track, album, artists) y luego ejecuta la integración final.

```bash
python -m src.main_transform
```
Salida generada:
```
data/processed/
   spotify_tracks_clean.json
   spotify_youtube_clean.json
   track_data_final_clean.json
   songs_integrated.json   ← archivo maestro final
```
## 🟧 3. LOAD

Carga el dataset integrado en PostgreSQL.

```bash
python -m src.main_load
```
