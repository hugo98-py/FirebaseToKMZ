# main.py
# -*- coding: utf-8 -*-
"""
FastAPI · Firestore → KMZ (link de descarga) · Render-ready
----------------------------------------------------------
GET /kmz?campana_id=...  ➜  {"download_url": ".../downloads/<archivo>.kmz"}

ENV obligatoria:
  FIREBASE_KEY_B64 = <service-account.json en base-64, una sola línea>

Start command (Render):
  uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import base64, io, json, os, uuid, warnings, zipfile
from pathlib import Path
from typing import List, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

import firebase_admin
from firebase_admin import credentials, firestore

# ───────────────────────────── CONFIG 🔧
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# ───────────────────────────── Firebase init
def init_firebase() -> firestore.Client:
    if firebase_admin._apps:                        # ya inicializado
        return firestore.client()

    b64 = os.getenv("FIREBASE_KEY_B64", "").strip()
    if not b64:
        raise RuntimeError(
            "❌  FIREBASE_KEY_B64 no está definida. "
            "Añádela en Render → Environment → Add Secret."
        )

    cred_info = json.loads(base64.b64decode(b64))
    firebase_admin.initialize_app(credentials.Certificate(cred_info))
    return firestore.client()

db = init_firebase()
warnings.filterwarnings("ignore", category=UserWarning)

# ───────────────────────────── Firestore → KMZ helpers
def fetch_coordinates(campana_id: str) -> List[Tuple[float, float]]:
    docs = (
        db.collection("Registro")              # ajusta nombre si es distinto
        .where("campanaID", "==", campana_id.strip('"'))
        .stream()
    )
    coords: List[Tuple[float, float]] = []
    for d in docs:
        data = d.to_dict() or {}
        geo = data.get("Coordinates")          # campo con C mayúscula
        try:                                   # GeoPoint
            lon, lat = geo.longitude, geo.latitude  # type: ignore
        except AttributeError:
            if isinstance(geo, dict):          # {'longitude': ..., 'latitude': ...}
                lon, lat = geo.get("longitude"), geo.get("latitude")
            elif isinstance(geo, (list, tuple)) and len(geo) == 2:  # [lat, lon]
                lat, lon = geo
            else:
                continue
        if None not in (lat, lon):
            coords.append((lon, lat))
    return coords


def coords_to_kml(coords: List[Tuple[float, float]]) -> str:
    placemarks = "\n".join(
        f"""    <Placemark>
      <Point><coordinates>{lon},{lat},0</coordinates></Point>
    </Placemark>"""
        for lon, lat in coords
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
{placemarks}
  </Document>
</kml>
"""


def write_kmz_file(kml_str: str, filename: str) -> Path:
    kmz_path = DOWNLOAD_DIR / filename
    with zipfile.ZipFile(kmz_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_str.encode("utf-8"))
    return kmz_path


# ───────────────────────────── FastAPI
app = FastAPI(title="API Firestore → KMZ (env-key)")

# Carpeta estática para descargas
app.mount("/downloads", StaticFiles(directory=DOWNLOAD_DIR), name="downloads")

# CORS simple; ajusta allow_origins en producción si lo necesitas
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"]
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get(
    "/kmz",
    summary="Genera KMZ y devuelve enlace de descarga",
    response_description="URL al KMZ generado",
)
def get_kmz(
    request: Request,
    campana_id: str = Query(..., description="ID de campaña a filtrar"),
):
    coords = fetch_coordinates(campana_id)
    if not coords:
        raise HTTPException(
            404, f"No hay registros con 'Coordinates' para campanaID='{campana_id}'."
        )

    # Evita colisiones si piden lo mismo muchas veces
    filename = f"registros_{campana_id}_{uuid.uuid4().hex[:6]}.kmz"
    write_kmz_file(coords_to_kml(coords), filename)

    download_url = request.url_for("downloads", path=filename)
    return JSONResponse({"download_url": str(download_url)})

