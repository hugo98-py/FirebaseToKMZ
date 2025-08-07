# main.py
# -*- coding: utf-8 -*-
"""
FastAPI · Firestore → KMZ (+ enlace de descarga)
-----------------------------------------------
• GET /kmz?campana_id=...  →  {"download_url": ".../downloads/<archivo>.kmz"}
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

# 🔑  TU CLAVE BASE-64 PEGA AQUÍ (una sola línea)
FIREBASE_KEY_B64 = """ "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYW1zYXBwdGVycmVubyIsCiAgInByaXZhdGVfa2V5X2lkIjogIjE2MDEwOGFjNzJjODNjNDhjMjczZTkwNGZjMGY4YjJiNzcyZGRhYmMiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRRGorTzFCQ1pqS0cwMERcbjBwU3ExUjAxcXNPd3VlK0lDSzRDUExkam11Q1orSDZ2T0N0T0trZkhXY3pNTXN5d1FHTEhaNGMwNWNHRUJyZk1cbjF3aWZwU3h6RFM1TElnTWtFbzZvZmRaY2tIcUkxNTlHbzZBelFLaXdEUWZEdngrRmdKNURoRmUyd3crV2FtOHFcbktBUEt5MGpvelNqUnVSZ0dINXV3RkRmejE5MWZRbVJaQXRxT3h6S3J3aE9OL2c4NjNqdXdiRDRFa2FVTmMzQWpcbjlJdGxLV0s3aWs1Rmlja0dHQXBYOVM5RGZGT2xlMmw5VWZYQzM2L2Rvc0hRc1FLUzRWeThGZjFNZHBiN3hMRVhcbjhZMzh3b05aY01kOFRwUWd1Rm1teUl1TlZyVVoydjA3QTkvak96bVJpa1hxcnRCMmpPYTlLejRQSzFmbm1pRUNcbkNMS2xVcEpyQWdNQkFBRUNnZ0VBRVRUZ0gya01oYkNpMHZlSmJnMXd5M1NzbVlHbk1TTEtrM09tdWRkZVBYRFNcbml3SVh6cW9GU2VnZXU0allCek9NNFJVUTBPN0llT0VTN1Z5TGs1VWhjanViODJMSlR1Y3hxa2o1TFhwZ0xVVHRcbkNpVmVlWS9YUzVab01CK1VUdnpDdFVEZlovVjREemZRVlNPNUNZem5YMTdneGxlTWx5VzZtQU1tcC9weWhIWjNcblkvNFA2WG13N2xFWmpwckZ6a3NGUFZjSXc0ZkRWTGRyd0JqUHdVMTBSWGphU2pZWlQxLzJ2RDZnU2dzYTFiSmNcbmVrUVR3TUcwd3lRVGQrYjBFcW1WN1l0Q3lTWHpYaE1TbWFBRVZ5VThweGdwaWNsdmxsSXVsVkNVSkoxVllBdmRcbmZVdks5d2RpalZSMDgxY1JweWdLVU83RUg1YWdncmVHYjdqaEdmS0M5UUtCZ1FENk14NFl6bUFoQ3phVEpJZVVcbjNGRUFWcnVFamtZMTdGb2lUWnhHWkxzWUV6bnEzR0dhS1dQNlNwdmY4UDNrWXJoQ1p0RnZPcVFtMDZFdnJHM1ZcbmltaDBEbjFuNWJhalFpWVR5dE8yTzhqSi9CRlhCODFyeG9MYnNLN2hlVzRXM2l3endJcjNiVEZROUhseU82QklcblBYQy8zWHlUT0FkWGkrUUF5cmxVQTh4LzdRS0JnUURwUWVVU2d1bnUzOEswR25nd3hhZDRKSEtKZFFaNnVTTnNcbmN0N3d4RHI1Vkl0TDRxZVpoeXFGZUh5QUswVWZwZHhTWmpNMFZIUjlSZVREcFM5aG9vSlphcjhMb3pZOEJoYkFcbmRTcjljckdrTzJmeUp4VU03M3FQK01oM0F6N0RST2lLMlJibUZ2THRBR0Y0ZW5LenIwRDU0QkR4NFVWN0lGSjNcblRnb2JPd3VndHdLQmdEQXp2TzZtZzlxWjRHL0VyK0k3OTljSzNxR1JEM0hBRVRWZW9tYmxiUzRjTmlKTFJ2cCtcblV1YWJqU0E5dTR4T2dKVTZYY3NDRHpIenY3QUJsajkzcE53SEZwMzNvWkllWWx1WnplUGo3TEVOUVFIMVhOd1FcbnFKRk9uc0tOdEdudmI3Q2d2YVVoZ2tqSWNNVlRPM1B2Y2xOa2htV09lOHVnQmRNcE96ODJaQ1daQW9HQkFNSE5cbjBqcVRNek1hRDNmOUZpZlAwZGxzencyT2doZXRnOVV6akRDWG1qaFhHbld1bUhwalo5MEZ3c3ZySjcrbjIydExcbkp2Z3Z3Nm5pVExTUnB0Q0VhYkZKay9walhVaDU5cVZwdEpKb25WaExVNDVMRXA1d3kzQ2IwRk95Q28vTHpUcXdcbkxPcDMzdGMwSGNnd1ZPVWVrK29KVllyakZRdThTMGg5T3NCdFNyZjVBb0dCQU1KWFp3aGEwZGx3RDBqanNaTWhcbjYwSmRyb3AxTU9UM25PS2Zram8wTTYrQjFZV0xKcnFGaWhxKysxMkxrSExxWFNPK1Frc1VxbmtlMSsvU1E3R2Vcbiszd2JTcnJodlV0S2YzWVQ0L0oyY2dSd0xKQXpCYktsVllaTjdBK3dHZTVaODE2U2F0U0dOSllhUThpejZHdE5cbnJnTFRjQzlCTnhjYlhYbWFJQWVTVkxIQVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogImZpcmViYXNlLWFkbWluc2RrLTltYmxnQGFtc2FwcHRlcnJlbm8uaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTA1NTg0NDc0ODI5NTMwNDIwNzQ3IiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9maXJlYmFzZS1hZG1pbnNkay05bWJsZyU0MGFtc2FwcHRlcnJlbm8uaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K" """
# (Usa tu base64 real de pruebas)

# ───────────────────────────── Firebase
def init_firebase() -> firestore.Client:
    if firebase_admin._apps:
        return firestore.client()
    cred_info = json.loads(base64.b64decode(FIREBASE_KEY_B64))
    firebase_admin.initialize_app(credentials.Certificate(cred_info))
    return firestore.client()

db = init_firebase()
warnings.filterwarnings("ignore", category=UserWarning)

# ───────────────────────────── Helpers Firestore → KMZ
def fetch_coordinates(campana_id: str) -> List[Tuple[float, float]]:
    docs = (
        db.collection("Registro")
        .where("campanaID", "==", campana_id.strip('"'))
        .stream()
    )
    coords: List[Tuple[float, float]] = []
    for d in docs:
        data = d.to_dict() or {}
        geo = data.get("Coordinates")  # ← campo con mayúscula
        try:
            lon, lat = geo.longitude, geo.latitude  # type: ignore
        except AttributeError:
            if isinstance(geo, dict):
                lon, lat = geo.get("longitude"), geo.get("latitude")
            elif isinstance(geo, (list, tuple)) and len(geo) == 2:
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
    """Crea downloads/<filename>.kmz y devuelve la ruta."""
    kmz_path = DOWNLOAD_DIR / filename
    with zipfile.ZipFile(kmz_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_str.encode("utf-8"))
    return kmz_path

# ───────────────────────────── FastAPI
app = FastAPI(title="API Firestore → KMZ (link de descarga)")

# Montamos carpeta estática para servir descargas
app.mount("/downloads", StaticFiles(directory=DOWNLOAD_DIR), name="downloads")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # ciérralo en prod
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get(
    "/kmz",
    summary="Genera KMZ y devuelve enlace de descarga",
    response_description="URL al archivo generado",
)
def get_kmz(
    request: Request,
    campana_id: str = Query(..., description="ID de campaña"),
):
    coords = fetch_coordinates(campana_id)
    if not coords:
        raise HTTPException(404, f"No hay registros para campanaID='{campana_id}'.")

    # Nombre único para evitar colisiones (campana + 6 caracteres aleatorios)
    slug = uuid.uuid4().hex[:6]
    filename = f"registros_{campana_id}_{slug}.kmz"
    write_kmz_file(coords_to_kml(coords), filename)

    # Construimos URL absoluta
    download_url = request.url_for("downloads", path=filename)

    return JSONResponse({"download_url": str(download_url)})
