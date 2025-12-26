import os
import io
import logging
import asyncio
from typing import Optional

import requests
import pdfplumber
from flask import Flask, request

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ================== CONFIG ==================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/tmp/LLuvia_7d%C3%ADas.pdf"
ESTACION_HUELMA = "Huelma"  # búsqueda flexible

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== FLASK ==================
app = Flask(__name__)

# ================== TELEGRAM APP (ptb v21+) ==================
tg_app: Optional[Application] = None
tg_initialized = False


# ================== PDF HELPERS ==================
def descargar_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.content


def extraer_linea_estacion(pdf_bytes: bytes, nombre_estacion: str) -> Optional[str]:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        texto = ""
        for page in pdf.pages:
            texto += (page.extract_text() or "") + "\n"

    for linea in texto.splitlines():
        if nombre_estacion.lower() in linea.lower():
            return linea.strip()
    return None


def parsear_valores(linea: str):
    partes = linea.replace(",", ".").split()
    vals = []
    for token in partes:
        if any(ch.isdigit() for ch in token):
            try:
                vals.append(float(token))
            except ValueError:
                pass
    return vals


def formatear_salida(nombre: str, linea: Optional[str]) -> str:
    if not linea:
        return f"No se ha encontrado la estación Huelma en {nombre}."

    valores = parsear_valores(linea)
    msg = f"📄 *{nombre}* – Huelma\n"
    msg += f"`{linea}`\n\n"
    if valores:
        msg += "Valores numéricos detectados (mm):\n" + ", ".join(f"{v:.1f}" for v in valores)
    else:
        msg += "No se han podido extraer valores numéricos."
    return msg


def obtener_datos_hoy() -> str:
    pdf = descargar_pdf(URL_HOY)
    linea = extraer_li_

