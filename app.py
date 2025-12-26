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
# ❌ NO pongas el token aquí
# ✔️ El token se lee de la variable de entorno TELEGRAM_TOKEN
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/tmp/LLuvia_7d%C3%ADas.pdf"
ESTACION_HUELMA = "Huelma"

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== FLASK ==================
app = Flask(__name__)

# ================== TELEGRAM APP ==================
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
    valores = []
    for token in partes:
        if any(ch.isdigit() for ch in token):
            try:
                valores.append(float(token))
            except ValueError:
                pass
    return valores


def formatear_salida(nombre: str, linea: Optional[str]) -> str:
    if not linea:
        return f"No se ha encontrado la estación Huelma en {nombre}."

    valores = parsear_valores(linea)
    msg = f"📄 *{nombre}* – Huelma\n"
    msg += f"`{linea}`\n\n"

    if valores:
        msg += "Valores numéricos detectados (mm):\n"
        msg += ", ".join(f"{v:.1f}" for v in valores)
    else:
        msg += "No se han podido extraer valores numéricos."

    return msg


def obtener_datos_hoy() -> str:
    pdf = descargar_pdf(URL_HOY)
    linea = extraer_linea_estacion(pdf, ESTACION_HUELMA)
    return formatear_salida("Lluvia_Hoy", linea)


def obtener_datos_7dias() -> str:
    pdf = descargar_pdf(URL_7DIAS)
    linea = extraer_linea_estacion(pdf, ESTACION_HUELMA)
    return formatear_salida("LLuvia_7días", linea)


# ================== TELEGRAM COMMANDS ==================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hola 👋\n"
        "Soy un bot que te da los datos de lluvia de Huelma.\n\n"
        "Comandos disponibles:\n"
        "/huelma  → Hoy + 7 días\n"
        "/hoy\n"
        "/siete\n"
    )


async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = obtener_datos_hoy()
    except Exception as e:
        logger.exception("Error en /hoy")
        msg = f"Error obteniendo datos de hoy: {e}"
    await update.message.reply_markdown(msg)


async def siete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = obtener_datos_7dias()
    except Exception as e:
        logger.exception("Error en /siete")
        msg = f"Error obteniendo datos de 7 días: {e}"
    await update.message.reply_markdown(msg)


async def huelma_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        h = obtener_datos_hoy()
    except Exception as e:
        logger.exception("Error hoy")
        h = f"Error Lluvia_Hoy: {e}"

    try:
        s = obtener_datos_7dias()
    except Exception as e:
        logger.exception("Error 7 días")
        s = f"Error LLuvia_7días: {e}"

    await update.message.reply_markdown(h + "\n\n" + s)


# ================== INIT TELEGRAM ==================
async def init_telegram_once():
    global tg_app, tg_initialized

    if tg_initialized:
        return

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN no está configurado en Render")

    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()

    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("hoy", hoy_cmd))
    tg_app.add_handler(CommandHandler("siete", siete_cmd))
    tg_app.add_handler(CommandHandler("huelma", huelma_cmd))

    await tg_app.initialize()
    tg_initialized = True
    logger.info("Telegram Application inicializada")


# ================== FLASK ROUTES ==================
@app.get("/")
def index():
    return "OK: Bot de lluvia de Huelma funcionando", 200


@app.get("/ping")
def ping():
    return "pong", 200


@app.post("/webhook")
@app.post("/webhook/")
def webhook():
    try:
        update_json = request.get_json(force=True)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(init_telegram_once())
        update = Update.de_json(update_json, tg_app.bot)
        loop.run_until_complete(tg_app.process_update(update))

        loop.close()

    except Exception:
        logger.exception("Error procesando update")

    return "ok", 200



