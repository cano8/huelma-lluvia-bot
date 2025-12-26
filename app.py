import os
import io
import re
import logging
import asyncio
from datetime import datetime, date
from typing import Optional, Tuple, List

import requests
import pdfplumber
from flask import Flask, request

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ================== CONFIG ==================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/tmp/LLuvia_7d%C3%ADas.pdf"

# Búsqueda flexible: si en el futuro cambia P63, seguimos encontrando la fila
ESTACION_HUELMA_KEY = "Huelma"

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== FLASK ==================
app = Flask(__name__)

# ================== TELEGRAM APP ==================
tg_app: Optional[Application] = None
tg_initialized = False


# ================== UTILIDADES TEXTO ==================
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
DATETIME_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})\b")


def _unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extraer_metadata(texto: str) -> Tuple[Optional[str], List[str]]:
    """
    Devuelve:
      - timestamp (str) si encuentra algo tipo 'dd/mm/yyyy hh:mm'
      - lista de fechas 'dd/mm/yyyy' únicas en orden de aparición
    """
    ts = None
    m = DATETIME_RE.search(texto)
    if m:
        ts = f"{m.group(1)} {m.group(2)}"

    fechas = DATE_RE.findall(texto)
    fechas = _unique_keep_order(fechas)

    return ts, fechas


def parsear_valores(linea: str) -> List[float]:
    """
    Extrae números de una línea que puede venir con comas decimales.
    Ej: "..., 0,7 13,5 0,6 ..." => [0.7, 13.5, 0.6, ...]
    """
    # normaliza coma decimal a punto y separa por espacios
    partes = linea.replace(",", ".").split()
    valores = []
    for token in partes:
        # admitimos 0.0, 13.5, etc.
        if re.fullmatch(r"-?\d+(\.\d+)?", token):
            try:
                valores.append(float(token))
            except ValueError:
                pass
    return valores


# ================== PDF HELPERS ==================
def descargar_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.content


def extraer_texto_pdf(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        textos = []
        for page in pdf.pages:
            textos.append(page.extract_text() or "")
        return "\n".join(textos)


def extraer_linea_estacion(texto: str, nombre_estacion: str) -> Optional[str]:
    for linea in texto.splitlines():
        if nombre_estacion.lower() in linea.lower():
            return linea.strip()
    return None


# ================== FORMATEO RESPUESTAS ==================
def formatear_hoy(timestamp: Optional[str], linea: Optional[str]) -> str:
    # Si no hay datos para Huelma hoy
    if not linea:
        if timestamp:
            return f"📄 *Lluvia_Hoy* (actualizado: {timestamp})\n\nNo encuentro datos de lluvia de hoy en *Huelma*."
        return "📄 *Lluvia_Hoy*\n\nNo encuentro datos de lluvia de hoy en *Huelma*."

    # Si sí hay datos, mostramos línea + números (por si quieres luego mapear columnas)
    valores = parsear_valores(linea)
    header = f"📄 *Lluvia_Hoy*"
    if timestamp:
        header += f" (actualizado: {timestamp})"
    msg = header + "\n"
    msg += f"*Huelma*:\n`{linea}`\n"
    if valores:
        msg += "\nValores numéricos detectados (mm):\n" + ", ".join(f"{v:.1f}" for v in valores)
    return msg


def formatear_7dias(timestamp: Optional[str], fechas: List[str], linea: Optional[str]) -> str:
    if not linea:
        header = "📄 *Lluvia_7días*"
        if timestamp:
            header += f" (actualizado: {timestamp})"
        return header + "\n\nNo encuentro la fila de *Huelma* en el PDF."

    valores = parsear_valores(linea)

    header = "📄 *Lluvia_7días*"
    if timestamp:
        header += f" (actualizado: {timestamp})"

    msg = header + "\n"
    msg += f"*Huelma*:\n`{linea}`\n"

    # --- Emparejar días con valores ---
    # En muchos PDFs de 7 días, el encabezado tiene 7 fechas y la fila tiene 7 valores (a veces + totales).
    # Estrategia robusta:
    # - Si encontramos >= 7 fechas en el PDF, nos quedamos con las 7 primeras (o las últimas, depende del formato).
    # - Si hay más valores que fechas, cogemos los últimos N valores (suele incluir totales al final o al principio).
    # Como no tenemos aquí el formato exacto de tu PDF, hacemos un emparejamiento prudente:
    #   - Tomamos las ÚLTIMAS 7 fechas encontradas (normalmente el tramo "últimos 7 días").
    #   - Tomamos las ÚLTIMAS 7 lluvias de la línea (ignorando posibles acumulados finales).
    fechas_7 = fechas[-7:] if len(fechas) >= 7 else fechas

    if len(fechas_7) >= 7 and len(valores) >= 7:
        lluvias_7 = valores[-7:]
        msg += "\nLluvia por día (mm):\n"
        for f, v in zip(fechas_7, lluvias_7):
            msg += f"• {f}: {v:.1f} mm\n"
        return msg.strip()

    # Si no conseguimos 7 fechas, devolvemos al menos timestamp + los valores detectados
    if valores:
        msg += "\nValores numéricos detectados (mm):\n" + ", ".join(f"{v:.1f}" for v in valores)

    return msg


# ================== OBTENER DATOS ==================
def obtener_hoy() -> str:
    pdf = descargar_pdf(URL_HOY)
    texto = extraer_texto_pdf(pdf)
    ts, _fechas = extraer_metadata(texto)
    linea = extraer_linea_estacion(texto, ESTACION_HUELMA_KEY)
    return formatear_hoy(ts, linea)


def obtener_7dias() -> str:
    pdf = descargar_pdf(URL_7DIAS)
    texto = extraer_texto_pdf(pdf)
    ts, fechas = extraer_metadata(texto)
    linea = extraer_linea_estacion(texto, ESTACION_HUELMA_KEY)
    return formatear_7dias(ts, fechas, linea)


# ================== TELEGRAM COMMANDS ==================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hola 👋\n"
        "Soy un bot que te da los datos de lluvia de Huelma.\n\n"
        "Comandos:\n"
        "/huelma  → Hoy + 7 días\n"
        "/hoy\n"
        "/siete\n"
    )


async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = obtener_hoy()
    except Exception as e:
        logger.exception("Error /hoy")
        msg = f"Error obteniendo datos de hoy: {e}"
    await update.message.reply_markdown(msg)


async def siete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = obtener_7dias()
    except Exception as e:
        logger.exception("Error /siete")
        msg = f"Error obteniendo datos de 7 días: {e}"
    await update.message.reply_markdown(msg)


async def huelma_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        h = obtener_hoy()
    except Exception as e:
        logger.exception("Error hoy")
        h = f"Error Lluvia_Hoy: {e}"

    try:
        s = obtener_7dias()
    except Exception as e:
        logger.exception("Error 7 días")
        s = f"Error Lluvia_7días: {e}"

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
