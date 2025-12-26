import os
import io
import logging
from typing import Optional

from flask import Flask, request
import requests
import pdfplumber
from telegram import Update, Bot
from telegram.ext import Dispatcher, CommandHandler, CallbackContext

# ================== CONFIGURACIÓN ==================

# Token del bot: en Render lo pondremos como variable de entorno TELEGRAM_TOKEN
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")

# URLs de los PDFs
URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/tmp/LLuvia_7d%C3%ADas.pdf"

# Búsqueda flexible (por si cambia el código Pxx): con que la fila contenga "Huelma"
ESTACION_HUELMA = "Huelma"

# ================== LOGGING ==================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ================== FLASK + TELEGRAM ==================

app = Flask(__name__)

if not TELEGRAM_TOKEN:
    logger.warning("TELEGRAM_TOKEN está vacío. En Render debes configurarlo en Environment Variables.")

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
dispatcher = Dispatcher(bot, None, workers=0) if bot else None

# ================== FUNCIONES DE DATOS ==================

def descargar_pdf(url: str) -> bytes:
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.content


def extraer_linea_estacion(pdf_bytes: bytes, nombre_estacion: str) -> Optional[str]:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        texto = ""
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            texto += page_text + "\n"

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


def formatear_salida(nombre_pdf: str, linea: Optional[str]):
    if not linea:
        return f"No se ha encontrado la estación Huelma en {nombre_pdf}."

    valores = parsear_valores(linea)
    texto = f"📄 *{nombre_pdf}* – Huelma\n"
    texto += f"`{linea}`\n\n"
    if valores:
        texto += "Valores numéricos detectados (mm):\n"
        texto += ", ".join(f"{v:.1f}" for v in valores)
    else:
        texto += "No se han podido extraer valores numéricos."
    return texto


def obtener_datos_hoy():
    pdf_bytes = descargar_pdf(URL_HOY)
    linea = extraer_linea_estacion(pdf_bytes, ESTACION_HUELMA)
    return formatear_salida("Lluvia_Hoy", linea)


def obtener_datos_7dias():
    pdf_bytes = descargar_pdf(URL_7DIAS)
    linea = extraer_linea_estacion(pdf_bytes, ESTACION_HUELMA)
    return formatear_salida("LLuvia_7días", linea)


# ================== HANDLERS DE TELEGRAM ==================

def start(update: Update, context: CallbackContext):
    msg = (
        "Hola 👋\n"
        "Soy un bot que te da los datos de lluvia de *Huelma*.\n\n"
        "Comandos:\n"
        "• /huelma - Hoy + 7 días\n"
        "• /hoy - Solo hoy\n"
        "• /siete - Solo 7 días\n"
    )
    update.message.reply_markdown(msg)


def cmd_hoy(update: Update, context: CallbackContext):
    try:
        texto = obtener_datos_hoy()
    except Exception as e:
        logger.exception("Error en /hoy")
        texto = f"Error obteniendo datos de hoy: {e}"
    update.message.reply_markdown(texto)


def cmd_siete(update: Update, context: CallbackContext):
    try:
        texto = obtener_datos_7dias()
    except Exception as e:
        logger.exception("Error en /siete")
        texto = f"Error obteniendo datos de 7 días: {e}"
    update.message.reply_markdown(texto)


def cmd_huelma(update: Update, context: CallbackContext):
    try:
        texto_hoy = obtener_datos_hoy()
    except Exception as e:
        logger.exception("Error en /huelma (hoy)")
        texto_hoy = f"Error Lluvia_Hoy: {e}"

    try:
        texto_7 = obtener_datos_7dias()
    except Exception as e:
        logger.exception("Error en /huelma (7 días)")
        texto_7 = f"Error LLuvia_7días: {e}"

    update.message.reply_markdown(texto_hoy + "\n\n" + texto_7)


def registrar_handlers():
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("hoy", cmd_hoy))
    dispatcher.add_handler(CommandHandler("siete", cmd_siete))
    dispatcher.add_handler(CommandHandler("huelma", cmd_huelma))


# ================== RUTAS WEB (WEBHOOK) ==================

@app.route("/")
def index():
    return "Bot de lluvia de Huelma funcionando.", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Telegram enviará aquí las updates.
    En Render configuraremos el webhook a:
      https://TU-SERVICIO.onrender.com/webhook
    """
    if not bot or not dispatcher:
        return "TELEGRAM_TOKEN no configurado", 500

    try:
        json_update = request.get_json(force=True)
        update = Update.de_json(json_update, bot)
        dispatcher.process_update(update)
    except Exception:
        logger.exception("Error procesando update")
    return "ok", 200


# ================== ARRANQUE LOCAL (opcional) ==================

if __name__ == "__main__":
    if TELEGRAM_TOKEN:
        registrar_handlers()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
