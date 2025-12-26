import os
import io
import re
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List

import requests
import pdfplumber
from flask import Flask, request

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ================== CONFIG ==================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/tmp/LLuvia_7d%C3%ADas.pdf"

ESTACION_HUELMA_KEY = "Huelma"

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== FLASK ==================
app = Flask(__name__)

# ================== TELEGRAM APP ==================
tg_app: Optional[Application] = None
tg_initialized = False

# ================== REGEX ==================
DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
TS_RE = re.compile(r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})")


# ================== UTILIDADES ==================
def _unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extraer_metadata(texto: str) -> Optional[str]:
    m = TS_RE.search(texto)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return None


def fechas_ultimos_7_dias_desde_timestamp(ts: Optional[str]) -> List[str]:
    if ts:
        try:
            end_date = datetime.strptime(ts.split()[0], "%d/%m/%Y").date()
        except Exception:
            end_date = datetime.utcnow().date()
    else:
        end_date = datetime.utcnow().date()

    return [
        (end_date - timedelta(days=d)).strftime("%d/%m/%Y")
        for d in range(6, -1, -1)
    ]


def parsear_valores(linea: str) -> List[float]:
    partes = linea.replace(",", ".").split()
    valores = []
    for token in partes:
        if re.fullmatch(r"-?\d+(\.\d+)?", token):
            try:
                valores.append(float(token))
            except ValueError:
                pass
    return valores


# ================== PDF ==================
def descargar_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.content


def extraer_texto_pdf(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def extraer_linea_estacion(texto: str, key: str) -> Optional[str]:
    for linea in texto.splitlines():
        if key.lower() in linea.lower():
            return linea.strip()
    return None


# ================== FORMATEO ==================
def formatear_hoy(timestamp: Optional[str], linea: Optional[str]) -> str:
    header = "📄 *Lluvia_Hoy*"
    if timestamp:
        header += f" (actualizado: {timestamp})"

    if not linea:
        return header + "\n\nNo encuentro datos de lluvia de hoy en *Huelma*."

    valores = parsear_valores(linea)
    msg = header + "\n"
    msg += f"*Huelma*:\n`{linea}`\n"
    if valores:
        msg += "\nValores detectados (mm): " + ", ".join(f"{v:.1f}" for v in valores)
    return msg


def formatear_7dias(timestamp: Optional[str], linea: Optional[str]) -> str:
    header = "📄 *Lluvia_7días*"
    if timestamp:
        header += f" (actualizado: {timestamp})"

    if not linea:
        return header + "\n\nNo encuentro la fila de *Huelma* en el PDF."

    valores = parsear_valores(linea)

    if len(valores) < 7:
        return header + "\n\nNo hay suficientes valores diarios."

    # 7 días
    lluvias_7 = valores[:7]
    fechas_7 = fechas_ultimos_7_dias_desde_timestamp(timestamp)

    # Acumulados
    mes_actual = valores[-2] if len(valores) >= 2 else None
    anio_hidrologico = valores[-1] if len(valores) >= 1 else None

    msg = header + "\n"
    msg += "*Huelma – lluvia diaria (mm):*\n"
    for f, v in zip(fechas_7, lluvias_7):
        msg += f"• {f}: *{v:.1f}* mm\n"

    msg += "\n*Acumulados:*\n"
    if mes_actual is not None:
        msg += f"• Mes actual: *{mes_actual:.1f}* mm\n"
    if anio_hidrologico is not None:
        msg += f"• Año hidrológico: *{anio_hidrologico:.1f}* mm\n"

    return msg.strip()


# ================== OBTENCIÓN ==================
def obtener_hoy() -> str:
    pdf = descargar_pdf(URL_HOY)
    texto = extraer_texto_pdf(pdf)
    ts = extraer_metadata(texto)
    linea = extraer_linea_estacion(texto, ESTACION_HUELMA_KEY)
    return formatear_hoy(ts, linea)


def obtener_7dias() -> str:
    pdf = descargar_pdf(URL_7DIAS)
    texto = extraer_texto_pdf(pdf)
    ts = extraer_metadata(texto)
    linea = extraer_linea_estacion(texto, ESTACION_HUELMA_KEY)
    return formatear_7dias(ts, linea)


# ================== TELEGRAM ==================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hola 👋\n"
        "Datos de lluvia en Huelma.\n\n"
        "/hoy\n"
        "/siete\n"
        "/huelma"
    )


async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(obtener_hoy())


async def siete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(obtener_7dias())


async def huelma_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(
        obtener_hoy() + "\n\n" + obtener_7dias()
    )


async def init_telegram_once():
    global tg_app, tg_initialized
    if tg_initialized:
        return

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN no configurado")

    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("hoy", hoy_cmd))
    tg_app.add_handler(CommandHandler("siete", siete_cmd))
    tg_app.add_handler(CommandHandler("huelma", huelma_cmd))

    await tg_app.initialize()
    tg_initialized = True


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
