import os
import io
import re
import logging
import threading
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

# ================== REGEX ==================
TS_RE = re.compile(r"\b(\d{2}/\d{2}/(\d{2}|\d{4}))\s+(\d{2}:\d{2})\b")
DATE_2Y_RE = re.compile(r"\b(\d{2}/\d{2}/\d{2})\b")

# ================== TELEGRAM GLOBALS ==================
tg_app: Optional[Application] = None
tg_loop: Optional[asyncio.AbstractEventLoop] = None
tg_thread_started = False


# ================== UTILIDADES ==================
def normalizar_fecha_ddmmyy_a_ddmmyyyy(ddmmyy: str) -> str:
    d, m, yy = ddmmyy.split("/")
    yyyy = 2000 + int(yy)
    return f"{d}/{m}/{yyyy}"


def extraer_timestamp(texto: str) -> Optional[str]:
    m = TS_RE.search(texto)
    if not m:
        return None

    fecha = m.group(1)  # dd/mm/yy o dd/mm/yyyy
    hora = m.group(3)

    parts = fecha.split("/")
    if len(parts[2]) == 2:
        fecha = normalizar_fecha_ddmmyy_a_ddmmyyyy(fecha)

    return f"{fecha} {hora}"


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


def fecha_de_timestamp(ts: Optional[str]) -> str:
    if ts:
        try:
            return ts.split()[0]
        except Exception:
            pass
    return datetime.utcnow().strftime("%d/%m/%Y")


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


def extraer_fechas_cabecera_semanal(pdf_bytes: bytes, ts: Optional[str]) -> Optional[List[str]]:
    """
    Cabecera tipo: "Día actual" + "09/12/25" "08/12/25" ...
    Devuelve lista en orden de izquierda a derecha normalizada a dd/mm/yyyy.
    """
    fecha_actual = fecha_de_timestamp(ts)

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            page0 = pdf.pages[0]
            tables = page0.extract_tables() or []
    except Exception as e:
        logger.exception("No pude extraer tablas del PDF semanal: %s", e)
        return None

    for t in tables:
        for row in t:
            if not row:
                continue
            cells = [c.strip() for c in row if isinstance(c, str) and c.strip()]
            if not cells:
                continue

            joined = " ".join(cells)
            if ("Día actual" not in joined) and ("Dia actual" not in joined):
                continue

            fechas_2y = DATE_2Y_RE.findall(joined)
            if len(fechas_2y) < 3:
                continue

            out: List[str] = []
            if any("Día actual" in c or "Dia actual" in c for c in cells):
                out.append(fecha_actual)

            for f in fechas_2y:
                out.append(normalizar_fecha_ddmmyy_a_ddmmyyyy(f))

            # dedupe manteniendo orden
            seen = set()
            out2 = []
            for x in out:
                if x not in seen:
                    seen.add(x)
                    out2.append(x)

            return out2

    return None


# ================== FORMATEO ==================
def formatear_hoy(timestamp: Optional[str], linea: Optional[str]) -> str:
    header = "📄 *Lluvia diaria*"
    if timestamp:
        header += f" (actualizado: {timestamp})"
    else:
        header += " (actualizado: no detectado)"

    if not linea:
        return header + "\n\nParece que hoy no ha llovido en Huelma."

    valores = parsear_valores(linea)
    msg = header + "\n"
    msg += "*Huelma*:\n"
    if valores:
        msg += "Valores detectados (mm): " + ", ".join(f"{v:.1f}" for v in valores)
    else:
        msg += "He encontrado la fila, pero no he podido extraer valores numéricos."
    return msg


def formatear_semanal(timestamp: Optional[str], fechas_cols: Optional[List[str]], linea: Optional[str]) -> str:
    header = "📄 *Lluvia semanal*"
    if timestamp:
        header += f" (actualizado: {timestamp})"
    else:
        header += " (actualizado: no detectado)"

    if not linea:
        return header + "\n\nNo encuentro la fila de *Huelma* en el PDF."

    valores = parsear_valores(linea)
    if len(valores) < 7:
        return header + "\n\nNo hay suficientes valores para mostrar la semana."

    # acumulados (estructura actual: últimos 2 números)
    mes_actual = valores[-2] if len(valores) >= 2 else None
    anio_hidrologico = valores[-1] if len(valores) >= 1 else None

    # cuántas columnas diarias según cabecera real
    if fechas_cols and len(fechas_cols) >= 2:
        n = len(fechas_cols)
    else:
        fechas_cols = None
        n = 7

    if len(valores) < n:
        return header + f"\n\nNo hay suficientes valores diarios ({len(valores)}) para {n} columnas del PDF."

    lluvias = valores[:n]

    msg = header + "\n"
    msg += "*Huelma – lluvia diaria (mm):*\n"

    if fechas_cols:
        # cabecera ya viene con “más reciente -> más antiguo”
        for f, v in zip(fechas_cols, lluvias):
            msg += f"• {f}: *{v:.1f}* mm\n"
    else:
        # fallback
        end_date = datetime.strptime(fecha_de_timestamp(timestamp), "%d/%m/%Y").date()
        fechas_est = [(end_date - timedelta(days=i)).strftime("%d/%m/%Y") for i in range(0, n)]
        for f, v in zip(fechas_est, lluvias):
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
    ts = extraer_timestamp(texto)
    linea = extraer_linea_estacion(texto, ESTACION_HUELMA_KEY)
    return formatear_hoy(ts, linea)


def obtener_semanal() -> str:
    pdf = descargar_pdf(URL_7DIAS)
    texto = extraer_texto_pdf(pdf)
    ts = extraer_timestamp(texto)
    fechas_cols = extraer_fechas_cabecera_semanal(pdf, ts)
    linea = extraer_linea_estacion(texto, ESTACION_HUELMA_KEY)
    return formatear_semanal(ts, fechas_cols, linea)


# ================== TELEGRAM HANDLERS ==================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hola 👋\n"
        "Soy un bot para proporcionarte los datos de lluvia en Huelma.\n\n"
        "/hoy  → lluvia diaria\n"
        "/siete → lluvia semanal\n"
    )


async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_markdown(obtener_hoy())
    except Exception as e:
        logger.exception("Error /hoy")
        await update.message.reply_text(f"Error en /hoy: {e}")


async def siete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_markdown(obtener_semanal())
    except Exception as e:
        logger.exception("Error /siete")
        await update.message.reply_text(f"Error en /siete: {e}")


async def huelma_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_markdown(obtener_hoy() + "\n\n" + obtener_semanal())
    except Exception as e:
        logger.exception("Error /huelma")
        await update.message.reply_text(f"Error en /huelma: {e}")


# ================== TELEGRAM LOOP THREAD ==================
async def _tg_init_app():
    global tg_app
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN no configurado")

    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("hoy", hoy_cmd))
    tg_app.add_handler(CommandHandler("siete", siete_cmd))
    tg_app.add_handler(CommandHandler("huelma", huelma_cmd))

    await tg_app.initialize()
    logger.info("Telegram Application inicializada (loop persistente).")


def _tg_loop_runner():
    global tg_loop
    tg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(tg_loop)
    tg_loop.run_until_complete(_tg_init_app())
    tg_loop.run_forever()


def ensure_tg_thread():
    global tg_thread_started
    if tg_thread_started:
        return
    tg_thread_started = True
    th = threading.Thread(target=_tg_loop_runner, daemon=True)
    th.start()
    logger.info("Hilo de loop Telegram arrancado.")


# ================== FLASK ROUTES ==================
@app.get("/")
def index():
    return "OK: Bot de lluvia de Huelma funcionando", 200


@app.post("/webhook")
@app.post("/webhook/")
def webhook():
    try:
        ensure_tg_thread()

        update_json = request.get_json(force=True)
        if tg_loop is None or tg_app is None:
            return "not ready", 503

        update = Update.de_json(update_json, tg_app.bot)

        # Enviar el procesamiento al loop persistente
        fut = asyncio.run_coroutine_threadsafe(tg_app.process_update(update), tg_loop)
        # Esperamos a que termine (con timeout para no colgar)
        fut.result(timeout=20)

    except Exception:
        logger.exception("Error procesando update")
        # devolvemos ok igualmente para que Telegram no reintente en bucle agresivo
        return "ok", 200

    return "ok", 200

