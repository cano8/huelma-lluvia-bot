import os
import io
import re
import logging
import asyncio
from datetime import datetime
from typing import Optional, List, Tuple

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
# Timestamp puede venir como 26/12/2025 12:36 o 10/12/25 18:00
TS_RE = re.compile(r"\b(\d{2}/\d{2}/(\d{2}|\d{4}))\s+(\d{2}:\d{2})\b")
# Fechas de cabecera tipo 09/12/25
DATE_2Y_RE = re.compile(r"\b(\d{2}/\d{2}/\d{2})\b")


# ================== UTILIDADES ==================
def normalizar_fecha_ddmmyy_a_ddmmyyyy(ddmmyy: str) -> str:
    """09/12/25 -> 09/12/2025 (asumiendo 2000+yy)."""
    d, m, yy = ddmmyy.split("/")
    yyyy = 2000 + int(yy)
    return f"{d}/{m}/{yyyy}"


def extraer_timestamp(texto: str) -> Optional[str]:
    """
    Devuelve 'dd/mm/yyyy hh:mm' si detecta timestamp en el texto del PDF.
    Acepta año con 2 o 4 dígitos y normaliza a 4.
    """
    m = TS_RE.search(texto)
    if not m:
        return None

    fecha = m.group(1)  # dd/mm/yy o dd/mm/yyyy
    hora = m.group(3)

    # Normalizar año
    parts = fecha.split("/")
    if len(parts[2]) == 2:
        fecha = normalizar_fecha_ddmmyy_a_ddmmyyyy(fecha)

    return f"{fecha} {hora}"


def parsear_valores(linea: str) -> List[float]:
    """
    Extrae números de una línea que puede venir con comas decimales.
    Ej: "..., 0,7 13,5 0,6 ..." => [0.7, 13.5, 0.6, ...]
    """
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
    """
    ts: 'dd/mm/yyyy hh:mm' -> devuelve 'dd/mm/yyyy'
    fallback: hoy (UTC) si no hay ts
    """
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
    Intenta extraer las columnas de días de la cabecera del PDF semanal:
    - 'Día actual' y fechas tipo '09/12/25'
    Devuelve lista en orden de izquierda a derecha, normalizada a dd/mm/yyyy.
    """
    fecha_actual = fecha_de_timestamp(ts)

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            page0 = pdf.pages[0]
            tables = page0.extract_tables() or []
    except Exception:
        return None

    # Buscamos una fila que contenga "Día actual" y varias fechas dd/mm/yy
    for t in tables:
        for row in t:
            if not row:
                continue

            cells = [c.strip() for c in row if isinstance(c, str) and c.strip()]
            if not cells:
                continue

            joined = " ".join(cells)
            if "Día actual" not in joined and "Dia actual" not in joined:
                continue

            fechas_2y = DATE_2Y_RE.findall(joined)
            # Debe haber varias fechas; si no, esta fila no es la cabecera de días
            if len(fechas_2y) < 3:
                continue

            # Reconstruimos en orden:
            # Si hay "Día actual" como celda, lo metemos como primera fecha_actual
            out: List[str] = []
            # Detectamos presencia literal
            if any("Día actual" in c or "Dia actual" in c for c in cells):
                out.append(fecha_actual)

            # Añadimos las fechas dd/mm/yy (normalizadas)
            for f in fechas_2y:
                out.append(normalizar_fecha_ddmmyy_a_ddmmyyyy(f))

            # Eliminamos duplicados conservando orden (por si aparece repetida en la tabla)
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

    if not linea:
        return header + "\n\nParece que no ha llovido nada en *Huelma* hoy."

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

    # Mes actual y año hidrológico (según tu estructura actual)
    mes_actual = valores[-2] if len(valores) >= 2 else None
    anio_hidrologico = valores[-1] if len(valores) >= 1 else None

    # Determinar cuántas columnas diarias tenemos según cabecera
    # Si no se pudo extraer cabecera, hacemos fallback a 7 valores.
    if fechas_cols and len(fechas_cols) >= 2:
        n = len(fechas_cols)
    else:
        fechas_cols = None
        n = 7

    if len(valores) < n:
        return header + "\n\nNo hay suficientes valores diarios para emparejar con los días de la cabecera."

    lluvias = valores[:n]

    # Orden: más reciente arriba. Normalmente la cabecera viene: Día actual, 09/12/25, 08/12/25...
    # Eso ya es "más reciente -> más antiguo". Aun así, lo forzamos invirtiendo según necesidad:
    # Si detectamos que la segunda fecha es anterior a la primera, asumimos ya descendente.
    # Lo más simple aquí: mostramos en el orden de la cabecera y listo.
    pares = list(zip(fechas_cols, lluvias)) if fechas_cols else None

    msg = header + "\n"
    msg += "*Huelma – lluvia diaria (mm):*\n"

    if pares:
        # Ya es más reciente primero si la cabecera lo está (como en tu captura).
        for f, v in pares:
            msg += f"• {f}: *{v:.1f}* mm\n"
    else:
        # Fallback: sin cabecera real
        msg += "_No he podido leer la cabecera del PDF; uso los últimos 7 días estimados._\n"
        # Estimación con fecha del timestamp
        end_date_str = fecha_de_timestamp(timestamp)
        end_date = datetime.strptime(end_date_str, "%d/%m/%Y").date()
        fechas_est = [(end_date).strftime("%d/%m/%Y")]
        for d in range(1, n):
            fechas_est.append((end_date.replace()).strftime("%d/%m/%Y"))
        # mejor: generar con timedelta
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


# ================== TELEGRAM ==================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hola 👋\n"
        "Datos de lluvia en Huelma.\n\n"
        "/hoy  → lluvia diaria\n"
        "/siete → lluvia semanal\n"
        "/huelma → ambas"
    )


async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(obtener_hoy())


async def siete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(obtener_semanal())


async def huelma_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(obtener_hoy() + "\n\n" + obtener_semanal())


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
