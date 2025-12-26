import os
import io
import re
import logging
import threading
import asyncio
import sqlite3
from datetime import datetime, timedelta, time
from typing import Optional, List

import requests
import pdfplumber
from flask import Flask, request

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes

# ================== CONFIG ==================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID")
ADMIN_CHAT_ID_INT = int(ADMIN_CHAT_ID) if ADMIN_CHAT_ID and ADMIN_CHAT_ID.isdigit() else None

URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/tmp/LLuvia_7d%C3%ADas.pdf"

ESTACION_HUELMA_KEY = "Huelma"

# Envío semanal automático (domingo 20:00)
WEEKLY_SEND_DAY = 6
WEEKLY_SEND_HOUR = 20
WEEKLY_SEND_MINUTE = 0
WEEKLY_SEND_DAY_NAME = "domingo"
WEEKLY_SEND_TIME_STR = f"{WEEKLY_SEND_HOUR:02d}:{WEEKLY_SEND_MINUTE:02d}"

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
tg_ready = False

# ================== DB ==================
DB_PATH = "bot_stats.sqlite"


def db_connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def db_init():
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            username TEXT,
            command TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_subscriptions (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER NOT NULL,
            created_ts TEXT NOT NULL
        )
        """
    )
    con.commit()
    con.close()


def db_log_usage(chat_id: int, username: Optional[str], command: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO usage_events(ts, chat_id, username, command) VALUES (?, ?, ?, ?)",
        (datetime.utcnow().isoformat(timespec="seconds"), chat_id, username, command),
    )
    con.commit()
    con.close()


def db_set_weekly_subscription(chat_id: int, enabled: bool):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO weekly_subscriptions(chat_id, enabled, created_ts)
        VALUES (?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET enabled=excluded.enabled
        """,
        (chat_id, 1 if enabled else 0, datetime.utcnow().isoformat(timespec="seconds")),
    )
    con.commit()
    con.close()


def db_get_weekly_subscribers() -> List[int]:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT chat_id FROM weekly_subscriptions WHERE enabled=1")
    rows = cur.fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def db_is_subscribed(chat_id: int) -> bool:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT enabled FROM weekly_subscriptions WHERE chat_id=?", (chat_id,))
    row = cur.fetchone()
    con.close()
    return bool(row and int(row[0]) == 1)


def db_stats_summary(days: int = 30) -> dict:
    con = db_connect()
    cur = con.cursor()

    since = (datetime.utcnow() - timedelta(days=days)).isoformat(timespec="seconds")
    cur.execute("SELECT COUNT(*) FROM usage_events WHERE ts >= ?", (since,))
    total = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT command, COUNT(*) as c
        FROM usage_events
        WHERE ts >= ?
        GROUP BY command
        ORDER BY c DESC
        """,
        (since,),
    )
    by_cmd = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM weekly_subscriptions WHERE enabled=1")
    subs = int(cur.fetchone()[0])

    con.close()
    return {"total": total, "by_cmd": by_cmd, "subs": subs}


# ================== UTILIDADES ==================
def normalizar_fecha_ddmmyy_a_ddmmyyyy(ddmmyy: str) -> str:
    d, m, yy = ddmmyy.split("/")
    return f"{d}/{m}/{2000 + int(yy)}"


def extraer_timestamp(texto: str) -> Optional[str]:
    m = TS_RE.search(texto)
    if not m:
        return None
    fecha = m.group(1)
    hora = m.group(3)
    if len(fecha.split("/")[-1]) == 2:
        fecha = normalizar_fecha_ddmmyy_a_ddmmyyyy(fecha)
    return f"{fecha} {hora}"


def parsear_valores(linea: str) -> List[float]:
    partes = linea.replace(",", ".").split()
    return [float(t) for t in partes if re.fullmatch(r"-?\d+(\.\d+)?", t)]


def fecha_de_timestamp(ts: Optional[str]) -> str:
    return ts.split()[0] if ts else datetime.utcnow().strftime("%d/%m/%Y")


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
    header = "📄 *Lluvia diaria*"
    header += f" (actualizado: {timestamp})" if timestamp else ""
    if not linea:
        return header + "\n\nNo se han registrado precipitaciones en *Huelma* hoy."
    valores = parsear_valores(linea)
    return header + "\n\n" + ", ".join(f"{v:.1f} mm" for v in valores)


def formatear_semanal(timestamp: Optional[str], linea: Optional[str]) -> str:
    header = "📄 *Lluvia semanal*"
    header += f" (actualizado: {timestamp})" if timestamp else ""
    if not linea:
        return header + "\n\nNo hay datos para Huelma."
    valores = parsear_valores(linea)
    return header + "\n\n" + ", ".join(f"{v:.1f} mm" for v in valores)


def obtener_hoy() -> str:
    pdf = descargar_pdf(URL_HOY)
    texto = extraer_texto_pdf(pdf)
    return formatear_hoy(extraer_timestamp(texto), extraer_linea_estacion(texto, ESTACION_HUELMA_KEY))


def obtener_semanal() -> str:
    pdf = descargar_pdf(URL_7DIAS)
    texto = extraer_texto_pdf(pdf)
    return formatear_semanal(extraer_timestamp(texto), extraer_linea_estacion(texto, ESTACION_HUELMA_KEY))


# ================== TELEGRAM COMMANDS ==================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        user = update.effective_user
        username = user.username if user else None
        db_log_usage(chat_id, username, "/start")

        text = (
            "Hola 👋\n"
            "Datos de lluvia en Huelma.\n\n"
            "/hoy → lluvia diaria\n"
            "/semanal → lluvia semanal\n"
            f"/suscribir → recibir lluvia semanal cada {WEEKLY_SEND_DAY_NAME} a las {WEEKLY_SEND_TIME_STR}\n"
            "/cancelar → cancelar suscripción\n"
            "/estado → estado de suscripción\n"
            "/chatid → ver tu chat_id"
        )

        msg = update.effective_message
        if msg:
            await msg.reply_text(text, parse_mode="Markdown")
        else:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")

    except Exception as e:
        logger.exception("Error en /start: %s", e)


async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db_log_usage(update.effective_chat.id, update.effective_user.username, "/hoy")
    await update.effective_message.reply_markdown(obtener_hoy())


async def semanal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db_log_usage(update.effective_chat.id, update.effective_user.username, "/semanal")
    await update.effective_message.reply_markdown(obtener_semanal())


# ================== INIT TELEGRAM ==================
async def _tg_init_app():
    global tg_app, tg_ready
    db_init()
    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()

    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("hoy", hoy_cmd))
    tg_app.add_handler(CommandHandler("semanal", semanal_cmd))

    await tg_app.initialize()
    await tg_app.start()

    await tg_app.bot.set_my_commands(
        [
            BotCommand("start", "Ver ayuda"),
            BotCommand("hoy", "Lluvia de hoy"),
            BotCommand("semanal", "Lluvia semanal"),
        ]
    )

    tg_ready = True


def _tg_loop_runner():
    global tg_loop
    tg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(tg_loop)
    tg_loop.run_until_complete(_tg_init_app())
    tg_loop.run_forever()


def ensure_tg_thread():
    global tg_thread_started
    if not tg_thread_started:
        tg_thread_started = True
        threading.Thread(target=_tg_loop_runner, daemon=True).start()


# ================== FLASK ==================
@app.post("/webhook")
def webhook():
    ensure_tg_thread()
    if not tg_ready:
        return "not ready", 503

    update = Update.de_json(request.get_json(force=True), tg_app.bot)
    asyncio.run_coroutine_threadsafe(tg_app.process_update(update), tg_loop)
    return "ok", 200
