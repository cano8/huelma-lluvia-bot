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
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID")  # pon aquí tu chat_id para /stats
ADMIN_CHAT_ID_INT = int(ADMIN_CHAT_ID) if ADMIN_CHAT_ID and ADMIN_CHAT_ID.isdigit() else None

URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/tmp/LLuvia_7d%C3%ADas.pdf"

ESTACION_HUELMA_KEY = "Huelma"

# Suscripción semanal: Domingo 20:00
# 0=Lunes ... 6=Domingo
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
TS_RE = re.compile(r"\b(\d{2}/\d{2}/(\d{2}|\d{4}))\s+(\d{1,2}:\d{2})\b")
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
    return {"total": total, "by_cmd": by_cmd, "subs": subs, "days": days}


# ================== UTILIDADES ==================
def normalizar_fecha_ddmmyy_a_ddmmyyyy(ddmmyy: str) -> str:
    d, m, yy = ddmmyy.split("/")
    yyyy = 2000 + int(yy)
    return f"{d}/{m}/{yyyy}"


def extraer_timestamp(texto: str) -> Optional[str]:
    m = TS_RE.search(texto)
    if not m:
        return None

    fecha = m.group(1)
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


def proximo_envio_semanal_utc(now_utc: Optional[datetime] = None) -> datetime:
    now = now_utc or datetime.utcnow()
    target = now.replace(hour=WEEKLY_SEND_HOUR, minute=WEEKLY_SEND_MINUTE, second=0, microsecond=0)
    days_ahead = (WEEKLY_SEND_DAY - target.weekday()) % 7
    candidate = target + timedelta(days=days_ahead)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate


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
    Lee la cabecera real del PDF: "Día actual" + "09/12/25"...
    Devuelve lista dd/mm/yyyy en el orden del PDF (normalmente más reciente -> más antiguo).
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
    header += f" (actualizado: {timestamp})" if timestamp else " (actualizado: no detectado)"

    if not linea:
        return header + "\n\nNo se han registrado precipitaciones en *Huelma* hoy."

    valores = parsear_valores(linea)

    # En el PDF de "hoy" la tabla suele ser:
    # HORA: Actual, Anterior
    # DÍA:  Actual, Anterior
    # MES:  Actual, Anterior
    # AÑO HIDROLÓGICO: Actual
    # => 7 valores
    etiquetas = [
        "Hora (actual)",
        "Hora (anterior)",
        "Día (actual)",
        "Día (anterior)",
        "Mes (actual)",
        "Mes (anterior)",
        "Año hidrológico (actual)",
    ]

    msg = header + "\n*Huelma*:\n"

    if len(valores) >= 7:
        for lab, v in zip(etiquetas, valores[:7]):
            msg += f"• {lab}: *{v:.1f}* mm\n"
        return msg.strip()

    # Fallback si el PDF cambia y no vienen 7 valores
    if valores:
        msg += "Valores detectados (mm): " + ", ".join(f"{v:.1f}" for v in valores)
    else:
        msg += "He encontrado la fila, pero no he podido extraer valores numéricos."
    return msg



def formatear_semanal(timestamp: Optional[str], fechas_cols: Optional[List[str]], linea: Optional[str]) -> str:
    header = "📄 *Lluvia semanal*"
    header += f" (actualizado: {timestamp})" if timestamp else " (actualizado: no detectado)"

    if not linea:
        return header + "\n\nNo encuentro la fila de *Huelma* en el PDF."

    valores = parsear_valores(linea)
    if len(valores) < 7:
        return header + "\n\nNo hay suficientes valores para mostrar la semana."

    # acumulados (los dos últimos números)
    mes_actual = valores[-2] if len(valores) >= 2 else None
    anio_hidrologico = valores[-1] if len(valores) >= 1 else None

    # columnas según cabecera real del PDF
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
        # El PDF suele estar en orden más reciente -> más antiguo (eso es lo que tú quieres)
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
    """
    Start robusto: sin Markdown para evitar errores de parseo.
    """
    try:
        chat_id = update.effective_chat.id
        user = update.effective_user
        username = user.username if user else None
        db_log_usage(chat_id, username, "/start")

        text = (
            "Hola 👋\n"
            "Te muestro los datos de lluvia en Huelma. Estos datos se extraen de la Confederación Hidrográfica del Guadalquivir (CHG). Los comandos disponibles son los siguientes:\n\n"
            "/hoy  → lluvia hoy\n"
            "/semanal → lluvia semanal\n"
            f"/suscribir → recibir datos de lluvia semanal cada {WEEKLY_SEND_DAY_NAME} a las {WEEKLY_SEND_TIME_STR}\n"
            "/cancelar → cancelar suscripción\n"
            "/estado → ver estado de suscripción"
        )

        msg = update.effective_message
        if msg:
            await msg.reply_text(text)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text)

    except Exception as e:
        logger.exception("Error en /start: %s", e)
        try:
            if update.effective_chat:
                await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Error en /start: {e}")
        except Exception:
            pass



async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/hoy")
    await update.effective_message.reply_markdown(obtener_hoy())


async def semanal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/semanal")
    await update.effective_message.reply_markdown(obtener_semanal())


# Alias opcional para no romper /siete
async def siete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/siete(alias)")
    await update.effective_message.reply_markdown(obtener_semanal())


async def suscribir_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/suscribir")
    db_set_weekly_subscription(chat_id, True)
    await update.effective_message.reply_text(
        f"✅ Suscripción semanal activada.\nTe enviaré *Lluvia semanal* cada {WEEKLY_SEND_DAY_NAME} a las {WEEKLY_SEND_TIME_STR}.",
        parse_mode="Markdown",
    )


async def cancelar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/cancelar")
    db_set_weekly_subscription(chat_id, False)
    await update.effective_message.reply_text("🛑 Suscripción semanal desactivada.")


async def estado_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/estado")

    sub = db_is_subscribed(chat_id)
    proximo = proximo_envio_semanal_utc()

    estado = "✅ Activada" if sub else "⛔ Desactivada"
    await update.effective_message.reply_text(
        "📌 *Estado*\n"
        f"• Suscripción semanal: {estado}\n"
        f"• Envío: cada {WEEKLY_SEND_DAY_NAME} a las {WEEKLY_SEND_TIME_STR}\n"
        f"• Próximo envío (UTC): {proximo.strftime('%d/%m/%Y %H:%M')}",
        parse_mode="Markdown",
    )


async def chatid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/chatid")
    await update.effective_message.reply_text(f"Tu chat_id es: {chat_id}")


def _is_admin(chat_id: int) -> bool:
    return ADMIN_CHAT_ID_INT is not None and chat_id == ADMIN_CHAT_ID_INT


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username if update.effective_user else None
    db_log_usage(chat_id, username, "/stats")

    if not _is_admin(chat_id):
        await update.effective_message.reply_text("Este comando es solo para el administrador.")
        return

    s = db_stats_summary(days=30)
    lines = [
        f"📊 *Uso del bot (últimos {s['days']} días)*",
        f"• Total: *{s['total']}*",
        f"• Suscriptores activos: *{s['subs']}*",
        "",
        "*Por comando:*",
    ]
    for cmd, c in s["by_cmd"][:12]:
        lines.append(f"• {cmd}: {c}")

    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


# ================== JOB SEMANAL ==================
async def weekly_job(context: ContextTypes.DEFAULT_TYPE):
    subs = db_get_weekly_subscribers()
    if not subs:
        return

    msg = obtener_semanal()
    for chat_id in subs:
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
            db_log_usage(chat_id, None, "AUTO_WEEKLY")
        except Exception:
            logger.exception("No pude enviar semanal a chat_id=%s", chat_id)


# ================== TELEGRAM INIT (loop persistente) ==================
async def _tg_post_init(application: Application):
    # Menú de comandos
    await application.bot.set_my_commands(
        [
            BotCommand("hoy", "Lluvia registrada hoy en Huelma"),
            BotCommand("semanal", "Lluvia semanal (por días)"),
            BotCommand("suscribir", f"Recibir la lluvia semanal cada {WEEKLY_SEND_DAY_NAME}"),
            BotCommand("cancelar", "Cancelar la suscripción semanal"),
            BotCommand("estado", "Ver estado de la suscripción"),
            BotCommand("chatid", "Ver tu chat_id"),
            BotCommand("start", "Ver ayuda"),
        ]
    )

    # JobQueue opcional: si no existe, NO tiramos el bot
    if application.job_queue is None:
        logger.warning("JobQueue no disponible: el bot funciona, pero NO enviará mensajes automáticos.")
        return

    async def _weekly_wrapper(ctx: ContextTypes.DEFAULT_TYPE):
        # Día en UTC (si quieres hora Madrid exacta, lo ajusto con zoneinfo)
        if datetime.utcnow().weekday() == WEEKLY_SEND_DAY:
            await weekly_job(ctx)

    application.job_queue.run_daily(
        _weekly_wrapper,
        time=time(WEEKLY_SEND_HOUR, WEEKLY_SEND_MINUTE),
        name="weekly_subscriptions",
    )
    logger.info("Job semanal programado.")


async def _tg_init_app():
    global tg_app, tg_ready

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN no configurado")

    db_init()

    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()

    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("hoy", hoy_cmd))
    tg_app.add_handler(CommandHandler("semanal", semanal_cmd))
    tg_app.add_handler(CommandHandler("siete", siete_cmd))  # alias
    tg_app.add_handler(CommandHandler("suscribir", suscribir_cmd))
    tg_app.add_handler(CommandHandler("cancelar", cancelar_cmd))
    tg_app.add_handler(CommandHandler("estado", estado_cmd))
    tg_app.add_handler(CommandHandler("chatid", chatid_cmd))
    tg_app.add_handler(CommandHandler("stats", stats_cmd))

    await tg_app.initialize()
    await tg_app.start()
    await _tg_post_init(tg_app)

    tg_ready = True
    logger.info("Telegram Application lista (ready=True).")


def _tg_loop_runner():
    global tg_loop, tg_ready
    try:
        tg_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(tg_loop)
        tg_loop.run_until_complete(_tg_init_app())
        tg_loop.run_forever()
    except Exception:
        tg_ready = False
        logger.exception("Fallo arrancando el loop de Telegram (el bot no quedará listo).")


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
    return "OK", 200


@app.post("/webhook")
@app.post("/webhook/")
def webhook():
    try:
        ensure_tg_thread()

        if tg_loop is None or tg_app is None or not tg_ready:
            return "not ready", 503

        update_json = request.get_json(force=True)
        update = Update.de_json(update_json, tg_app.bot)

        fut = asyncio.run_coroutine_threadsafe(tg_app.process_update(update), tg_loop)
        fut.result(timeout=20)

    except Exception:
        logger.exception("Error procesando update")
        return "ok", 200

    return "ok", 200



