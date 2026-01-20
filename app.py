# app.py
import os
import re
import io
import json
import time
import sqlite3
import logging
import tempfile
import threading
import asyncio
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from flask import Flask, request, abort

from PyPDF2 import PdfReader

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# =========================
# CONFIG
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
logger = logging.getLogger("bot")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("Falta TELEGRAM_TOKEN en variables de entorno.")

# (Opcional) protege el endpoint: /webhook/<WEBHOOK_SECRET>
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()  # recomendable
WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}" if WEBHOOK_SECRET else "/webhook"

BASE_SAIH = "https://www.chguadalquivir.es/saih/"
INFORMES_URL = urljoin(BASE_SAIH, "Informes.aspx")

# Si /hoy lo tienes ya estable por URL directa, déjalo aquí:
# (si algún día cambia, se puede adaptar igual que semanal)
URL_HOY_PDF_DIRECTO = urljoin(BASE_SAIH, "tmp/LLuvia_diaria.pdf")

ESTACION_HUELMA_KEY = "Huelma"  # cómo aparece en el PDF

DB_PATH = os.environ.get("DB_PATH", "bot.db")

# Timeouts
HTTP_TIMEOUT = 30

# =========================
# DB
# =========================
def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            user_id INTEGER,
            username TEXT,
            command TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            enabled INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def db_log_usage(user_id: int | None, username: str | None, command: str):
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO usage (ts, user_id, username, command) VALUES (?,?,?,?)",
            (int(time.time()), user_id, username, command),
        )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Error guardando uso en DB")

def db_get_usage_stats():
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM usage")
    total = cur.fetchone()["c"]

    cur.execute("SELECT command, COUNT(*) AS c FROM usage GROUP BY command ORDER BY c DESC")
    by_cmd = cur.fetchall()

    conn.close()
    return total, by_cmd

def db_get_subscription(user_id: int) -> bool:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT enabled FROM subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row and row["enabled"] == 1)

def db_set_subscription(user_id: int, username: str | None, enabled: bool):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO subscriptions (user_id, username, enabled)
        VALUES (?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            enabled=excluded.enabled
    """, (user_id, username, 1 if enabled else 0))
    conn.commit()
    conn.close()

# =========================
# HELPERS (PDF / PARSING)
# =========================
MESES_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
}

def safe_float(s: str) -> float:
    s = s.strip().replace(",", ".")
    return float(s)

def pdf_text_from_bytes(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    out = []
    for page in reader.pages:
        try:
            out.append(page.extract_text() or "")
        except Exception:
            out.append("")
    return "\n".join(out)

def extract_timestamp_from_text(text: str) -> datetime | None:
    """
    Intenta encontrar algo tipo:
    28/12/2025 12:52
    o 28/12/2025 9:55
    """
    m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2})", text)
    if not m:
        return None
    d, t = m.group(1), m.group(2)
    # normaliza hora 9:55 -> 09:55
    if len(t.split(":")[0]) == 1:
        t = "0" + t
    try:
        return datetime.strptime(f"{d} {t}", "%d/%m/%Y %H:%M")
    except ValueError:
        return None

def find_station_line_numbers(text: str, station_key: str) -> list[float]:
    """
    Busca la línea de la estación y devuelve una lista de floats encontrados.
    Para /hoy esperamos 7 valores:
      Hora(actual), Hora(anterior), Día(actual), Día(anterior),
      Mes(actual), Mes(anterior), Año Hidrológico(actual)
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    target = None

    # Busca línea que contenga exactamente el nombre (normalmente empieza por estación)
    for ln in lines:
        if station_key.lower() in ln.lower():
            # descartamos líneas tipo encabezados
            # y preferimos las que tengan números
            nums = re.findall(r"[-+]?\d+(?:[.,]\d+)?", ln)
            if len(nums) >= 5:
                target = ln
                break

    if not target:
        # fallback: intenta unir varias líneas alrededor
        for i, ln in enumerate(lines):
            if station_key.lower() in ln.lower():
                chunk = " ".join(lines[i:i+3])
                nums = re.findall(r"[-+]?\d+(?:[.,]\d+)?", chunk)
                if len(nums) >= 5:
                    target = chunk
                    break

    if not target:
        raise RuntimeError(f"No se encontró la fila de estación '{station_key}' en el PDF.")

    nums = re.findall(r"[-+]?\d+(?:[.,]\d+)?", target)
    vals = [safe_float(x) for x in nums]
    return vals

def format_hoy_message(ts: datetime | None, vals: list[float]) -> str:
    """
    vals esperados (por orden típico en CHG):
      0: hora_actual
      1: hora_anterior
      2: dia_actual
      3: dia_anterior
      4: mes_actual
      5: mes_anterior
      6: anno_hidro_actual
    """
    if len(vals) < 7:
        # si por extracción vienen más valores, intentamos coger los 7 primeros
        if len(vals) >= 7:
            vals = vals[:7]
        else:
            raise RuntimeError(f"Se esperaban >=7 valores en /hoy y llegaron {len(vals)}: {vals}")

    hora_act, hora_ant, dia_act, dia_ant, mes_act, mes_ant, ah_act = vals[:7]

    if ts is None:
        ts_txt = "no detectado"
        # sin timestamp no podemos poner etiquetas temporales; ponemos genéricas
        dia_act_lbl = "Día"
        dia_ant_lbl = "Día-1"
        hora_act_lbl = "Hora"
        hora_ant_lbl = "Hora-1"
        mes_act_lbl = "Mes"
        mes_ant_lbl = "Mes-1"
    else:
        ts_txt = ts.strftime("%d/%m/%Y %H:%M")

        # Día labels (sin año): 28/12
        d_act = ts.date()
        d_ant = (ts - timedelta(days=1)).date()
        dia_act_lbl = d_act.strftime("%d/%m")
        dia_ant_lbl = d_ant.strftime("%d/%m")

        # Hora labels (solo hora en formato "12h")
        h_act = ts.strftime("%H") + "h"
        h_ant = (ts - timedelta(hours=1)).strftime("%H") + "h"
        hora_act_lbl = h_act
        hora_ant_lbl = h_ant

        # Mes labels (12-diciembre / 11-noviembre)
        mes_num_act = ts.month
        mes_nom_act = MESES_ES.get(mes_num_act, str(mes_num_act))
        # mes anterior (maneja enero -> diciembre del año anterior)
        prev_month_dt = (ts.replace(day=1) - timedelta(days=1))
        mes_num_ant = prev_month_dt.month
        mes_nom_ant = MESES_ES.get(mes_num_ant, str(mes_num_ant))
        mes_act_lbl = f"{mes_num_act:02d}-{mes_nom_act}"
        mes_ant_lbl = f"{mes_num_ant:02d}-{mes_nom_ant}"

    # Orden pedido: día, hora, mes, año hidrológico
    msg = (
        f"📄 Lluvia diaria (actualizado: {ts_txt})\n"
        f"{ESTACION_HUELMA_KEY}:\n"
        f"• Día ({dia_act_lbl}): {dia_act:.1f} mm\n"
        f"• Día ({dia_ant_lbl}): {dia_ant:.1f} mm\n"
        f"• Hora ({hora_act_lbl}): {hora_act:.1f} mm\n"
        f"• Hora ({hora_ant_lbl}): {hora_ant:.1f} mm\n"
        f"• Mes ({mes_act_lbl}): {mes_act:.1f} mm\n"
        f"• Mes ({mes_ant_lbl}): {mes_ant:.1f} mm\n"
        f"• Año hidrológico (actual): {ah_act:.1f} mm"
    )
    return msg

# =========================
# DESCARGA PDFs
# =========================
def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; TelegramBot/1.0; +https://t.me/)",
        "Accept": "*/*",
    })
    return s

def download_pdf_direct(url: str) -> bytes:
    s = http_session()
    r = s.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "pdf" not in ctype and not r.content.startswith(b"%PDF"):
        # a veces devuelven HTML de error
        raise RuntimeError(f"Descarga no parece PDF (Content-Type={ctype}) desde {url}")
    return r.content

def download_pdf_from_informes(button_unique: str) -> bytes:
    """
    Simula el click de los botones de Informes.aspx.
    button_unique debe ser el NAME del input (ej: 'ctl00$ContentPlaceHolder1$But_Llu7dpdf')
    En ASP.NET, los image buttons envían también .x y .y
    """
    s = http_session()

    # 1) GET para capturar VIEWSTATE/EVENTVALIDATION
    r = s.get(INFORMES_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    html = r.text

    def get_hidden(name: str) -> str:
        m = re.search(rf'name="{re.escape(name)}"\s+id="{re.escape(name)}"\s+value="([^"]*)"', html)
        if not m:
            # a veces el id no coincide exactamente; intentamos por name solo
            m = re.search(rf'name="{re.escape(name)}"\s+value="([^"]*)"', html)
        if not m:
            raise RuntimeError(f"No encontré hidden field {name} en Informes.aspx")
        return m.group(1)

    viewstate = get_hidden("__VIEWSTATE")
    eventvalidation = get_hidden("__EVENTVALIDATION")
    viewstategen = None
    mgen = re.search(r'name="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', html)
    if mgen:
        viewstategen = mgen.group(1)

    # 2) POST simulando click image button
    # Para image button, el nombre real que viaja es:
    #   ctl00$ContentPlaceHolder1$But_Llu7dpdf.x
    #   ctl00$ContentPlaceHolder1$But_Llu7dpdf.y
    # y además se envían __VIEWSTATE, __EVENTVALIDATION
    data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
    }
    if viewstategen:
        data["__VIEWSTATEGENERATOR"] = viewstategen

    # coords del click (cualquier número vale)
    data[f"{button_unique}.x"] = "10"
    data[f"{button_unique}.y"] = "10"

    rp = s.post(INFORMES_URL, data=data, timeout=HTTP_TIMEOUT, allow_redirects=True)
    rp.raise_for_status()

    # Si responde PDF directo:
    ctype = (rp.headers.get("Content-Type") or "").lower()
    if "pdf" in ctype or rp.content.startswith(b"%PDF"):
        return rp.content

    # A veces devuelve HTML con un redirect o algo; intentamos seguir si hay Location
    # (requests ya sigue redirects, pero por si devuelven link dentro)
    if "text/html" in ctype:
        # intenta detectar un link .pdf
        mm = re.search(r'href="([^"]+\.pdf)"', rp.text, flags=re.IGNORECASE)
        if mm:
            pdf_url = urljoin(INFORMES_URL, mm.group(1))
            rr = s.get(pdf_url, timeout=HTTP_TIMEOUT)
            rr.raise_for_status()
            if rr.content.startswith(b"%PDF"):
                return rr.content

    raise RuntimeError("No pude obtener el PDF desde Informes.aspx (respuesta no-PDF).")

# =========================
# LOGICA DE COMANDOS
# =========================
def obtener_hoy() -> str:
    pdf = download_pdf_direct(URL_HOY_PDF_DIRECTO)
    text = pdf_text_from_bytes(pdf)
    ts = extract_timestamp_from_text(text)
    vals = find_station_line_numbers(text, ESTACION_HUELMA_KEY)

    # Algunas veces la extracción mete valores extra al final; intentamos usar los 7 primeros
    return format_hoy_message(ts, vals)

def obtener_semanal() -> str:
    # Botón PDF semanal según tu HTML:
    # name="ctl00$ContentPlaceHolder1$But_Llu7dpdf"
    pdf = download_pdf_from_informes("ctl00$ContentPlaceHolder1$But_Llu7dpdf")
    text = pdf_text_from_bytes(pdf)
    ts = extract_timestamp_from_text(text)
    ts_txt = ts.strftime("%d/%m/%Y %H:%M") if ts else "no detectado"

    # Para semanal, mantenemos el formato que ya te funcionaba.
    # Aquí hacemos una extracción "típica": buscamos la fila de Huelma y sacamos los valores diarios.
    # En tus capturas, ya lo tenías bien con bullets; lo replico:
    vals = find_station_line_numbers(text, ESTACION_HUELMA_KEY)

    # Heurística común: en semanal suele venir una lista de días + acumulados.
    # Si tu implementación anterior era distinta, sustituye esta parte por tu parser antiguo.
    #
    # Intento: coger los últimos 7 valores "diarios" del final de la fila.
    # Como no tenemos el layout exacto aquí, hacemos algo razonable:
    # - Tomamos todos los floats y mostramos los 7 primeros como “días” si hay muchos.
    # - y los dos últimos como acumulados mes/año si existen.
    #
    # Si tu PDF semanal es el clásico, esto suele cuadrar bien (pero si quieres 100% exacto,
    # dime cuántas columnas trae la fila de Huelma en tu semanal y lo fijo a medida).
    if len(vals) < 9:
        raise RuntimeError(f"En semanal esperaba más valores; recibí {len(vals)}: {vals}")

    # Normalmente: ... [d1..d7, mes, año_hidro] o similar
    # Cogemos 7 primeros como días (o 7 últimos antes de acumulados)
    # Intento robusto: asumimos que los 2 últimos son acumulados.
    acumulado_mes = vals[-2]
    acumulado_ah = vals[-1]
    diarios = vals[:-2]

    # si hay más de 7, nos quedamos con los últimos 7 (últimos días)
    if len(diarios) > 7:
        diarios_7 = diarios[-7:]
    else:
        diarios_7 = diarios

    # Etiquetas de días: si el timestamp existe, asignamos fecha hacia atrás
    lines = [f"📄 Lluvia semanal (actualizado: {ts_txt})", f"{ESTACION_HUELMA_KEY} – lluvia diaria (mm):"]
    if ts:
        d0 = ts.date()
        # diario_7 corresponde a últimos 7 días terminando en d0 (asumido)
        start = d0 - timedelta(days=len(diarios_7)-1)
        for i, v in enumerate(diarios_7):
            di = start + timedelta(days=i)
            lines.append(f"• {di.strftime('%d/%m/%Y')}: {v:.1f} mm")
    else:
        for i, v in enumerate(diarios_7, start=1):
            lines.append(f"• Día {i}: {v:.1f} mm")

    lines.append("")
    lines.append("Acumulados:")
    lines.append(f"• Mes actual: {acumulado_mes:.1f} mm")
    lines.append(f"• Año hidrológico: {acumulado_ah:.1f} mm")
    return "\n".join(lines)

# =========================
# TELEGRAM HANDLERS
# =========================
def log_cmd(update: Update, cmd: str):
    user = update.effective_user
    db_log_usage(
        user_id=user.id if user else None,
        username=(user.username if user else None),
        command=cmd,
    )

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_cmd(update, "/start")
    sub = db_get_subscription(update.effective_user.id)
    sub_txt = "✅ activada (domingo 20:00)" if sub else "❌ desactivada"

    text = (
        "Hola 👋\n"
        "Comandos disponibles:\n"
        "• /hoy → lluvia diaria (día/hora/mes/año hidrológico)\n"
        "• /semanal → lluvia últimos 7 días\n"
        "• /suscribir → recibe el resumen semanal (domingo 20:00)\n"
        "• /desuscribir → cancela la suscripción\n"
        "• /estado → estadísticas y estado\n\n"
        f"Suscripción semanal: {sub_txt}"
    )
    await update.message.reply_text(text)

async def hoy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_cmd(update, "/hoy")
    try:
        msg = obtener_hoy()
        await update.message.reply_text(msg)
    except Exception as e:
        logger.exception("Error en /hoy")
        await update.message.reply_text(f"Error en /hoy: {e}")

async def semanal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_cmd(update, "/semanal")
    try:
        msg = obtener_semanal()
        await update.message.reply_text(msg)
    except Exception as e:
        logger.exception("Error en /semanal")
        await update.message.reply_text(f"Error en /semanal: {e}")

async def suscribir_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_cmd(update, "/suscribir")
    user = update.effective_user
    db_set_subscription(user.id, user.username, True)
    await update.message.reply_text("✅ Suscripción activada. Te enviaré el resumen semanal los domingos a las 20:00.")

async def desuscribir_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_cmd(update, "/desuscribir")
    user = update.effective_user
    db_set_subscription(user.id, user.username, False)
    await update.message.reply_text("✅ Suscripción desactivada.")

async def estado_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_cmd(update, "/estado")
    total, by_cmd = db_get_usage_stats()
    sub = db_get_subscription(update.effective_user.id)

    lines = [
        "📊 Estado del bot",
        f"• Usos totales: {total}",
        "• Usos por comando:",
    ]
    for row in by_cmd:
        lines.append(f"  - {row['command']}: {row['c']}")

    lines.append("")
    lines.append("🗓️ Suscripción semanal:")
    lines.append("• Estado: " + ("✅ activada (domingo 20:00)" if sub else "❌ desactivada"))

    # Nota útil sobre “se queda tonto”
    lines.append("")
    lines.append("ℹ️ Nota: si Render duerme el servicio por inactividad, el primer mensaje puede tardar (cold start).")

    await update.message.reply_text("\n".join(lines))

# =========================
# TELEGRAM APP (async) + FLASK WEBHOOK
# =========================
tg_app: Application | None = None
tg_loop: asyncio.AbstractEventLoop | None = None

def run_telegram_loop():
    global tg_app, tg_loop
    tg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(tg_loop)

    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("hoy", hoy_cmd))
    tg_app.add_handler(CommandHandler("semanal", semanal_cmd))
    tg_app.add_handler(CommandHandler("suscribir", suscribir_cmd))
    tg_app.add_handler(CommandHandler("desuscribir", desuscribir_cmd))
    tg_app.add_handler(CommandHandler("estado", estado_cmd))

    async def _init():
        await tg_app.initialize()
        await tg_app.start()
        logger.info("Telegram Application iniciada.")

    tg_loop.run_until_complete(_init())
    tg_loop.run_forever()

app = Flask(__name__)

@app.get("/")
def index():
    return "OK"

@app.post(WEBHOOK_PATH)
def webhook():
    # si usas WEBHOOK_SECRET, esto protege el endpoint
    if not tg_app or not tg_loop:
        abort(503, "Bot not ready")

    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        abort(400, "Invalid JSON")

    try:
        update = Update.de_json(data, tg_app.bot)
        fut = asyncio.run_coroutine_threadsafe(tg_app.process_update(update), tg_loop)
        # No esperamos mucho: Telegram webhook tiene tiempos; respondemos rápido
        _ = fut
    except Exception as e:
        logger.exception("Error procesando update")
        abort(500, str(e))

    return "OK"

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    db_init()

    t = threading.Thread(target=run_telegram_loop, daemon=True)
    t.start()

    # Render usa PORT
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
