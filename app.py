import os
import re
from io import BytesIO
from datetime import datetime

import requests
from flask import Flask, request
from PyPDF2 import PdfReader

# =========================
# Config (solo TELEGRAM_TOKEN)
# =========================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("Falta TELEGRAM_TOKEN en variables de entorno.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
HTTP_TIMEOUT = 25

PDF_HOY_URL = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
PDF_7DIAS_URL = "https://www.chguadalquivir.es/saih/Informes/Lluvia7Dias.pdf"

TARGET_NAME = "Huelma"  # cámbialo si quieres

app = Flask(__name__)

# =========================
# Telegram helpers
# =========================
def tg_send_message(chat_id: int, text: str):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_message_text(update: dict) -> str:
    msg = update.get("message") or update.get("edited_message") or {}
    return (msg.get("text") or "").strip()

def get_chat_id(update: dict) -> int | None:
    msg = update.get("message") or update.get("edited_message") or {}
    chat = msg.get("chat") or {}
    return chat.get("id")

# =========================
# PDF extraction helpers
# =========================
def fetch_pdf_text(url: str) -> str:
    """
    Lee el PDF directamente de la URL (en memoria) y extrae texto con PyPDF2.
    """
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()

    reader = PdfReader(BytesIO(r.content))
    parts = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        if txt.strip():
            parts.append(txt)

    return "\n".join(parts).strip()

def normalize_text(t: str) -> str:
    # Reduce ruido típico de extracción de PDF
    t = t.replace("\r", "\n")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def extract_block_around_place(text: str, place: str, before: int = 500, after: int = 900) -> str:
    """
    Devuelve un bloque alrededor del nombre del lugar. Sirve como fallback.
    """
    t = normalize_text(text)
    m = re.search(rf"(?i)\b{re.escape(place)}\b", t)
    if not m:
        return t[:1400]  # fallback para ver qué llega
    start = max(0, m.start() - before)
    end = min(len(t), m.end() + after)
    return t[start:end].strip()

def try_parse_hoy_row(text: str, place: str) -> dict | None:
    """
    Intenta extraer 7 valores numéricos típicos de la tabla de HOY:
    Hora actual, Hora anterior, Día actual, Día anterior, Mes actual, Mes anterior, Año hidrológico
    Devuelve dict o None si no lo consigue.
    """
    t = normalize_text(text)

    # Buscamos una “línea” (o bloque) que contenga el lugar y números cerca.
    # Como en PDFs los saltos de línea son raros, permitimos que haya \n entre medias.
    # Capturamos 7 números con coma o punto.
    pattern = rf"(?is)\b{re.escape(place)}\b.*?(-?\d+(?:[.,]\d+)?).*?(-?\d+(?:[.,]\d+)?).*?(-?\d+(?:[.,]\d+)?).*?(-?\d+(?:[.,]\d+)?).*?(-?\d+(?:[.,]\d+)?).*?(-?\d+(?:[.,]\d+)?).*?(-?\d+(?:[.,]\d+)?)"
    m = re.search(pattern, t)
    if not m:
        return None

    nums = [float(x.replace(",", ".")) for x in m.groups()]
    return {
        "hour_actual": nums[0],
        "hour_prev": nums[1],
        "day_actual": nums[2],
        "day_prev": nums[3],
        "month_actual": nums[4],
        "month_prev": nums[5],
        "hydro_actual": nums[6],
    }

def format_hoy(values: dict, place: str) -> str:
    updated = datetime.now().strftime("%d/%m/%Y %H:%M")
    return (
        f"📄 Lluvia HOY (CHG) (consultado: {updated})\n"
        f"{place}:\n"
        f"• Día (actual): {values['day_actual']:.1f} mm\n"
        f"• Día (anterior): {values['day_prev']:.1f} mm\n"
        f"• Hora (actual): {values['hour_actual']:.1f} mm\n"
        f"• Hora (anterior): {values['hour_prev']:.1f} mm\n"
        f"• Mes (actual): {values['month_actual']:.1f} mm\n"
        f"• Mes (anterior): {values['month_prev']:.1f} mm\n"
        f"• Año hidrológico (actual): {values['hydro_actual']:.1f} mm"
    )

def build_hoy_message(place: str) -> str:
    text = fetch_pdf_text(PDF_HOY_URL)
    parsed = try_parse_hoy_row(text, place)
    if parsed:
        return format_hoy(parsed, place)

    # Fallback: bloque alrededor para poder ajustar el parser si el PDF viene distinto
    block = extract_block_around_place(text, place)
    updated = datetime.now().strftime("%d/%m/%Y %H:%M")
    return (
        f"📄 Lluvia HOY (CHG) (consultado: {updated})\n"
        f"No pude aislar la fila numérica automáticamente. Te muestro el bloque encontrado:\n\n"
        f"{block}"
    )

def build_semanal_message(place: str) -> str:
    text = fetch_pdf_text(PDF_7DIAS_URL)
    block = extract_block_around_place(text, place, before=700, after=1200)
    updated = datetime.now().strftime("%d/%m/%Y %H:%M")
    return (
        f"📄 Lluvia 7 días (CHG) (consultado: {updated})\n"
        f"{place}:\n"
        f"{block}"
    )

# =========================
# Commands
# =========================
def cmd_start(chat_id: int):
    tg_send_message(
        chat_id,
        "👋 Bot de lluvia (CHG)\n\nComandos:\n• /hoy\n• /semanal"
    )

# =========================
# Flask routes
# =========================
@app.route("/", methods=["GET"])
def health():
    return "ok", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    update = request.get_json(force=True, silent=False)

    chat_id = get_chat_id(update)
    if chat_id is None:
        return "no chat", 200

    text = get_message_text(update)
    norm = text.strip()
    if norm.lower() in ("hoy", "semanal", "start"):
        norm = "/" + norm.lower()

    try:
        cmd = norm.split()[0].lower()

        if cmd == "/start":
            cmd_start(chat_id)
        elif cmd == "/hoy":
            tg_send_message(chat_id, build_hoy_message(TARGET_NAME))
        elif cmd in ("/semanal", "/siete"):
            tg_send_message(chat_id, build_semanal_message(TARGET_NAME))
        else:
            pass

    except Exception as e:
        tg_send_message(chat_id, f"Error: {type(e).__name__}: {e}")

    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
