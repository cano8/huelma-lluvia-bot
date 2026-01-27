import os
import re
import sqlite3
from datetime import datetime
from io import BytesIO

import requests
from flask import Flask, request

# PDF parsing (paquete recomendado)
try:
    from pypdf import PdfReader
except Exception:  # fallback por si lo tienes con otro nombre
    from PyPDF2 import PdfReader  # type: ignore

# =========================
# Config (mínimo)
# =========================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("Falta TELEGRAM_TOKEN en variables de entorno.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "20"))

# URLs fijas (sin variables)
URL_HOY = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
URL_7DIAS = "https://www.chguadalquivir.es/saih/Informes/Lluvia7Dias.pdf"

TARGET_NAME = "Huelma"

# SQLite para usos
DB_PATH = os.environ.get("DB_PATH", "bot_stats.sqlite3")

app = Flask(__name__)

# =========================
# DB helpers
# =========================
def db_init():
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                username TEXT,
                command TEXT NOT NULL
            )
            """
        )
        con.commit()

def db_log_usage(chat_id: str, username: str | None, command: str):
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO usage (ts, chat_id, username, command) VALUES (?, ?, ?, ?)",
            (datetime.now().isoformat(timespec="seconds"), str(chat_id), username, command),
        )
        con.commit()

# Inicializa DB al importar (importante en gunicorn)
db_init()

# =========================
# Telegram helpers
# =========================
def tg_send_message(chat_id: int, text: str, reply_to_message_id: int | None = None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": None,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id

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

def get_username(update: dict) -> str | None:
    msg = update.get("message") or update.get("edited_message") or {}
    frm = msg.get("from") or {}
    return frm.get("username")

# =========================
# PDF helpers
# =========================
def download_pdf_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def pdf_to_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(pdf_bytes))
    parts = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        parts.append(txt)
    return "\n".join(parts)

def normalize_text(s: str) -> str:
    # normaliza espacios y separadores
    s = s.replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n", s)
    return s.strip()

def to_float(x: str) -> float:
    return float(x.replace(",", "."))

def extract_pdf_datetime(text: str) -> str | None:
    """
    Extrae fecha/hora que aparece en el PDF.
    Ejemplos típicos:
      26/01/2026 18:13
      26/01/26 19:00
    """
    t = normalize_text(text)

    m = re.search(r"\b(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2})\b", t)
    if m:
        return f"{m.group(1)} {m.group(2)}"

    m = re.search(r"\b(\d{2}/\d{2}/\d{2})\s+(\d{1,2}:\d{2})\b", t)
    if m:
        # deja tal cual (formato corto) porque así sale en el PDF de 7 días
        return f"{m.group(1)} {m.group(2)}"

    return None

# =========================
# HOY (lo dejamos como lo tenías conceptualmente)
# =========================
MONTHS_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
}

def parse_hoy_values_from_text(text: str, place: str) -> dict:
    """
    En Lluvia_Hoy.pdf, la fila suele tener (para el punto):
    HoraActual HoraAnterior DiaActual DiaAnterior MesActual MesAnterior AnioHidroActual
    """
    t = normalize_text(text)
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
    target = place.lower()

    # busca línea con el punto
    block = None
    for i, ln in enumerate(lines):
        if target in ln.lower():
            # concatena con la siguiente por si parte la fila
            block = ln
            if i + 1 < len(lines):
                block += " " + lines[i + 1]
            break
    if not block:
        raise ValueError(f"No encontré '{place}' en el PDF de hoy.")

    # quita prefijos tipo P63/E01 al principio si aparecen
    block = re.sub(r"^\s*(?:P\d+|[A-Z]\d{2})\b", "", block, flags=re.IGNORECASE).strip()

    nums = re.findall(r"-?\d+(?:[.,]\d+)?", block)
    if len(nums) < 7:
        raise ValueError(f"Fila encontrada pero sin 7 valores numéricos. Extraídos: {len(nums)}")

    vals = [to_float(x) for x in nums[:7]]
    return {
        "hour_actual": vals[0],
        "hour_prev": vals[1],
        "day_actual": vals[2],
        "day_prev": vals[3],
        "month_actual": vals[4],
        "month_prev": vals[5],
        "hydro_actual": vals[6],
    }

def format_hoy_message(updated_str: str | None, values: dict, place: str) -> str:
    up = updated_str or "no detectado"
    return "\n".join([
        f"📄 Lluvia HOY (actualizado: {up})",
        f"{place}:",
        f"• Día (actual): {values['day_actual']:.1f} mm",
        f"• Día (anterior): {values['day_prev']:.1f} mm",
        f"• Hora (actual): {values['hour_actual']:.1f} mm",
        f"• Hora (anterior): {values['hour_prev']:.1f} mm",
        f"• Mes (actual): {values['month_actual']:.1f} mm",
        f"• Mes (anterior): {values['month_prev']:.1f} mm",
        f"• Año hidrológico (actual): {values['hydro_actual']:.1f} mm",
    ])

def fetch_hoy(place: str) -> str:
    pdf_bytes = download_pdf_bytes(URL_HOY)
    text = pdf_to_text(pdf_bytes)
    updated_str = extract_pdf_datetime(text)
    values = parse_hoy_values_from_text(text, place)
    return format_hoy_message(updated_str, values, place)

# =========================
# SEMANAL (arreglado: ignora P63 y mapea bien columnas)
# =========================
def extract_week_dates_from_text(text: str) -> list[str]:
    """
    Saca las fechas de cabecera (25/01/26 24/01/26 ... 19/01/26).
    Devuelve lista en orden de aparición.
    """
    t = normalize_text(text)

    # en el PDF aparecen varias fechas; nos quedamos con el bloque de 7 días
    dates = re.findall(r"\b\d{2}/\d{2}/\d{2}\b", t)

    # típico: aparecen justo en la cabecera y en el cuerpo; filtramos quedándonos con una secuencia única
    uniq = []
    for d in dates:
        if d not in uniq:
            uniq.append(d)

    # normalmente necesitamos 7 fechas (día1..día7). Si hay más, cogemos las primeras 7 “razonables”.
    # En ese PDF suele ser: 25/01/26 24/01/26 ... 19/01/26 (7 items)
    return uniq[:7]

def find_place_row_11_values(text: str, place: str) -> list[float] | None:
    """
    Encuentra la fila del punto y extrae exactamente 11 valores:
      [DIA_ACTUAL] [D1] [D2] [D3] [D4] [D5] [D6] [D7] [TOTAL_7D] [TOTAL_MES] [TOTAL_HIDRO]
    FIX: ignora el código P63/E01 para no comerse el 63/01.
    """
    t = normalize_text(text)
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
    target = place.lower()

    idx = None
    for i, ln in enumerate(lines):
        if target in ln.lower():
            idx = i
            break
    if idx is None:
        return None

    # Concatena varias líneas por si la fila se parte
    block = lines[idx]
    for k in range(1, 4):
        if idx + k >= len(lines):
            break
        nxt = lines[idx + k]
        # si parece inicio de otra estación, corta
        if re.match(r"^(?:[A-Z]\d{2}|P\d+)\b", nxt, flags=re.IGNORECASE):
            break
        block += " " + nxt

    # FIX: elimina prefijo estación SOLO al inicio (P63/E01)
    block = re.sub(r"^\s*(?:P\d+|[A-Z]\d{2})\b", "", block, flags=re.IGNORECASE).strip()

    nums = re.findall(r"-?\d+(?:[.,]\d+)?", block)
    if len(nums) < 11:
        return None

    vals = [to_float(x) for x in nums[:11]]
    return vals

def format_semanal_message(updated_str: str | None, place: str, dates7: list[str], vals11: list[float]) -> str:
    # vals11 = [dia_actual, d1..d7, total7, total_mes, total_hidro]
    dia_actual = vals11[0]
    days = vals11[1:8]          # 7 valores
    total7 = vals11[8]
    total_mes = vals11[9]
    total_hidro = vals11[10]

    up = updated_str or "no detectado"

    lines = [
        f"📄 Lluvia 7 días (actualizado: {up})",
        f"{place}:",
        f"• Hoy (Día actual): {dia_actual:.1f} mm",
    ]

    # fechas7 corresponde a las columnas 25/01/26..19/01/26 (D1..D7)
    # Si no logramos fechas, mostramos “Día 1..7”
    if len(dates7) == 7:
        for d, v in zip(dates7, days):
            lines.append(f"• {d}: {v:.1f} mm")
    else:
        for i, v in enumerate(days, start=1):
            lines.append(f"• Día {i}: {v:.1f} mm")

    lines += [
        f"• Total semana (7 días): {total7:.1f} mm",
        f"• Total mes: {total_mes:.1f} mm",
        f"• Total año hidrológico: {total_hidro:.1f} mm",
    ]
    return "\n".join(lines)

def fetch_semanal(place: str) -> str:
    pdf_bytes = download_pdf_bytes(URL_7DIAS)
    text = pdf_to_text(pdf_bytes)

    updated_str = extract_pdf_datetime(text)
    dates7 = extract_week_dates_from_text(text)

    vals11 = find_place_row_11_values(text, place)
    if not vals11:
        raise ValueError(f"No pude extraer la fila de '{place}' en el PDF semanal.")

    return format_semanal_message(updated_str, place, dates7, vals11)

# =========================
# Commands
# =========================
def cmd_start(chat_id: int):
    text = (
        "👋 ¡Hola! Soy tu bot de lluvia.\n\n"
        "Comandos:\n"
        "• /hoy → lluvia (día/hora/mes/año hidrológico)\n"
        "• /semanal → lluvia últimos 7 días (solo Huelma)\n"
    )
    tg_send_message(chat_id, text)

def cmd_hoy(chat_id: int):
    msg = fetch_hoy(TARGET_NAME)
    tg_send_message(chat_id, msg)

def cmd_semanal(chat_id: int):
    msg = fetch_semanal(TARGET_NAME)
    tg_send_message(chat_id, msg)

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
    username = get_username(update)

    norm = text.strip()
    if norm.lower() in ("hoy", "semanal", "start"):
        norm = "/" + norm.lower()

    if norm.startswith("/"):
        cmd = norm.split()[0].lower()
        db_log_usage(str(chat_id), username, cmd)

    try:
        cmd = norm.split()[0].lower()

        if cmd == "/start":
            cmd_start(chat_id)
        elif cmd == "/hoy":
            cmd_hoy(chat_id)
        elif cmd in ("/semanal", "/siete"):
            cmd_semanal(chat_id)
        else:
            pass

    except Exception as e:
        tg_send_message(chat_id, f"Error: {type(e).__name__}: {e}")

    return "ok", 200

# Nota: en Render NO hace falta app.run; gunicorn importa app:app
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
