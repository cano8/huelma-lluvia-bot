import os
import re
import time
from io import BytesIO
from datetime import datetime

import requests
from flask import Flask, request
from pypdf import PdfReader

# =========================
# Config (solo TELEGRAM_TOKEN)
# =========================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("Falta TELEGRAM_TOKEN en variables de entorno (TELEGRAM_TOKEN).")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
HTTP_TIMEOUT = 25

PDF_HOY_URL = "https://www.chguadalquivir.es/saih/tmp/Lluvia_Hoy.pdf"
PDF_7DIAS_URL = "https://www.chguadalquivir.es/saih/Informes/Lluvia7Dias.pdf"

TARGET_NAME = "Huelma"

app = Flask(__name__)

# =========================
# Telegram helpers
# =========================
def tg_send_message(chat_id: int, text: str):
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
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
# PDF helpers
# =========================
def _requests_get_nocache(url: str) -> requests.Response:
    # Rompe caché: query param + headers
    bust = f"{url}{'&' if '?' in url else '?'}t={int(time.time())}"
    headers = {
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }
    r = requests.get(bust, headers=headers, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r

def fetch_pdf_text(url: str) -> str:
    r = _requests_get_nocache(url)
    reader = PdfReader(BytesIO(r.content))
    out = []
    for page in reader.pages:
        t = page.extract_text() or ""
        if t.strip():
            out.append(t)
    return "\n".join(out).strip()

def normalize_text(t: str) -> str:
    t = t.replace("\r", "\n")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def to_float(s: str) -> float:
    return float(s.replace(",", "."))

def parse_pdf_datetime_anywhere(text: str) -> datetime | None:
    """
    Extrae fecha/hora del PDF si aparece como:
      26/01/2026 18:13
      26/01/26 19:00
    """
    t = normalize_text(text)

    # primero año 4 dígitos
    m = re.search(r"\b(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2})\b", t)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d/%m/%Y %H:%M")
        except ValueError:
            pass

    # luego año 2 dígitos
    m = re.search(r"\b(\d{2}/\d{2}/\d{2})\s+(\d{1,2}:\d{2})\b", t)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d/%m/%y %H:%M")
        except ValueError:
            pass

    return None

# ==========================================================
# /HOY (NO TOCAR: lo dejo como estaba cuando te funcionaba)
# ==========================================================
def try_parse_hoy_row(text: str, place: str) -> dict | None:
    t = normalize_text(text)

    pattern = (
        rf"(?is)\b{re.escape(place)}\b.*?"
        r"(-?\d+(?:[.,]\d+)?).*?"
        r"(-?\d+(?:[.,]\d+)?).*?"
        r"(-?\d+(?:[.,]\d+)?).*?"
        r"(-?\d+(?:[.,]\d+)?).*?"
        r"(-?\d+(?:[.,]\d+)?).*?"
        r"(-?\d+(?:[.,]\d+)?).*?"
        r"(-?\d+(?:[.,]\d+)?)"
    )
    m = re.search(pattern, t)
    if not m:
        return None

    nums = [to_float(x) for x in m.groups()]
    return {
        "hour_actual": nums[0],
        "hour_prev": nums[1],
        "day_actual": nums[2],
        "day_prev": nums[3],
        "month_actual": nums[4],
        "month_prev": nums[5],
        "hydro_actual": nums[6],
    }

def format_hoy(values: dict, place: str, updated_dt: datetime | None) -> str:
    updated_str = updated_dt.strftime("%d/%m/%Y %H:%M") if updated_dt else "no detectado"
    return (
        f"📄 Lluvia HOY (consultado: {updated_str})\n"
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
    updated_dt = parse_pdf_datetime_anywhere(text)
    parsed = try_parse_hoy_row(text, place)
    if not parsed:
        updated_str = updated_dt.strftime("%d/%m/%Y %H:%M") if updated_dt else "no detectado"
        return f"📄 Lluvia HOY (consultado: {updated_str})\nNo pude extraer la fila de {place} del PDF."
    return format_hoy(parsed, place, updated_dt)

# =========================
# /SEMANAL (ARREGLADO)
# =========================
def extract_weekly_header_dates(text: str, updated_dt: datetime | None) -> list[str]:
    """
    Saca fechas de columna tipo: 25/01/26 24/01/26 ... (7 fechas)
    y construye la lista de etiquetas:
      ["Día actual", "25/01/26", ..., "19/01/26"]
    Si no se pueden sacar fechas, deja solo "Día actual" y luego D-1..D-7.
    """
    t = normalize_text(text)

    # En el PDF suelen aparecer en línea como "25/01/26 24/01/26 ..."
    dates = re.findall(r"\b\d{2}/\d{2}/\d{2}\b", t)
    # Nos quedamos con el primer bloque de 7 fechas consecutivas si existe.
    # (En la práctica, el PDF repite fechas pocas veces; esto suele funcionar)
    uniq = []
    for d in dates:
        if not uniq or uniq[-1] != d:
            uniq.append(d)

    # buscamos un tramo de 7 que tenga pinta de cabecera:
    # normalmente aparece justo después del título/tabla, así que usamos las primeras
    header_7 = uniq[:7] if len(uniq) >= 7 else []

    labels = ["Día actual"]
    if header_7:
        labels.extend(header_7)
        return labels

    # fallback si el texto no trae fechas claras
    if updated_dt:
        # genera 7 días previos como yy
        base = updated_dt
        for i in range(1, 8):
            d = (base.replace(hour=0, minute=0, second=0, microsecond=0) - (i * (base - base))).date()  # dummy safe
        # si no tenemos forma robusta sin timedelta aquí, mejor no inventar:
        pass

    labels.extend([f"D-{i}" for i in range(1, 8)])
    return labels

def find_place_row_numbers(text: str, place: str) -> list[float] | None:
    """
    Busca la fila de 'Huelma' y extrae 11 números:
      [día_actual] [d1]...[d7] [total_7d] [total_mes] [total_hidro]
    Importante: en PDF a veces la fila parte en 2 líneas, así que:
      - encuentra la línea que contiene 'Huelma'
      - concatena líneas siguientes hasta reunir >= 11 números
    """
    t = normalize_text(text)
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]

    # localiza índice de la línea con Huelma (ignorando may/min)
    idx = None
    for i, ln in enumerate(lines):
        if re.search(rf"\b{re.escape(place)}\b", ln, flags=re.IGNORECASE):
            idx = i
            break
    if idx is None:
        return None

    block = lines[idx]
    nums = re.findall(r"-?\d+(?:[.,]\d+)?", block)

    j = idx + 1
    while len(nums) < 11 and j < len(lines):
        # si detectamos inicio de otra estación, paramos
        # (en tus PDFs suelen ser códigos tipo E01/E02... o P63 etc)
        if re.match(r"^[A-Z]\d{2}\b", lines[j]) or re.match(r"^P\d+\b", lines[j]):
            break
        block += " " + lines[j]
        nums = re.findall(r"-?\d+(?:[.,]\d+)?", block)
        j += 1

    if len(nums) < 11:
        return None

    vals = [to_float(x) for x in nums[:11]]
    return vals

def build_semanal_message(place: str) -> str:
    text = fetch_pdf_text(PDF_7DIAS_URL)
    updated_dt = parse_pdf_datetime_anywhere(text)
    updated_str = updated_dt.strftime("%d/%m/%Y %H:%M") if updated_dt else "no detectado"

    vals = find_place_row_numbers(text, place)
    if not vals:
        return (
            f"📄 Lluvia 7 días (actualizado: {updated_str})\n"
            f"{place}:\n"
            f"No pude localizar/interpretar la fila de {place} en el PDF."
        )

    # vals: 0..7 (día actual + 7 fechas), 8 total7d, 9 total mes, 10 total hidro
    daily = vals[0:8]
    total_7d, total_mes, total_hidro = vals[8], vals[9], vals[10]

    # Etiquetas de fechas tomadas del PDF
    labels = extract_weekly_header_dates(text, updated_dt)  # ["Día actual", "25/01/26"...]
    # Asegura 8 etiquetas (día actual + 7)
    if len(labels) < 8:
        labels = (labels + [f"D-{i}" for i in range(1, 8)])[:8]

    out = [f"📄 Lluvia 7 días (actualizado: {updated_str})", f"{place}:"]

    # HOY = Día actual
    out.append(f"• Hoy ({labels[0]}): {daily[0]:.1f} mm")

    # Los demás días: usan las fechas reales del PDF (labels[1..7])
    for i in range(1, 8):
        out.append(f"• {labels[i]}: {daily[i]:.1f} mm")

    out.append(f"• Total semana: {total_7d:.1f} mm")
    out.append(f"• Total mes: {total_mes:.1f} mm")
    out.append(f"• Total año hidrológico: {total_hidro:.1f} mm")

    return "\n".join(out)

# =========================
# Routes
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

    text = get_message_text(update).strip()
    if text.lower() in ("hoy", "semanal", "start"):
        text = "/" + text.lower()

    try:
        cmd = text.split()[0].lower()
        if cmd == "/start":
            tg_send_message(chat_id, "👋 Bot de lluvia (CHG)\n\nComandos:\n• /hoy\n• /semanal")
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
