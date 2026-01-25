import os
import re
from io import BytesIO
from datetime import datetime, timedelta

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
# PDF helpers
# =========================
def fetch_pdf_text(url: str) -> str:
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
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

def parse_updated_datetime_from_pdf_text(text: str) -> datetime | None:
    """
    Busca una marca de actualización dentro del texto del PDF.
    Típicos:
      - "Actualizados: 25/01/2026 15:11"
      - "Actualizado: 25/01/2026 15:11"
      - "Actualización: 25/01/2026 15:11"
    """
    t = normalize_text(text)

    m = re.search(
        r"(?i)Actualizad[oa]s?\s*:\s*(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2})",
        t
    )
    if not m:
        m = re.search(
            r"(?i)Actualizaci[oó]n\s*:\s*(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2})",
            t
        )
    if not m:
        return None

    try:
        return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d/%m/%Y %H:%M")
    except ValueError:
        return None

# =========================
# /HOY parsing
# =========================
def try_parse_hoy_row(text: str, place: str) -> dict | None:
    t = normalize_text(text)

    # Capturamos 7 números tras la estación (hour_actual, hour_prev, day_actual, day_prev, month_actual, month_prev, hydro_actual)
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
        f"📄 Lluvia HOY (actualizado: {updated_str})\n"
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
    updated_dt = parse_updated_datetime_from_pdf_text(text)
    parsed = try_parse_hoy_row(text, place)
    if not parsed:
        return (
            f"📄 Lluvia HOY\n"
            f"No pude extraer la fila de {place} del PDF."
        )
    return format_hoy(parsed, place, updated_dt)

# =========================
# /SEMANAL parsing (Huelma only)
# =========================
def find_line_for_place(text: str, place: str) -> str | None:
    """
    Busca una línea (o bloque lineal) que contenga la estación.
    En tu caso sale como: "P63 Huelma (JA) ..."
    """
    t = normalize_text(text)

    # A veces el extract_text “rompe” líneas raro; buscamos un segmento a partir de "Pxx Huelma"
    m = re.search(rf"(?im)^(P\d+\s+{re.escape(place)}\b.*)$", t)
    if m:
        return m.group(1).strip()

    # fallback: buscamos la primera aparición de "P.. Huelma" y recortamos hasta el final de línea o salto doble
    m = re.search(rf"(?is)(P\d+\s+{re.escape(place)}\b.*?)(?:\n\n|\nP\d+\s+|\Z)", t)
    if m:
        return m.group(1).strip()

    # fallback: buscar solo "Huelma" y recortar
    m = re.search(rf"(?is)({re.escape(place)}\b.*?)(?:\n\n|\Z)", t)
    if m:
        return m.group(1).strip()

    return None

def parse_weekly_from_line(line: str) -> tuple[list[float], float | None, float | None, float | None]:
    """
    De una línea tipo:
      P63 Huelma (JA) 19,1 5,2 29,3 7,5 5,2 0,0 0,0 66,3 98,9 325,2

    Interpreta:
      - 7 primeros: diarios (hoy, ayer, ...)
      - luego (si están): total semana, total mes, total año hidrológico
    """
    nums = re.findall(r"-?\d+(?:[.,]\d+)?", line)
    vals = [to_float(x) for x in nums]

    if len(vals) < 7:
        return [], None, None, None

    daily = vals[:7]

    total_sem = None
    total_mes = None
    total_hidro = None

    # Caso típico (como tu ejemplo): 7 + 3 totales => 10 o más
    # En tu captura salen 7 + 3 = 10, pero a veces hay algún campo extra, por eso pillamos “los últimos 3” tras los 7.
    if len(vals) >= 10:
        # asumimos: ... daily(7) ... total_sem total_mes total_hidro al final
        total_sem = vals[-3]
        total_mes = vals[-2]
        total_hidro = vals[-1]
    else:
        # si no hay totales, al menos total semana lo calculamos
        total_sem = sum(daily)

    return daily, total_sem, total_mes, total_hidro

def build_semanal_message(place: str) -> str:
    text = fetch_pdf_text(PDF_7DIAS_URL)
    updated_dt = parse_updated_datetime_from_pdf_text(text)
    updated_str = updated_dt.strftime("%d/%m/%Y %H:%M") if updated_dt else "no detectado"

    line = find_line_for_place(text, place)
    if not line:
        return (
            f"📄 Lluvia 7 días (actualizado: {updated_str})\n"
            f"{place}:\n"
            f"No encontré la fila de {place} en el PDF."
        )

    daily, total_sem, total_mes, total_hidro = parse_weekly_from_line(line)
    if not daily:
        return (
            f"📄 Lluvia 7 días (actualizado: {updated_str})\n"
            f"{place}:\n"
            f"No pude extraer los valores diarios.\n"
            f"Fila detectada:\n{line}"
        )

    # Generar fechas: hoy = fecha del PDF; si no existe, usamos fecha actual
    base_date = (updated_dt.date() if updated_dt else datetime.now().date())

    # daily[0] = hoy, daily[1].. = días anteriores
    lines = [f"📄 Lluvia 7 días (actualizado: {updated_str})", f"{place}:"]

    # Hoy
    hoy_label = base_date.strftime("%d/%m")
    lines.append(f"• Hoy ({hoy_label}): {daily[0]:.1f} mm")

    # Días anteriores
    for i in range(1, len(daily)):
        d = (base_date - timedelta(days=i)).strftime("%d/%m")
        lines.append(f"• {d}: {daily[i]:.1f} mm")

    # Totales
    if total_sem is not None:
        lines.append(f"• Total semana: {total_sem:.1f} mm")
    else:
        lines.append("• Total semana: (no detectado)")

    if total_mes is not None:
        lines.append(f"• Total mes: {total_mes:.1f} mm")
    else:
        lines.append("• Total mes: (no detectado)")

    if total_hidro is not None:
        lines.append(f"• Total año hidrológico: {total_hidro:.1f} mm")
    else:
        lines.append("• Total año hidrológico: (no detectado)")

    return "\n".join(lines)

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
