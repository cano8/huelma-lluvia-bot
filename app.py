import os
import re
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

TARGET_NAME = "Huelma"  # solo esto

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
# PDF extraction
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

def extract_block_around_place(text: str, place: str, before: int = 700, after: int = 1500) -> str:
    t = normalize_text(text)
    m = re.search(rf"(?i)\b{re.escape(place)}\b", t)
    if not m:
        return t[:2000]
    start = max(0, m.start() - before)
    end = min(len(t), m.end() + after)
    return t[start:end].strip()

def to_float(s: str) -> float:
    return float(s.replace(",", "."))

# =========================
# HOY parser (ya te funciona)
# =========================
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

def format_hoy(values: dict, place: str) -> str:
    updated = datetime.now().strftime("%d/%m/%Y %H:%M")
    return (
        f"📄 Lluvia HOY (consultado: {updated})\n"
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

    block = extract_block_around_place(text, place)
    updated = datetime.now().strftime("%d/%m/%Y %H:%M")
    return (
        f"📄 Lluvia HOY (consultado: {updated})\n"
        f"No pude aislar automáticamente los 7 valores para {place}.\n"
        f"Bloque detectado:\n\n{block}"
    )

# =========================
# SEMANAL parser (nuevo)
# =========================
def parse_weekly_for_place(text: str, place: str) -> tuple[list[tuple[str, float]], float | None, float | None, float | None]:
    """
    Devuelve:
      - pares (fecha_dd/mm, mm) para los días que aparezcan (en orden)
      - total_semana, total_mes, total_hidrologico (si se detectan)
    """
    block = extract_block_around_place(text, place)
    block_norm = normalize_text(block)

    # 1) Intentar detectar totales por etiquetas (si aparecen en el PDF)
    total_semana = None
    total_mes = None
    total_hidro = None

    # Variantes típicas (por si cambian acentos/espacios)
    m = re.search(r"(?i)total\s*(?:semana|7\s*d[ií]as)\s*[: ]\s*(-?\d+(?:[.,]\d+)?)", block_norm)
    if m:
        total_semana = to_float(m.group(1))

    m = re.search(r"(?i)total\s*mes\s*[: ]\s*(-?\d+(?:[.,]\d+)?)", block_norm)
    if m:
        total_mes = to_float(m.group(1))

    m = re.search(r"(?i)total\s*(?:año|ano)\s*hidrol[oó]gico\s*[: ]\s*(-?\d+(?:[.,]\d+)?)", block_norm)
    if m:
        total_hidro = to_float(m.group(1))

    # 2) Extraer pares (dd/mm -> mm) recorriendo tokens tras "Huelma"
    #    Esto funciona aunque el PDF venga “aplastado” en una línea.
    idx = re.search(rf"(?i)\b{re.escape(place)}\b", block_norm)
    if not idx:
        return [], total_semana, total_mes, total_hidro

    after = block_norm[idx.end():]

    tokens = re.split(r"[\s]+", after.strip())
    date_re = re.compile(r"^\d{2}/\d{2}$")
    num_re = re.compile(r"^-?\d+(?:[.,]\d+)?$")

    day_pairs: list[tuple[str, float]] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i].strip()

        if date_re.match(tok):
            # buscar el siguiente token numérico
            j = i + 1
            while j < len(tokens) and not num_re.match(tokens[j].strip()):
                j += 1
            if j < len(tokens):
                mm = to_float(tokens[j].strip())
                day_pairs.append((tok, mm))
                i = j + 1
                continue

        i += 1

    # 3) Si no detectamos totales por etiqueta, intentar inferirlos:
    #    Muchos PDFs ponen al final 3 columnas: semana/mes/año.
    #    Si hay números extra después de las fechas, los pillamos así:
    if total_semana is None or total_mes is None or total_hidro is None:
        # extraemos todos los números del bloque tras la estación
        nums_all = re.findall(r"-?\d+(?:[.,]\d+)?", after)
        nums_all = [to_float(x) for x in nums_all]

        # quitamos los ya usados en day_pairs (aprox) para no duplicar
        used = [mm for _, mm in day_pairs]
        remaining = []
        for x in nums_all:
            # comparación tolerante (por decimales)
            if any(abs(x - u) < 1e-6 for u in used):
                continue
            remaining.append(x)

        # si quedan al menos 3, asumimos que los 3 últimos son semana/mes/año hidro
        if len(remaining) >= 3:
            cand_sem, cand_mes, cand_hid = remaining[-3], remaining[-2], remaining[-1]
            if total_semana is None:
                total_semana = cand_sem
            if total_mes is None:
                total_mes = cand_mes
            if total_hidro is None:
                total_hidro = cand_hid

    return day_pairs, total_semana, total_mes, total_hidro

def build_semanal_message(place: str) -> str:
    text = fetch_pdf_text(PDF_7DIAS_URL)
    day_pairs, total_semana, total_mes, total_hidro = parse_weekly_for_place(text, place)

    updated = datetime.now().strftime("%d/%m/%Y %H:%M")
    lines = [f"📄 Lluvia 7 días (consultado: {updated})", f"{place}:"]

    if not day_pairs:
        block = extract_block_around_place(text, place)
        lines.append("No pude extraer bien la fila. Bloque detectado:")
        lines.append(block)
        return "\n".join(lines)

    # Primer día como "Hoy", el resto tal cual (24/01, 23/01, etc.)
    hoy_date, hoy_mm = day_pairs[0]
    lines.append(f"• Hoy ({hoy_date}): {hoy_mm:.1f} mm")
    for d, mm in day_pairs[1:]:
        lines.append(f"• {d}: {mm:.1f} mm")

    # Totales (si no salen, al menos no rompemos)
    if total_semana is not None:
        lines.append(f"• Total semana: {total_semana:.1f} mm")
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
# Commands
# =========================
def cmd_start(chat_id: int):
    tg_send_message(chat_id, "👋 Bot de lluvia (CHG)\n\nComandos:\n• /hoy\n• /semanal")

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
