import os
import re
import json
import time
import sqlite3
from datetime import datetime, timedelta
from html import escape as html_escape

import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from flask import Flask, request

# =========================
# Config
# =========================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("Falta TELEGRAM_TOKEN en variables de entorno.")

# URL donde está la tabla "INFORMACIÓN PLUVIOMÉTRICA" con HORA / DÍA / MES / AÑO HIDROLÓGICO
PLUVO_URL = os.environ.get("PLUO_URL") or os.environ.get("PLUVO_URL", "").strip()

# URL de Informes.aspx (si quieres usarlo para semanal o enlaces)
INFORMES_URL = os.environ.get("INFORMES_URL", "").strip()

# Localidad objetivo
TARGET_NAME = os.environ.get("TARGET_NAME", "Huelma").strip()

# Username admin (sin @). En tu caso: Rc_8_8
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "Rc_8_8").lstrip("@").strip()

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# SQLite para registrar usos
DB_PATH = os.environ.get("DB_PATH", "bot_stats.sqlite3")

# Timeout requests
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "20"))

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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subs (
                chat_id TEXT PRIMARY KEY,
                username TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_ts TEXT NOT NULL
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

def db_usage_summary():
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM usage")
        total = cur.fetchone()[0]
        cur.execute("SELECT command, COUNT(*) FROM usage GROUP BY command ORDER BY COUNT(*) DESC")
        by_cmd = cur.fetchall()
        return total, by_cmd

# =========================
# Telegram helpers
# =========================
def tg_send_message(chat_id: int, text: str, reply_to_message_id: int | None = None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        # IMPORTANT: evitamos parse_mode para no volver a romper /start con caracteres raros
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
    u = frm.get("username")
    return u

def is_admin(username: str | None) -> bool:
    if not username:
        return False
    return username.lstrip("@") == ADMIN_USERNAME

# =========================
# Scraping helpers
# =========================
MONTHS_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
}

def parse_updated_datetime_from_html(html: str) -> datetime | None:
    """
    Busca textos tipo:
    'Actualizados: 28/12/2025 12:52'
    """
    m = re.search(r"Actualizados:\s*(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2})", html)
    if not m:
        return None
    date_s, time_s = m.group(1), m.group(2)
    try:
        return datetime.strptime(f"{date_s} {time_s}", "%d/%m/%Y %H:%M")
    except ValueError:
        return None

def find_huelma_row_values_from_pluvio_page(html: str, target_name: str) -> dict:
    """
    Espera encontrar una tabla donde en una fila aparezca el nombre del punto (Huelma)
    y en columnas cercanas existan valores para:
      - Hora Actual / Hora Anterior
      - Día Actual / Día Anterior
      - Mes Actual / Mes Anterior
      - Año Hidrológico Actual

    Devuelve dict con claves:
      hour_actual, hour_prev, day_actual, day_prev, month_actual, month_prev, hydro_actual
    """
    soup = BeautifulSoup(html, "html.parser")

    # Estrategia robusta:
    # 1) Buscamos cualquier tabla que contenga el texto del target.
    tables = soup.find_all("table")
    target_lower = target_name.strip().lower()

    for table in tables:
        text = table.get_text(" ", strip=True).lower()
        if target_lower not in text:
            continue

        # Recorremos filas
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if not tds:
                continue
            row_text = " ".join(td.get_text(" ", strip=True) for td in tds).lower()
            if target_lower not in row_text:
                continue

            # Intentamos extraer números (mm) de la fila en orden
            # Normalmente la fila tendrá: nombre + varios valores
            # Sacamos todos los floats que aparezcan (con coma o punto)
            raw = " ".join(td.get_text(" ", strip=True) for td in tds)
            nums = re.findall(r"(-?\d+(?:[.,]\d+)?)", raw)
            nums = [float(x.replace(",", ".")) for x in nums]

            # Si el sitio devuelve exactamente 7 valores (como en tu screenshot):
            # Hora actual, Hora anterior, Día actual, Día anterior, Mes actual, Mes anterior, Año hidrológico actual
            if len(nums) >= 7:
                return {
                    "hour_actual": nums[0],
                    "hour_prev": nums[1],
                    "day_actual": nums[2],
                    "day_prev": nums[3],
                    "month_actual": nums[4],
                    "month_prev": nums[5],
                    "hydro_actual": nums[6],
                }

            # Si cambia el layout, preferimos fallar claramente
            raise ValueError(
                f"Fila encontrada para '{target_name}', pero no pude extraer 7 valores numéricos. Encontrados: {len(nums)}"
            )

    raise ValueError(f"No encontré una tabla/fila para '{target_name}' en la página de pluviometría.")

def format_hoy_message(updated_dt: datetime | None, values: dict, place: str) -> str:
    """
    Formato final pedido:
    - primero día (actual y anterior): 28/12 y 27/12 (sin año)
    - luego hora (actual y anterior): 12h y 11h (sin minutos)
    - luego mes: 12-diciembre y 11-noviembre
    - año hidrológico: (actual) se queda como 'actual'
    """
    # Si no hay updated_dt, no podemos mapear etiquetas por fecha/hora: ponemos fallback legible
    if updated_dt is None:
        day_label_actual = "día (actual)"
        day_label_prev = "día (anterior)"
        hour_label_actual = "hora (actual)"
        hour_label_prev = "hora (anterior)"
        month_label_actual = "mes (actual)"
        month_label_prev = "mes (anterior)"
        updated_str = "no detectado"
    else:
        # Etiquetas con el mapeo que quieres
        dt_prev_h = updated_dt - timedelta(hours=1)
        dt_prev_d = updated_dt - timedelta(days=1)
        dt_prev_m = updated_dt - relativedelta(months=1)

        day_label_actual = updated_dt.strftime("%d/%m")
        day_label_prev = dt_prev_d.strftime("%d/%m")

        hour_label_actual = f"{updated_dt.hour}h"
        hour_label_prev = f"{dt_prev_h.hour}h"

        month_label_actual = f"{updated_dt.month:02d}-{MONTHS_ES[updated_dt.month]}"
        month_label_prev = f"{dt_prev_m.month:02d}-{MONTHS_ES[dt_prev_m.month]}"

        updated_str = updated_dt.strftime("%d/%m/%Y %H:%M")

    # Orden pedido: Día, Hora, Mes, Año hidrológico
    lines = [
        f"📄 Lluvia diaria (actualizado: {updated_str})",
        f"{place}:",
        f"• Día ({day_label_actual}): {values['day_actual']:.1f} mm",
        f"• Día ({day_label_prev}): {values['day_prev']:.1f} mm",
        f"• Hora ({hour_label_actual}): {values['hour_actual']:.1f} mm",
        f"• Hora ({hour_label_prev}): {values['hour_prev']:.1f} mm",
        f"• Mes ({month_label_actual}): {values['month_actual']:.1f} mm",
        f"• Mes ({month_label_prev}): {values['month_prev']:.1f} mm",
        f"• Año hidrológico (actual): {values['hydro_actual']:.1f} mm",
    ]
    return "\n".join(lines)

def fetch_hoy(place: str) -> str:
    if not PLUVO_URL:
        raise RuntimeError("Falta PLUVO_URL en variables de entorno (URL de la tabla de pluviometría).")

    r = requests.get(PLUVO_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    html = r.text

    updated_dt = parse_updated_datetime_from_html(html)
    values = find_huelma_row_values_from_pluvio_page(html, place)

    return format_hoy_message(updated_dt, values, place)

# NOTA: /semanal lo dejo como “passthrough” si ya lo tienes funcionando con otro endpoint.
# Aquí lo dejo simple: si tienes una URL directa para 7 días, ponla en WEEKLY_URL.
WEEKLY_URL = os.environ.get("WEEKLY_URL", "").strip()

def fetch_semanal(place: str) -> str:
    """
    Opción A (recomendada): define WEEKLY_URL que devuelva texto/tabla ya con los 7 días.
    Opción B: implementas aquí tu lógica actual (si me pegas tu app.py anterior, lo dejo idéntico).
    """
    if not WEEKLY_URL:
        # fallback (para no romper): devolvemos un mensaje claro
        return (
            "📄 Lluvia semanal\n"
            "Ahora mismo no tengo configurada la fuente para /semanal.\n"
            "Pon WEEKLY_URL en Render (una URL que devuelva los datos de 7 días) "
            "o pégame tu app.py actual y lo adapto conservando tu fuente."
        )

    r = requests.get(WEEKLY_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    txt = r.text.strip()

    # Lo envolvemos bonito
    updated_dt = datetime.now().strftime("%d/%m/%Y %H:%M")
    return f"📄 Lluvia semanal (actualizado: {updated_dt})\n{place}:\n{txt}"

# =========================
# Commands
# =========================
def cmd_start(chat_id: int):
    text = (
        "👋 ¡Hola! Soy tu bot de lluvia.\n\n"
        "Comandos:\n"
        "• /hoy → lluvia (día/hora/mes/año hidrológico)\n"
        "• /semanal → lluvia últimos 7 días\n"
        "• /estado → info del bot y suscripción\n\n"
        "Suscripción semanal: domingos a las 20:00."
    )
    tg_send_message(chat_id, text)

def cmd_estado(chat_id: int, username: str | None):
    total, by_cmd = db_usage_summary()
    lines = [
        "📌 Estado del bot",
        f"• Suscripción semanal: Domingo a las 20:00",
        f"• Total usos registrados: {total}",
    ]
    if is_admin(username):
        lines.append("")
        lines.append("📊 Usos por comando:")
        for cmd, cnt in by_cmd[:10]:
            lines.append(f"• {cmd}: {cnt}")
    else:
        lines.append("• (Detalle por comando solo visible para admin)")

    tg_send_message(chat_id, "\n".join(lines))

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

    # Normaliza: aceptar "hoy" "semanal" sin slash, pero la forma oficial es con /
    norm = text.strip()
    if norm.lower() in ("hoy", "semanal", "start", "estado"):
        norm = "/" + norm.lower()

    # Log de uso (solo si parece comando)
    if norm.startswith("/"):
        # comando puro (sin argumentos)
        cmd = norm.split()[0].lower()
        db_log_usage(chat_id, username, cmd)

    try:
        cmd = norm.split()[0].lower()

        if cmd == "/start":
            cmd_start(chat_id)
        elif cmd == "/hoy":
            cmd_hoy(chat_id)
        elif cmd in ("/semanal", "/siete"):
            cmd_semanal(chat_id)
        elif cmd == "/estado":
            cmd_estado(chat_id, username)
        else:
            # no respondemos a TODO para no ser pesado, pero puedes activar un help
            pass

    except Exception as e:
        # Mensaje de error controlado
        err = f"Error: {type(e).__name__}: {e}"
        tg_send_message(chat_id, err)

    return "ok", 200

# =========================
# Run (local)
# =========================
if __name__ == "__main__":
    db_init()
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

