"""
SMS Campaign & CRM Dashboard — FastAPI Backend
================================================
Full-featured backend with SQLite persistence, background SMS sending engine,
SSE real-time streaming, and REST API for the single-page dashboard.
"""

import sqlite3
import json
import threading
import time
import random
import re
import csv
import io
import os
import uuid
import socket
from datetime import datetime, date
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, List

from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import httpx

# ═══════════════════════════════════════════════════════════════════════════════
# APP SETUP & LOCAL IP
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="SMS Campaign & CRM Dashboard")
templates = Jinja2Templates(directory="templates")
DB_PATH = "crm_sms.db"


def get_local_ip() -> str:
    """Detect local LAN IP address for Android gateway connectivity."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        # Connecting to a public UDP address selects the active network interface
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                gateway_mode TEXT DEFAULT 'custom_android',
                gateway_url TEXT DEFAULT 'http://192.168.1.105:8080',
                api_key TEXT DEFAULT '',
                device_id TEXT DEFAULT '',
                username TEXT DEFAULT '',
                password TEXT DEFAULT '',
                sim_slot TEXT DEFAULT '1',
                min_delay INTEGER DEFAULT 15,
                max_delay INTEGER DEFAULT 35,
                daily_limit INTEGER DEFAULT 500
            );

            INSERT OR IGNORE INTO config (id) VALUES (1);

            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL,
                name TEXT DEFAULT '',
                session_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sms_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                name TEXT DEFAULT '',
                message TEXT NOT NULL,
                status TEXT NOT NULL,
                error_msg TEXT DEFAULT '',
                sim_used TEXT DEFAULT '',
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS campaigns (
                id TEXT PRIMARY KEY,
                total INTEGER DEFAULT 0,
                sent INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'idle',
                message_template TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );
        """)
        # Ensure device_id column exists
        c.execute("PRAGMA table_info(config)")
        cols = [col[1] for col in c.fetchall()]
        if "device_id" not in cols:
            c.execute("ALTER TABLE config ADD COLUMN device_id TEXT DEFAULT ''")

        # Set default to custom_android if local or empty
        c.execute("UPDATE config SET gateway_mode = 'custom_android' WHERE id = 1 AND (gateway_mode = 'local' OR gateway_mode = '' OR gateway_mode IS NULL)")
        conn.commit()


init_db()


# ═══════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ═══════════════════════════════════════════════════════════════════════════════
class ConfigModel(BaseModel):
    gateway_mode: str = "custom_android"
    gateway_url: str = "http://192.168.1.105:8080"
    api_key: str = ""
    device_id: str = ""
    username: str = ""
    password: str = ""
    sim_slot: str = "1"
    min_delay: int = 15
    max_delay: int = 35
    daily_limit: int = 500


class MobileReportModel(BaseModel):
    task_id: str
    phone: str
    status: str
    error: Optional[str] = ""


class PasteContactsModel(BaseModel):
    text: str
    session_id: str = ""


class StartCampaignModel(BaseModel):
    message: str
    session_id: str


class TestSmsModel(BaseModel):
    phone: str
    message: str = "Test SMS — Dashboard tekshiruvi ✅"
    device_id: Optional[str] = ""
    api_key: Optional[str] = ""
    gateway_mode: Optional[str] = ""
    gateway_url: Optional[str] = ""


# ═══════════════════════════════════════════════════════════════════════════════
# PHONE NUMBER & GATEWAY UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
def extract_device_id(device_id_raw: str, gateway_url_raw: str = "") -> str:
    """Extract clean TextBee device ID even if full URL or path was provided."""
    raw = (device_id_raw or "").strip()
    if not raw:
        raw = (gateway_url_raw or "").strip()
    if not raw:
        return ""
    m = re.search(r'/devices/([a-zA-Z0-9_\-]+)', raw)
    if m:
        return m.group(1).strip()
    if "textbee.dev" in raw or "http://" in raw or "https://" in raw:
        clean = raw.split("?")[0].rstrip("/").replace("/send-sms", "")
        parts = clean.split("/")
        if parts:
            return parts[-1].strip()
    clean = raw.rstrip("/").replace("/send-sms", "").strip()
    if "/" in clean:
        clean = clean.split("/")[-1].strip()
    return clean


def clean_phone(raw: str) -> Optional[str]:
    """Standardize phone number to international format (default +998 for Uzbekistan)."""
    raw = raw.strip()
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None
    if digits.startswith("998") and len(digits) == 12:
        return f"+{digits}"
    if len(digits) == 9 and digits[0] in "123456789":
        return f"+998{digits}"
    if digits.startswith("8") and len(digits) == 10:
        return f"+998{digits[1:]}"
    if raw.startswith("+") and 8 <= len(digits) <= 15:
        return f"+{digits}"
    if 10 <= len(digits) <= 15:
        return f"+{digits}"
    return None


def parse_contacts_text(text: str) -> list:
    """Parse phone numbers (and optional names) from raw text."""
    results = []
    seen = set()
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r"[,;\t]+", line, maxsplit=1)
        raw_phone = parts[0].strip()
        name = parts[1].strip() if len(parts) > 1 else ""
        phone = clean_phone(raw_phone)
        if phone and phone not in seen:
            seen.add(phone)
            results.append((phone, name))
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# CAMPAIGN ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
class CampaignEngine:
    """Background SMS sending engine with pause/resume/stop controls."""

    def __init__(self):
        self.status = "idle"
        self.total = 0
        self.sent = 0
        self.failed = 0
        self.pending = 0
        self.campaign_id = None
        self.message_template = ""
        self.contacts = []
        self.logs = []
        self.progress = 0.0
        self.next_send_countdown = 0
        self.current_phone = ""
        self.lock = threading.RLock()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._stop_event = threading.Event()
        self._thread = None
        self.sim_slot = "1"
        self.sim_rotate_index = 0
        self.min_delay = 15
        self.max_delay = 35
        self.daily_limit = 500
        self.sent_today = 0
        self.today_date = date.today()
        self.gateway_mode = "custom_android"
        self.gateway_url = ""
        self.api_key = ""
        self.device_id = ""
        self.username = ""
        self.password = ""

        # Pull mode queue state
        self.current_task = None
        self.last_report = None
        self.task_reported_event = threading.Event()
        self.test_task = None

    def _add_log(self, phone: str, status: str, msg: str = ""):
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "phone": phone,
            "status": status,
            "message": msg,
        }
        with self.lock:
            self.logs.append(entry)
            if len(self.logs) > 500:
                self.logs = self.logs[-500:]

    def get_state(self) -> dict:
        with self.lock:
            return {
                "status": self.status,
                "total": self.total,
                "sent": self.sent,
                "failed": self.failed,
                "pending": self.pending,
                "progress": round(self.progress, 1),
                "next_send_countdown": self.next_send_countdown,
                "current_phone": self.current_phone,
                "campaign_id": self.campaign_id,
                "logs": list(self.logs[-100:]),
            }

    def _load_config(self):
        with get_db() as conn:
            row = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
            if row:
                self.gateway_mode = row["gateway_mode"]
                self.gateway_url = row["gateway_url"]
                self.api_key = row["api_key"]
                self.device_id = row["device_id"]
                self.username = row["username"]
                self.password = row["password"]
                self.sim_slot = row["sim_slot"]
                self.min_delay = row["min_delay"]
                self.max_delay = row["max_delay"]
                self.daily_limit = row["daily_limit"]

    def _get_current_sim(self) -> str:
        if self.sim_slot == "auto":
            sims = ["1", "2"]
            sim = sims[self.sim_rotate_index % 2]
            self.sim_rotate_index += 1
            return sim
        return self.sim_slot

    def _send_sms(self, phone: str, message: str, sim: str) -> tuple:
        """Send SMS via configured gateway. Returns (success: bool, error_msg: str)."""
        try:
            if self.gateway_mode == "textbee":
                # ── TextBee Cloud API (official spec) ──
                device_id = extract_device_id(self.device_id, self.gateway_url)
                api_key = (self.api_key or "").strip()

                if not device_id:
                    err = "TextBee Device ID kiritilmagan! Sozlamalardan Device ID ni kiriting."
                    return False, err
                if not api_key:
                    err = "TextBee API Key kiritilmagan! Sozlamalardan API kalitni kiriting."
                    return False, err

                url = f"https://api.textbee.dev/api/v1/gateway/devices/{device_id}/send-sms"
                headers = {
                    "x-api-key": api_key,
                    "Content-Type": "application/json",
                }
                payload = {
                    "recipients": [phone],
                    "message": message,
                }

                self._add_log(phone, "info", f"TextBee API ga yuborilmoqda (Device: {device_id[:8]}...)...")

                with httpx.Client(timeout=15.0) as client:
                    resp = client.post(url, json=payload, headers=headers)
                    resp_text = resp.text.strip()[:300]

                    # Treat ANY status code in [200, 201, 202] as SUCCESS
                    if resp.status_code in (200, 201, 202):
                        return True, f"HTTP {resp.status_code}"
                    else:
                        err = f"TextBee HTTP {resp.status_code}: {resp_text}"
                        return False, err

            else:
                # ── Local Android SMS Gateway ──
                url = (self.gateway_url or "").strip().rstrip("/")
                if not url:
                    return False, "Gateway URL kiritilmagan! Sozlamalarni tekshiring."

                payload = {
                    "phone": phone,
                    "message": message,
                    "sim_slot": int(sim) if str(sim).isdigit() else 1,
                }
                headers = {"Content-Type": "application/json"}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"

                auth = None
                if self.username and self.password:
                    auth = (self.username, self.password)

                self._add_log(phone, "info", f"Local Gateway ga yuborilmoqda ({url})...")

                with httpx.Client(timeout=15.0) as client:
                    resp = client.post(url, json=payload, headers=headers, auth=auth)
                    resp_text = resp.text.strip()[:300]
                    if resp.status_code in (200, 201, 202):
                        return True, f"HTTP {resp.status_code}"
                    else:
                        err = f"Local Gateway HTTP {resp.status_code}: {resp_text}"
                        return False, err

        except httpx.ConnectError as e:
            return False, f"Gateway ulanish xatosi (Connection failed): {str(e)[:150]}"
        except httpx.TimeoutException:
            return False, "Gateway 15 soniya ichida javob bermadi (timeout)"
        except Exception as e:
            return False, f"Xatolik ({type(e).__name__}): {str(e)[:150]}"

    def start(self, contacts: list, message: str):
        if self.status == "running":
            return False, "Kampaniya allaqachon ishlamoqda"

        self._load_config()
        self.contacts = contacts
        self.message_template = message
        self.total = len(contacts)
        self.sent = 0
        self.failed = 0
        self.pending = self.total
        self.progress = 0.0
        self.logs = []
        self.current_phone = ""
        self.next_send_countdown = 0
        self.campaign_id = f"camp_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        self.status = "running"
        self.sim_rotate_index = 0

        # Reset today counter if new day
        if date.today() != self.today_date:
            self.sent_today = 0
            self.today_date = date.today()

        self._stop_event.clear()
        self._pause_event.set()
        self.task_reported_event.clear()
        self.current_task = None
        self.last_report = None
        self.test_task = None

        # Save campaign to DB
        with get_db() as conn:
            conn.execute(
                "INSERT INTO campaigns (id, total, status, message_template) VALUES (?, ?, 'running', ?)",
                (self.campaign_id, self.total, message),
            )
            conn.commit()

        self._add_log("SYSTEM", "info", f"Kampaniya boshlandi — {self.total} ta raqam, ID: {self.campaign_id}")

        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()
        return True, "Kampaniya boshlandi"

    def pause(self):
        if self.status != "running":
            return False, "Kampaniya ishlamayapti"
        self._pause_event.clear()
        self.status = "paused"
        self._add_log("SYSTEM", "warning", "Kampaniya vaqtincha to'xtatildi")
        return True, "Kampaniya to'xtatildi"

    def resume(self):
        if self.status != "paused":
            return False, "Kampaniya pause holatida emas"
        self._pause_event.set()
        self.status = "running"
        self._add_log("SYSTEM", "info", "Kampaniya davom etmoqda")
        return True, "Kampaniya davom etmoqda"

    def stop(self):
        if self.status not in ("running", "paused"):
            return False, "Kampaniya ishlamayapti"
        self._stop_event.set()
        self._pause_event.set()
        self.task_reported_event.set()
        self.current_task = None
        self.test_task = None
        self.current_phone = ""
        self.status = "stopped"
        self._add_log("SYSTEM", "error", "Kampaniya bekor qilindi")

        with get_db() as conn:
            conn.execute(
                "UPDATE campaigns SET status = 'stopped', sent = ?, failed = ?, completed_at = ? WHERE id = ?",
                (self.sent, self.failed, datetime.now().isoformat(), self.campaign_id),
            )
            conn.commit()

        return True, "Kampaniya bekor qilindi"

    def _worker(self):
        """Background worker thread — sends SMS messages one by one with delays."""
        try:
            for i, (phone, name) in enumerate(self.contacts):
                # ── Wrapped completely per contact so loop NEVER crashes ──
                try:
                    # ── Check stop signal ──
                    if self._stop_event.is_set():
                        break

                    # ── Check pause signal (blocks here while paused) ──
                    self._pause_event.wait()
                    if self._stop_event.is_set():
                        break

                    # ── Check daily limit ──
                    if date.today() != self.today_date:
                        self.sent_today = 0
                        self.today_date = date.today()

                    if self.daily_limit > 0 and self.sent_today >= self.daily_limit:
                        self._add_log("SYSTEM", "warning", f"Kunlik limit ({self.daily_limit}) tugadi. Kampaniya to'xtatildi.")
                        self.status = "paused"
                        self._pause_event.clear()
                        self._pause_event.wait()
                        if self._stop_event.is_set():
                            break

                    # ── Prepare message ──
                    msg = self.message_template
                    if name:
                        msg = msg.replace("{ism}", name)
                    else:
                        msg = msg.replace("{ism}", "")

                    sim = self._get_current_sim()
                    success = False
                    error = ""

                    if self.gateway_mode == "custom_android":
                        # ── Pull Mode: Android app polls and executes task ──
                        task_id = f"task_{self.campaign_id}_{i}_{uuid.uuid4().hex[:6]}"
                        sim_int = int(sim) if str(sim).isdigit() else 1
                        with self.lock:
                            self.current_phone = phone
                            self.current_task = {
                                "has_task": True,
                                "task_id": task_id,
                                "phone": phone,
                                "message": msg,
                                "sim_slot": sim_int,
                            }
                            self.last_report = None
                            self.task_reported_event.clear()

                        self._add_log(phone, "info", f"📱 'SMS Bridge Pro' ilovasi so'rovini kutmoqda ({phone})...")

                        # Wait until Android app reports back or campaign is cancelled/paused
                        while not self._stop_event.is_set():
                            if self.task_reported_event.wait(timeout=0.5):
                                break
                            if not self._pause_event.is_set():
                                self._pause_event.wait()

                        if self._stop_event.is_set():
                            break

                        report = self.last_report or {}
                        success = (report.get("status") == "sent")
                        error = report.get("error", "")

                        with self.lock:
                            self.current_phone = ""
                            self.current_task = None
                            if success:
                                self.sent += 1
                                self.sent_today += 1
                                self._add_log(phone, "sent", f"✅ Yuborildi (SMS Bridge Pro): {phone}")
                            else:
                                self.failed += 1
                                self._add_log(phone, "failed", f"❌ Xatolik (SMS Bridge Pro): {error or 'Yuborilmadi'}")

                            self.pending = max(0, self.total - self.sent - self.failed)
                            self.progress = round(((self.sent + self.failed) / self.total * 100), 1) if self.total > 0 else 0
                    else:
                        # ── Push Mode: Direct send via TextBee or Local Android Gateway ──
                        with self.lock:
                            self.current_phone = phone

                        try:
                            success, error = self._send_sms(phone, msg, sim)
                        except Exception as send_err:
                            success = False
                            error = f"Kutilmagan xatolik: {str(send_err)[:150]}"

                        # ── IMMEDIATELY update state and clear current_phone BEFORE sleep ──
                        with self.lock:
                            self.current_phone = ""
                            if success:
                                self.sent += 1
                                self.sent_today += 1
                                self._add_log(phone, "sent", f"✅ Yuborildi: {phone}")
                            else:
                                self.failed += 1
                                self._add_log(phone, "failed", f"❌ Xatolik: {error}")

                            self.pending = max(0, self.total - self.sent - self.failed)
                            self.progress = round(((self.sent + self.failed) / self.total * 100), 1) if self.total > 0 else 0

                    # ── Save state to SQLite immediately ──
                    try:
                        with get_db() as conn:
                            conn.execute(
                                "INSERT INTO sms_logs (campaign_id, phone, name, message, status, error_msg, sim_used) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (
                                    self.campaign_id,
                                    phone,
                                    name,
                                    msg,
                                    "sent" if success else "failed",
                                    error,
                                    sim,
                                ),
                            )
                            conn.execute(
                                "UPDATE campaigns SET sent = ?, failed = ? WHERE id = ?",
                                (self.sent, self.failed, self.campaign_id),
                            )
                            conn.commit()
                    except Exception as db_err:
                        self._add_log("SYSTEM", "warning", f"DB ga yozishda xatolik: {str(db_err)[:100]}")

                    # ── Anti-spam random delay before next SMS ──
                    if i < len(self.contacts) - 1 and not self._stop_event.is_set():
                        delay = random.uniform(self.min_delay, self.max_delay)
                        int_delay = max(1, int(round(delay)))
                        self._add_log("SYSTEM", "info", f"⏳ Keyingi SMS gacha {int_delay} soniya kutilmoqda...")

                        for remaining in range(int_delay, 0, -1):
                            if self._stop_event.is_set():
                                break
                            self._pause_event.wait()
                            with self.lock:
                                self.next_send_countdown = remaining
                            time.sleep(1)

                        with self.lock:
                            self.next_send_countdown = 0

                except Exception as loop_e:
                    # If an error happens for this contact, log and continue to next contact
                    self._add_log(phone, "failed", f"Xatolik yuz berdi: {str(loop_e)[:150]}")
                    with self.lock:
                        self.current_phone = ""
                        self.failed += 1
                        self.pending = max(0, self.total - self.sent - self.failed)
                        self.progress = round(((self.sent + self.failed) / self.total * 100), 1) if self.total > 0 else 0

            # ── Campaign completed ──
            if not self._stop_event.is_set():
                with self.lock:
                    self.status = "completed"
                    self.current_phone = ""
                    self.pending = 0
                    self.progress = 100.0
                    self.next_send_countdown = 0
                self._add_log(
                    "SYSTEM",
                    "info",
                    f"Kampaniya tugadi! ✅ {self.sent} yuborildi, ❌ {self.failed} xatolik",
                )
                try:
                    with get_db() as conn:
                        conn.execute(
                            "UPDATE campaigns SET status = 'completed', sent = ?, failed = ?, completed_at = ? WHERE id = ?",
                            (self.sent, self.failed, datetime.now().isoformat(), self.campaign_id),
                        )
                        conn.commit()
                except Exception:
                    pass

        except Exception as fatal_e:
            self._add_log("SYSTEM", "error", f"Kutilmagan xatolik yuz berdi: {str(fatal_e)}")
            with self.lock:
                self.status = "stopped"
                self.current_phone = ""
                self.next_send_countdown = 0
        finally:
            with self.lock:
                self.current_phone = ""
                self.next_send_countdown = 0


# Global engine instance
engine = CampaignEngine()


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — PAGES
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    local_ip = get_local_ip()
    return templates.TemplateResponse("index.html", {"request": request, "server_ip": local_ip})


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/config")
async def get_config():
    with get_db() as conn:
        row = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
        data = dict(row)
        data["server_ip"] = get_local_ip()
        return data


@app.post("/api/config")
async def save_config(cfg: ConfigModel):
    with get_db() as conn:
        conn.execute(
            """UPDATE config SET
                gateway_mode = ?, gateway_url = ?, api_key = ?,
                device_id = ?, username = ?, password = ?, sim_slot = ?,
                min_delay = ?, max_delay = ?, daily_limit = ?
            WHERE id = 1""",
            (
                cfg.gateway_mode,
                cfg.gateway_url,
                cfg.api_key,
                cfg.device_id,
                cfg.username,
                cfg.password,
                cfg.sim_slot,
                cfg.min_delay,
                cfg.max_delay,
                cfg.daily_limit,
            ),
        )
        conn.commit()
    return {"ok": True, "message": "Sozlamalar saqlandi"}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — CONTACTS
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/contacts/upload")
async def upload_contacts(file: UploadFile = File(...), session_id: str = Form("")):
    if not session_id:
        session_id = uuid.uuid4().hex[:12]

    content = await file.read()
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    contacts = parse_contacts_text(text)
    if not contacts:
        raise HTTPException(400, "Hech qanday to'g'ri raqam topilmadi")

    with get_db() as conn:
        conn.execute("DELETE FROM contacts WHERE session_id = ?", (session_id,))
        conn.executemany(
            "INSERT OR IGNORE INTO contacts (phone, name, session_id) VALUES (?, ?, ?)",
            [(p, n, session_id) for p, n in contacts],
        )
        conn.commit()

    return {
        "ok": True,
        "session_id": session_id,
        "total": len(contacts),
        "message": f"{len(contacts)} ta raqam yuklandi",
    }


@app.post("/api/contacts/paste")
async def paste_contacts(data: PasteContactsModel):
    session_id = data.session_id or uuid.uuid4().hex[:12]
    contacts = parse_contacts_text(data.text)
    if not contacts:
        raise HTTPException(400, "Hech qanday to'g'ri raqam topilmadi")

    with get_db() as conn:
        conn.execute("DELETE FROM contacts WHERE session_id = ?", (session_id,))
        conn.executemany(
            "INSERT OR IGNORE INTO contacts (phone, name, session_id) VALUES (?, ?, ?)",
            [(p, n, session_id) for p, n in contacts],
        )
        conn.commit()

    return {
        "ok": True,
        "session_id": session_id,
        "total": len(contacts),
        "message": f"{len(contacts)} ta raqam yuklandi",
    }


@app.get("/api/contacts/{session_id}")
async def get_contacts(session_id: str):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT phone, name FROM contacts WHERE session_id = ? ORDER BY id",
            (session_id,),
        ).fetchall()
    return {"contacts": [dict(r) for r in rows], "total": len(rows)}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — CAMPAIGN CONTROLS
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/campaign/start")
async def start_campaign(data: StartCampaignModel):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT phone, name FROM contacts WHERE session_id = ? ORDER BY id",
            (data.session_id,),
        ).fetchall()

    contacts = [(r["phone"], r["name"]) for r in rows]
    if not contacts:
        raise HTTPException(400, "Raqamlar bazasi bo'sh! Avval raqamlarni yuklang.")
    if not data.message.strip():
        raise HTTPException(400, "Xabar matni bo'sh bo'lmasligi kerak!")

    ok, msg = engine.start(contacts, data.message)
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg, "campaign_id": engine.campaign_id}


@app.post("/api/campaign/pause")
async def pause_campaign():
    ok, msg = engine.pause()
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg}


@app.post("/api/campaign/resume")
async def resume_campaign():
    ok, msg = engine.resume()
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg}


@app.post("/api/campaign/stop")
async def stop_campaign():
    ok, msg = engine.stop()
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg}


@app.get("/api/campaign/status")
async def campaign_status():
    return engine.get_state()


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — TEST SMS
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/test-sms")
async def test_sms(data: TestSmsModel):
    phone = clean_phone(data.phone)
    if not phone:
        raise HTTPException(400, "Noto'g'ri telefon raqam! Namuna: +998901234567")

    engine._load_config()

    # Override with parameters passed from modal if present
    if data.device_id:
        engine.device_id = data.device_id
    if data.api_key:
        engine.api_key = data.api_key
    if data.gateway_mode:
        engine.gateway_mode = data.gateway_mode
    if data.gateway_url:
        engine.gateway_url = data.gateway_url

    if engine.gateway_mode == "custom_android":
        sim_val = engine.sim_slot
        sim_int = int(sim_val) if str(sim_val).isdigit() else 1
        engine.test_task = {
            "has_task": True,
            "task_id": f"test_{uuid.uuid4().hex[:6]}",
            "phone": phone,
            "message": data.message,
            "sim_slot": sim_int,
        }
        engine._add_log(phone, "info", f"📱 Test SMS navbatga qo'yildi. 'SMS Bridge Pro' ilovangiz yuboradi: {phone}")
        return {"ok": True, "message": f"Test SMS navbatga qo'yildi. 'SMS Bridge Pro' ilovangiz orqali yuboriladi ✅"}

    sim = engine._get_current_sim()
    success, error = engine._send_sms(phone, data.message, sim)

    if success:
        engine._add_log(phone, "sent", "Test SMS muvaffaqiyatli yuborildi ✅")
        return {"ok": True, "message": f"Test SMS {phone} ga muvaffaqiyatli yuborildi ✅"}
    else:
        engine._add_log(phone, "failed", f"Test SMS xatosi: {error}")
        return {"ok": False, "message": f"Xatolik: {error}"}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — MOBILE ANDROID GATEWAY (PULL QUEUE)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/mobile/ping")
def mobile_ping():
    return {
        "status": "ok",
        "server_ip": get_local_ip(),
        "campaign_active": engine.status == "running",
        "gateway_mode": engine.gateway_mode,
    }


@app.get("/api/mobile/poll")
def mobile_poll():
    # 1. Check if there's a test task waiting
    if engine.test_task:
        task = dict(engine.test_task)
        return task

    # 2. Check if a campaign is active
    if engine.status != "running":
        return {"has_task": False, "wait_seconds": 3, "message": "Kampaniya faol emas"}

    # 3. If in anti-spam countdown delay
    if engine.next_send_countdown > 0:
        return {"has_task": False, "wait_seconds": engine.next_send_countdown, "message": "Anti-spam tanaffus"}

    # 4. If current task is ready for pulling
    if engine.current_task:
        return engine.current_task

    return {"has_task": False, "wait_seconds": 2}


@app.post("/api/mobile/report")
def mobile_report(rep: MobileReportModel):
    # If this was a test task
    if rep.task_id.startswith("test_"):
        if rep.status == "sent":
            engine._add_log(rep.phone, "sent", f"✅ Test SMS yuborildi (SMS Bridge Pro): {rep.phone}")
        else:
            engine._add_log(rep.phone, "failed", f"❌ Test SMS xatosi: {rep.error or 'Yuborilmadi'}")
        engine.test_task = None
        return {"ok": True, "message": "Test hisoboti qabul qilindi"}

    # Campaign task
    engine.last_report = {
        "task_id": rep.task_id,
        "phone": rep.phone,
        "status": rep.status,
        "error": rep.error or "",
    }
    engine.task_reported_event.set()
    return {"ok": True, "message": "Hisobot qabul qilindi"}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — SSE STREAM
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/stream")
async def sse_stream(request: Request):
    """Server-Sent Events endpoint for real-time campaign updates."""

    import asyncio

    async def event_generator():
        last_log_count = 0
        while True:
            if await request.is_disconnected():
                break
            state = engine.get_state()
            yield f"data: {json.dumps(state, ensure_ascii=False)}\n\n"
            await asyncio.sleep(1.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — EXPORT
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/export/{status_filter}")
async def export_logs(status_filter: str):
    """Export campaign logs as CSV. status_filter: 'all', 'sent', 'failed'."""
    with get_db() as conn:
        if status_filter in ("sent", "failed"):
            rows = conn.execute(
                "SELECT phone, name, message, status, error_msg, sim_used, sent_at FROM sms_logs WHERE status = ? ORDER BY id DESC",
                (status_filter,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT phone, name, message, status, error_msg, sim_used, sent_at FROM sms_logs ORDER BY id DESC"
            ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Telefon", "Ism", "Xabar", "Status", "Xatolik", "SIM", "Vaqt"])
    for r in rows:
        writer.writerow([r["phone"], r["name"], r["message"], r["status"], r["error_msg"], r["sim_used"], r["sent_at"]])

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=sms_{status_filter}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — HISTORY
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/history")
async def campaign_history():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM campaigns ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
    return {"campaigns": [dict(r) for r in rows]}


@app.get("/api/history/{campaign_id}/logs")
async def campaign_logs(campaign_id: str):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT phone, name, status, error_msg, sim_used, sent_at FROM sms_logs WHERE campaign_id = ? ORDER BY id",
            (campaign_id,),
        ).fetchall()
    return {"logs": [dict(r) for r in rows]}


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    local_ip = get_local_ip()
    print("=" * 65)
    print(" 🚀 SMS Campaign & CRM Dashboard ishga tushirilmoqda...")
    print(f" 👉 Kompyuter brauzerida:      http://localhost:8000")
    print(f" 👉 Yoki:                     http://127.0.0.1:8000")
    print(f" 📱 Android ilova / Telefon:   http://{local_ip}:8000")
    print(" ⚠️  DIQQAT: Brauzerga '0.0.0.0:8000' deb yozmang (xatolik beradi)!")
    print("=" * 65)
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

