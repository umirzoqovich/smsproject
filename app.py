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
import hashlib
import secrets
from datetime import datetime, date
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, List

from fastapi import FastAPI, UploadFile, File, Form, Request, Response, HTTPException, Query
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, RedirectResponse
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
# PASSWORD HASHING (STANDARD LIBRARY SHA-256 + SALT)
# ═══════════════════════════════════════════════════════════════════════════════
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{h}"


def verify_password(password: str, stored: str) -> bool:
    try:
        if ":" not in stored:
            return password == stored
        salt, h = stored.split(":", 1)
        return hashlib.sha256((salt + password).encode()).hexdigest() == h
    except Exception:
        return False


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
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT DEFAULT '',
                role TEXT DEFAULT 'user',
                device_token TEXT UNIQUE NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER DEFAULT 1,
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

            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER DEFAULT 1,
                phone TEXT NOT NULL,
                name TEXT DEFAULT '',
                session_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sms_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER DEFAULT 1,
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
                user_id INTEGER DEFAULT 1,
                total INTEGER DEFAULT 0,
                sent INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'idle',
                message_template TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );
        """)

        # Migration: Ensure user_id column exists in existing tables
        for tbl in ["contacts", "campaigns", "sms_logs", "config"]:
            c.execute(f"PRAGMA table_info({tbl})")
            cols = [col[1] for col in c.fetchall()]
            if "user_id" not in cols:
                c.execute(f"ALTER TABLE {tbl} ADD COLUMN user_id INTEGER DEFAULT 1")

        # Ensure device_id column exists in config
        c.execute("PRAGMA table_info(config)")
        cfg_cols = [col[1] for col in c.fetchall()]
        if "device_id" not in cfg_cols:
            c.execute("ALTER TABLE config ADD COLUMN device_id TEXT DEFAULT ''")

        # Migration: remove legacy single-user constraint CHECK (id = 1) from config table if present
        c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='config'")
        cfg_tbl = c.fetchone()
        if cfg_tbl and "CHECK" in (cfg_tbl[0] or "").upper():
            c.execute("PRAGMA table_info(config)")
            existing_cols = {col[1] for col in c.fetchall()}
            c.execute("""
                CREATE TABLE IF NOT EXISTS config_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER DEFAULT 1,
                    gateway_mode TEXT DEFAULT 'custom_android',
                    gateway_url TEXT DEFAULT '',
                    api_key TEXT DEFAULT '',
                    device_id TEXT DEFAULT '',
                    username TEXT DEFAULT '',
                    password TEXT DEFAULT '',
                    sim_slot TEXT DEFAULT '1',
                    min_delay INTEGER DEFAULT 15,
                    max_delay INTEGER DEFAULT 35,
                    daily_limit INTEGER DEFAULT 500
                )
            """)
            col_list = ["id", "gateway_mode", "gateway_url", "api_key", "username", "password", "sim_slot", "min_delay", "max_delay", "daily_limit"]
            if "device_id" in existing_cols:
                col_list.append("device_id")
            if "user_id" in existing_cols:
                col_list.append("user_id")
            cols_str = ", ".join(col_list)
            c.execute(f"INSERT OR IGNORE INTO config_v2 ({cols_str}) SELECT {cols_str} FROM config")
            c.execute("DROP TABLE config")
            c.execute("ALTER TABLE config_v2 RENAME TO config")

        # Set default gateway_mode to custom_android for configs without custom gateway_url
        c.execute("UPDATE config SET gateway_mode = 'custom_android' WHERE gateway_mode = 'local' AND (gateway_url IS NULL OR gateway_url = '' OR gateway_url LIKE '%192.168.1.105%')")

        # Seed initial admin account if users table is empty
        c.execute("SELECT COUNT(*) FROM users")
        user_count = c.fetchone()[0]
        if user_count == 0:
            admin_pwd = hash_password("admin123")
            c.execute(
                "INSERT INTO users (id, username, password_hash, full_name, role, device_token, is_active) VALUES (1, 'admin', ?, 'Administrator', 'admin', 'admin', 1)",
                (admin_pwd,),
            )
            # Ensure config for user 1
            c.execute("SELECT COUNT(*) FROM config WHERE user_id = 1")
            if c.fetchone()[0] == 0:
                c.execute(
                    "INSERT INTO config (user_id, gateway_mode, min_delay, max_delay, daily_limit, sim_slot) VALUES (1, 'custom_android', 15, 35, 500, '1')"
                )
            else:
                c.execute("UPDATE config SET gateway_mode = 'custom_android', user_id = 1 WHERE id = 1 OR user_id = 1")

        conn.commit()


init_db()


# ═══════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ═══════════════════════════════════════════════════════════════════════════════
class LoginModel(BaseModel):
    username: str
    password: str


class CreateUserModel(BaseModel):
    username: str
    password: str
    full_name: Optional[str] = ""
    role: Optional[str] = "user"
    device_token: Optional[str] = ""


class ResetPasswordModel(BaseModel):
    new_password: str


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
    device_token: Optional[str] = ""


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


def cell_to_str(val) -> str:
    """Convert Excel cell value to clean string, handling float notation (.0) properly."""
    if val is None:
        return ""
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        return str(val).split(".")[0]
    if isinstance(val, int):
        return str(val)
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def parse_contacts_excel(content_bytes: bytes) -> list:
    """Parse phone numbers and optional names from Excel (.xlsx) bytes."""
    import openpyxl
    results = []
    seen = set()

    try:
        wb = openpyxl.load_workbook(filename=io.BytesIO(content_bytes), data_only=True, read_only=True)
        sheet = wb.active
        if not sheet:
            return []

        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            return []

        # Check for header row
        first_row = [str(c or "").strip().lower() for c in rows[0]]
        phone_col_idx = None
        name_col_idx = None

        phone_keywords = ["phone", "telefon", "raqam", "nomer", "tel", "aloqa", "mobile", "номер", "телефон"]
        name_keywords = ["name", "ism", "fio", "mijoz", "klient", "client", "имя", "фио"]

        for idx, col_name in enumerate(first_row):
            for kw in phone_keywords:
                if kw in col_name and phone_col_idx is None:
                    phone_col_idx = idx
            for kw in name_keywords:
                if kw in col_name and name_col_idx is None:
                    name_col_idx = idx

        start_row_idx = 1 if phone_col_idx is not None else 0

        for row in rows[start_row_idx:]:
            if not row or all(v is None or str(v).strip() == "" for v in row):
                continue

            raw_phone = ""
            name = ""

            if phone_col_idx is not None and phone_col_idx < len(row):
                raw_phone = cell_to_str(row[phone_col_idx])
                if name_col_idx is not None and name_col_idx < len(row):
                    name = str(row[name_col_idx] or "").strip()

            phone = clean_phone(raw_phone)
            # If not detected via header or column index, search across row cells
            if not phone:
                for c_idx, cell_val in enumerate(row):
                    cand = clean_phone(cell_to_str(cell_val))
                    if cand:
                        phone = cand
                        # Check remaining cells for a name
                        for n_idx, n_val in enumerate(row):
                            if n_idx != c_idx and n_val:
                                s_val = str(n_val).strip()
                                if s_val and not re.sub(r"[^\d]", "", s_val):
                                    name = s_val
                                    break
                                elif s_val and len(s_val) < 50 and not s_val.startswith("+") and not s_val.isdigit():
                                    name = s_val
                                    break
                        break

            if phone and phone not in seen:
                seen.add(phone)
                results.append((phone, name))

        return results
    except Exception as e:
        print(f"Error parsing Excel file: {e}")
        return []


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
    """Background SMS sending engine with pause/resume/stop controls per user."""

    def __init__(self, user_id: int = 1):
        self.user_id = user_id
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
            row = conn.execute("SELECT * FROM config WHERE user_id = ? ORDER BY id DESC LIMIT 1", (self.user_id,)).fetchone()
            if not row and self.user_id == 1:
                row = conn.execute("SELECT * FROM config WHERE id = 1").fetchone()
            if row:
                self.gateway_mode = row["gateway_mode"] or "custom_android"
                self.gateway_url = row["gateway_url"] or ""
                self.api_key = row["api_key"] or ""
                self.device_id = row["device_id"] or ""
                self.username = row["username"] or ""
                self.password = row["password"] or ""
                self.sim_slot = row["sim_slot"] or "1"
                self.min_delay = row["min_delay"] or 15
                self.max_delay = row["max_delay"] or 35
                self.daily_limit = row["daily_limit"] or 500

                # Auto fallback to custom_android if mode is empty or local without custom url
                if not self.gateway_mode or (self.gateway_mode == "local" and not (self.gateway_url or "").strip()):
                    self.gateway_mode = "custom_android"

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
                    return False, "Gateway URL kiritilmagan! Agar 'SMS Bridge Pro' ilovasini ishlatayotgan bo'lsangiz, Sozlamalardan 'Shaxsiy Android Ilova (Pull Mode)' rejimini tanlang va Saqlang."

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
                "INSERT INTO campaigns (id, user_id, total, status, message_template) VALUES (?, ?, ?, 'running', ?)",
                (self.campaign_id, self.user_id, self.total, message),
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
                                "INSERT INTO sms_logs (campaign_id, user_id, phone, name, message, status, error_msg, sim_used) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    self.campaign_id,
                                    self.user_id,
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


# ═══════════════════════════════════════════════════════════════════════════════
# MULTI-USER ENGINE MANAGER & AUTH HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
engines: dict = {}
engine_lock = threading.Lock()


def get_engine(user_id: int) -> CampaignEngine:
    with engine_lock:
        if user_id not in engines:
            engines[user_id] = CampaignEngine(user_id=user_id)
        return engines[user_id]


def get_current_user(request: Request) -> Optional[dict]:
    token = request.cookies.get("session_token")
    if not token:
        auth = request.headers.get("Authorization")
        if auth and auth.startswith("Bearer "):
            token = auth[7:].strip()
    if not token:
        return None
    with get_db() as conn:
        row = conn.execute(
            """SELECT u.id, u.username, u.full_name, u.role, u.device_token, u.is_active
               FROM sessions s
               JOIN users u ON s.user_id = u.id
               WHERE s.token = ? AND u.is_active = 1""",
            (token,),
        ).fetchone()
        return dict(row) if row else None


def require_user(request: Request) -> dict:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Tizimga kirilmagan! Iltimos, avval login qiling.")
    return user


def require_admin(request: Request) -> dict:
    user = require_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Ruxsat etilmagan! Faqat administrator uchun.")
    return user


def resolve_user_for_device(token: Optional[str] = None, request: Optional[Request] = None) -> Optional[dict]:
    cand = (token or "").strip()
    if not cand and request:
        cand = (
            request.query_params.get("token")
            or request.query_params.get("device_token")
            or request.query_params.get("t")
            or request.headers.get("x-device-token")
            or ""
        ).strip()
        if not cand:
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer "):
                cand = auth[7:].strip()

    with get_db() as conn:
        if cand:
            row = conn.execute(
                "SELECT id, username, full_name, role, device_token FROM users WHERE (device_token = ? OR username = ?) AND is_active = 1",
                (cand, cand),
            ).fetchone()
            if row:
                return dict(row)

        # Fallback to admin or first active user
        row = conn.execute("SELECT id, username, full_name, role, device_token FROM users WHERE role = 'admin' AND is_active = 1 LIMIT 1").fetchone()
        if row:
            return dict(row)
        row = conn.execute("SELECT id, username, full_name, role, device_token FROM users WHERE is_active = 1 LIMIT 1").fetchone()
        return dict(row) if row else None


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — AUTH & PAGES
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/api/login")
async def api_login(data: LoginModel, response: Response):
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, username, password_hash, full_name, role, device_token, is_active FROM users WHERE username = ?",
            (data.username.strip().lower(),),
        ).fetchone()
        if not row:
            raise HTTPException(400, "Login yoki parol noto'g'ri")
        user = dict(row)
        if not user.get("is_active"):
            raise HTTPException(403, "Ushbu hisob faolsizlantirilgan. Administratorga murojaat qiling.")
        if not verify_password(data.password, user["password_hash"]):
            raise HTTPException(400, "Login yoki parol noto'g'ri")

        session_token = secrets.token_hex(32)
        conn.execute("INSERT INTO sessions (token, user_id) VALUES (?, ?)", (session_token, user["id"]))
        conn.commit()

        resp = JSONResponse({
            "ok": True,
            "message": "Muvaffaqiyatli kirildi",
            "redirect": "/",
            "user": {
                "id": user["id"],
                "username": user["username"],
                "full_name": user["full_name"],
                "role": user["role"],
                "device_token": user["device_token"],
            },
        })
        resp.set_cookie(key="session_token", value=session_token, max_age=30 * 86400, httponly=True, samesite="lax")
        return resp


@app.post("/api/logout")
async def api_logout(request: Request):
    token = request.cookies.get("session_token")
    if token:
        with get_db() as conn:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
    resp = JSONResponse({"ok": True, "redirect": "/login"})
    resp.delete_cookie(key="session_token")
    return resp


@app.get("/api/me")
async def api_me(request: Request):
    user = require_user(request)
    return {"user": user}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    local_ip = get_local_ip()
    return templates.TemplateResponse("index.html", {"request": request, "server_ip": local_ip, "user": user})


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — ADMIN (USER MANAGEMENT)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/users")
async def admin_list_users(request: Request):
    require_admin(request)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, username, full_name, role, device_token, is_active, created_at FROM users ORDER BY id ASC"
        ).fetchall()
        return {"users": [dict(r) for r in rows]}


@app.post("/api/admin/users")
async def admin_create_user(data: CreateUserModel, request: Request):
    require_admin(request)
    username = data.username.strip().lower()
    if not username or len(username) < 3:
        raise HTTPException(400, "Login kamida 3 ta belgidan iborat bo'lishi kerak")
    if not data.password or len(data.password) < 4:
        raise HTTPException(400, "Parol kamida 4 ta belgidan iborat bo'lishi kerak")

    device_token = (data.device_token or "").strip() or username
    pwd_hash = hash_password(data.password)

    try:
        with get_db() as conn:
            existing = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if existing:
                raise HTTPException(400, f"'{username}' nomli login allaqachon mavjud!")
            tok_existing = conn.execute("SELECT id FROM users WHERE device_token = ?", (device_token,)).fetchone()
            if tok_existing:
                device_token = f"{device_token}_{secrets.token_hex(2)}"

            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (username, password_hash, full_name, role, device_token, is_active) VALUES (?, ?, ?, ?, ?, 1)",
                (username, pwd_hash, data.full_name or username, data.role or "user", device_token),
            )
            new_id = cursor.lastrowid

            # Create default config for this user if not already present
            cfg_exists = conn.execute("SELECT id FROM config WHERE user_id = ?", (new_id,)).fetchone()
            if not cfg_exists:
                conn.execute(
                    "INSERT INTO config (user_id, gateway_mode, min_delay, max_delay, daily_limit, sim_slot) VALUES (?, 'custom_android', 15, 35, 500, '1')",
                    (new_id,),
                )
            conn.commit()

        return {
            "ok": True,
            "message": f"Foydalanuvchi '{username}' muvaffaqiyatli yaratildi",
            "user": {
                "id": new_id,
                "username": username,
                "full_name": data.full_name,
                "device_token": device_token,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating user: {e}")
        raise HTTPException(500, f"Foydalanuvchi yaratishda xatolik: {str(e)}")


@app.delete("/api/admin/users/{user_id}")
async def admin_delete_user(user_id: int, request: Request):
    curr = require_admin(request)
    if curr["id"] == user_id:
        raise HTTPException(400, "O'zingizning administrator akkauntingizni o'chira olmaysiz!")

    with get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.execute("DELETE FROM config WHERE user_id = ?", (user_id,))
        conn.commit()

    with engine_lock:
        if user_id in engines:
            engines[user_id].stop()
            del engines[user_id]

    return {"ok": True, "message": "Foydalanuvchi o'chirildi"}


@app.post("/api/admin/users/{user_id}/reset-password")
async def admin_reset_password(user_id: int, data: ResetPasswordModel, request: Request):
    require_admin(request)
    if not data.new_password or len(data.new_password) < 4:
        raise HTTPException(400, "Yangi parol kamida 4 ta belgidan iborat bo'lishi kerak")

    pwd_hash = hash_password(data.new_password)
    with get_db() as conn:
        conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pwd_hash, user_id))
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        conn.commit()

    return {"ok": True, "message": "Parol yangilandi"}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/config")
async def get_config(request: Request):
    user = require_user(request)
    with get_db() as conn:
        row = conn.execute("SELECT * FROM config WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user["id"],)).fetchone()
        if not row:
            conn.execute("INSERT INTO config (user_id, gateway_mode) VALUES (?, 'custom_android')", (user["id"],))
            conn.commit()
            row = conn.execute("SELECT * FROM config WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user["id"],)).fetchone()
        data = dict(row)
        data["server_ip"] = get_local_ip()
        data["device_token"] = user.get("device_token", "")
        data["username"] = user.get("username", "")
        return data


@app.post("/api/config")
async def save_config(cfg: ConfigModel, request: Request):
    user = require_user(request)
    with get_db() as conn:
        row = conn.execute("SELECT id FROM config WHERE user_id = ?", (user["id"],)).fetchone()
        if row:
            conn.execute(
                """UPDATE config SET
                    gateway_mode = ?, gateway_url = ?, api_key = ?,
                    device_id = ?, username = ?, password = ?, sim_slot = ?,
                    min_delay = ?, max_delay = ?, daily_limit = ?
                WHERE user_id = ?""",
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
                    user["id"],
                ),
            )
        else:
            conn.execute(
                """INSERT INTO config (user_id, gateway_mode, gateway_url, api_key, device_id, username, password, sim_slot, min_delay, max_delay, daily_limit)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    user["id"],
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

    eng = get_engine(user["id"])
    eng._load_config()
    return {"ok": True, "message": "Sozlamalar saqlandi"}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — CONTACTS
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/contacts/upload")
async def upload_contacts(request: Request, file: UploadFile = File(...), session_id: str = Form("")):
    user = require_user(request)
    if not session_id:
        session_id = uuid.uuid4().hex[:12]

    content = await file.read()
    filename = (file.filename or "").lower()

    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        contacts = parse_contacts_excel(content)
    else:
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("latin-1")
        contacts = parse_contacts_text(text)

    if not contacts:
        raise HTTPException(400, "Fayldan hech qanday to'g'ri telefon raqam topilmadi! Excel (.xlsx), CSV yoki TXT formatida yuklang.")

    with get_db() as conn:
        conn.execute("DELETE FROM contacts WHERE user_id = ? AND session_id = ?", (user["id"], session_id))
        conn.executemany(
            "INSERT OR IGNORE INTO contacts (user_id, phone, name, session_id) VALUES (?, ?, ?, ?)",
            [(user["id"], p, n, session_id) for p, n in contacts],
        )
        conn.commit()

    return {
        "ok": True,
        "session_id": session_id,
        "total": len(contacts),
        "message": f"{len(contacts)} ta raqam yuklandi",
    }


@app.post("/api/contacts/paste")
async def paste_contacts(data: PasteContactsModel, request: Request):
    user = require_user(request)
    session_id = data.session_id or uuid.uuid4().hex[:12]
    contacts = parse_contacts_text(data.text)
    if not contacts:
        raise HTTPException(400, "Hech qanday to'g'ri raqam topilmadi")

    with get_db() as conn:
        conn.execute("DELETE FROM contacts WHERE user_id = ? AND session_id = ?", (user["id"], session_id))
        conn.executemany(
            "INSERT OR IGNORE INTO contacts (user_id, phone, name, session_id) VALUES (?, ?, ?, ?)",
            [(user["id"], p, n, session_id) for p, n in contacts],
        )
        conn.commit()

    return {
        "ok": True,
        "session_id": session_id,
        "total": len(contacts),
        "message": f"{len(contacts)} ta raqam yuklandi",
    }


@app.get("/api/contacts/{session_id}")
async def get_contacts(session_id: str, request: Request):
    user = require_user(request)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT phone, name FROM contacts WHERE user_id = ? AND session_id = ? ORDER BY id",
            (user["id"], session_id),
        ).fetchall()
    return {"contacts": [dict(r) for r in rows], "total": len(rows)}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — CAMPAIGN CONTROLS
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/campaign/start")
async def start_campaign(data: StartCampaignModel, request: Request):
    user = require_user(request)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT phone, name FROM contacts WHERE user_id = ? AND session_id = ? ORDER BY id",
            (user["id"], data.session_id),
        ).fetchall()

    contacts = [(r["phone"], r["name"]) for r in rows]
    if not contacts:
        raise HTTPException(400, "Raqamlar bazasi bo'sh! Avval raqamlarni yuklang.")
    if not data.message.strip():
        raise HTTPException(400, "Xabar matni bo'sh bo'lmasligi kerak!")

    eng = get_engine(user["id"])
    ok, msg = eng.start(contacts, data.message)
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg, "campaign_id": eng.campaign_id}


@app.post("/api/campaign/pause")
async def pause_campaign(request: Request):
    user = require_user(request)
    eng = get_engine(user["id"])
    ok, msg = eng.pause()
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg}


@app.post("/api/campaign/resume")
async def resume_campaign(request: Request):
    user = require_user(request)
    eng = get_engine(user["id"])
    ok, msg = eng.resume()
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg}


@app.post("/api/campaign/stop")
async def stop_campaign(request: Request):
    user = require_user(request)
    eng = get_engine(user["id"])
    ok, msg = eng.stop()
    if not ok:
        raise HTTPException(400, msg)
    return {"ok": True, "message": msg}


@app.get("/api/campaign/status")
async def campaign_status(request: Request):
    user = require_user(request)
    eng = get_engine(user["id"])
    return eng.get_state()


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — TEST SMS
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/api/test-sms")
async def test_sms(data: TestSmsModel, request: Request):
    user = require_user(request)
    eng = get_engine(user["id"])
    phone = clean_phone(data.phone)
    if not phone:
        raise HTTPException(400, "Noto'g'ri telefon raqam! Namuna: +998901234567")

    eng._load_config()

    if data.device_id:
        eng.device_id = data.device_id
    if data.api_key:
        eng.api_key = data.api_key
    if data.gateway_mode:
        eng.gateway_mode = data.gateway_mode
    if data.gateway_url:
        eng.gateway_url = data.gateway_url

    if eng.gateway_mode == "custom_android":
        sim_val = eng.sim_slot
        sim_int = int(sim_val) if str(sim_val).isdigit() else 1
        eng.test_task = {
            "has_task": True,
            "task_id": f"test_{uuid.uuid4().hex[:6]}",
            "phone": phone,
            "message": data.message,
            "sim_slot": sim_int,
        }
        eng._add_log(phone, "info", f"📱 Test SMS navbatga qo'yildi. 'SMS Bridge Pro' ilovangiz yuboradi: {phone}")
        return {"ok": True, "message": f"Test SMS navbatga qo'yildi. 'SMS Bridge Pro' ilovangiz orqali yuboriladi ✅"}

    sim = eng._get_current_sim()
    success, error = eng._send_sms(phone, data.message, sim)

    if success:
        eng._add_log(phone, "sent", "Test SMS muvaffaqiyatli yuborildi ✅")
        return {"ok": True, "message": f"Test SMS {phone} ga muvaffaqiyatli yuborildi ✅"}
    else:
        eng._add_log(phone, "failed", f"Test SMS xatosi: {error}")
        return {"ok": False, "message": f"Xatolik: {error}"}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — MOBILE ANDROID GATEWAY (PULL QUEUE) — MULTI-DEVICE SUPPORT
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/mobile/ping")
@app.get("/api/mobile/{token}/ping")
@app.get("/u/{token}/api/mobile/ping")
def mobile_ping(request: Request, token: Optional[str] = None):
    user = resolve_user_for_device(token, request)
    if not user:
        return {"status": "error", "message": "Foydalanuvchi topilmadi"}
    eng = get_engine(user["id"])
    return {
        "status": "ok",
        "username": user["username"],
        "device_token": user["device_token"],
        "server_ip": get_local_ip(),
        "campaign_active": eng.status == "running",
        "gateway_mode": eng.gateway_mode,
    }


@app.get("/api/mobile/poll")
@app.get("/api/mobile/{token}/poll")
@app.get("/u/{token}/api/mobile/poll")
def mobile_poll(request: Request, token: Optional[str] = None):
    user = resolve_user_for_device(token, request)
    if not user:
        return {"has_task": False, "wait_seconds": 5, "message": "Foydalanuvchi topilmadi"}
    eng = get_engine(user["id"])

    # 1. Check if there's a test task waiting
    if eng.test_task:
        task = dict(eng.test_task)
        return task

    # 2. Check if a campaign is active
    if eng.status != "running":
        return {"has_task": False, "wait_seconds": 3, "message": "Kampaniya faol emas"}

    # 3. If in anti-spam countdown delay
    if eng.next_send_countdown > 0:
        return {"has_task": False, "wait_seconds": eng.next_send_countdown, "message": "Anti-spam tanaffus"}

    # 4. If current task is ready for pulling
    if eng.current_task:
        return eng.current_task

    return {"has_task": False, "wait_seconds": 2}


@app.post("/api/mobile/report")
@app.post("/api/mobile/{token}/report")
@app.post("/u/{token}/api/mobile/report")
def mobile_report(rep: MobileReportModel, request: Request, token: Optional[str] = None):
    device_tok = token or getattr(rep, "device_token", None)
    user = resolve_user_for_device(device_tok, request)
    if not user:
        return {"ok": False, "message": "Foydalanuvchi topilmadi"}
    eng = get_engine(user["id"])

    # If this was a test task
    if rep.task_id.startswith("test_"):
        if rep.status == "sent":
            eng._add_log(rep.phone, "sent", f"✅ Test SMS yuborildi (SMS Bridge Pro): {rep.phone}")
        else:
            eng._add_log(rep.phone, "failed", f"❌ Test SMS xatosi: {rep.error or 'Yuborilmadi'}")
        eng.test_task = None
        return {"ok": True, "message": "Test hisoboti qabul qilindi"}

    # Campaign task
    eng.last_report = {
        "task_id": rep.task_id,
        "phone": rep.phone,
        "status": rep.status,
        "error": rep.error or "",
    }
    eng.task_reported_event.set()
    return {"ok": True, "message": "Hisobot qabul qilindi"}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — SSE STREAM (PER USER)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/stream")
async def sse_stream(request: Request):
    """Server-Sent Events endpoint for real-time campaign updates."""
    user = require_user(request)
    eng = get_engine(user["id"])

    import asyncio

    async def event_generator():
        while True:
            if await request.is_disconnected():
                break
            state = eng.get_state()
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
# ROUTES — EXPORT (PER USER)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/export/{status_filter}")
async def export_logs(status_filter: str, request: Request):
    """Export campaign logs as CSV for the current user."""
    user = require_user(request)
    with get_db() as conn:
        if status_filter in ("sent", "failed"):
            rows = conn.execute(
                "SELECT phone, name, message, status, error_msg, sim_used, sent_at FROM sms_logs WHERE user_id = ? AND status = ? ORDER BY id DESC",
                (user["id"], status_filter),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT phone, name, message, status, error_msg, sim_used, sent_at FROM sms_logs WHERE user_id = ? ORDER BY id DESC",
                (user["id"],),
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
# ROUTES — HISTORY (PER USER)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/history")
async def campaign_history(request: Request):
    user = require_user(request)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM campaigns WHERE user_id = ? ORDER BY created_at DESC LIMIT 20",
            (user["id"],),
        ).fetchall()
    return {"campaigns": [dict(r) for r in rows]}


@app.get("/api/history/{campaign_id}/logs")
async def campaign_logs(campaign_id: str, request: Request):
    user = require_user(request)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT phone, name, status, error_msg, sim_used, sent_at FROM sms_logs WHERE user_id = ? AND campaign_id = ? ORDER BY id",
            (user["id"], campaign_id),
        ).fetchall()
    return {"logs": [dict(r) for r in rows]}


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    local_ip = get_local_ip()
    print("=" * 65)
    print(" 🚀 SMS Campaign & CRM Dashboard (Ko'p foydalanuvchili rejim)")
    print(f" 👉 Kompyuter brauzerida:      http://localhost:8000")
    print(f" 👉 Asosiy admin login:        admin")
    print(f" 👉 Birlamchi parol:           admin123")
    print(f" 📱 Android ilova / Telefon:   http://{local_ip}:8000")
    print(" ⚠️  DIQQAT: Brauzerga '0.0.0.0:8000' deb yozmang (xatolik beradi)!")
    print("=" * 65)
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

