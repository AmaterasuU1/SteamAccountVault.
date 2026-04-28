import base64
import copy
import hashlib
import hmac
import json
import logging
import math
import os
import shutil
import struct
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import customtkinter as ctk
from tkinter import filedialog, messagebox


# =========================
# 2. constants/settings
# =========================
APP_NAME = "SteamAccountVault"
APP_GEOMETRY = "1540x930"
PAGE_SIZE = 100

DATA_DIR = Path("data")
MAFILES_DIR = DATA_DIR / "mafiles"
BACKUPS_DIR = DATA_DIR / "backups"
ACCOUNTS_PATH = DATA_DIR / "accounts.json"
LOG_PATH = Path("manager.log")

STATUSES = {
    "EMAIL_ONLY": {"label": "Email only", "color": "#80838A"},
    "NO_LOGIN": {"label": "No login", "color": "#EF4444"},
    "SETUP_DONE": {"label": "Setup done", "color": "#3B82F6"},
    "CS_LVL2": {"label": "CS lvl 2", "color": "#EAB308"},
    "READY": {"label": "Ready", "color": "#22C55E"},
}

TABS = ["All", "Ready", "CS lvl 2", "Setup done", "No login", "Email only", "Leaderboard"]
TAB_TO_STATUS = {
    "Ready": "READY",
    "CS lvl 2": "CS_LVL2",
    "Setup done": "SETUP_DONE",
    "No login": "NO_LOGIN",
    "Email only": "EMAIL_ONLY",
}

SORT_FIELDS = [
    "steam_level",
    "inventory_value_usd",
    "weekly_value_usd",
    "total_profit_usd",
    "status",
    "created_at",
    "updated_at",
    "nickname",
    "steam_login",
]

SAFE_LOG_KEYS = {
    "steam_login",
    "email_login",
    "nickname",
    "status",
    "pair_group",
    "id",
}

STEAM_GUARD_ALPHABET = "23456789BCDFGHJKMNPQRTVWXY"


# =========================
# 3. utility functions
# =========================
def setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8",
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def now_timestamp_ms() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]


def safe_log_account(account: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in account.items() if k in SAFE_LOG_KEYS}


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MAFILES_DIR.mkdir(parents=True, exist_ok=True)
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def week_bounds_wed_start(dt: Optional[datetime] = None) -> Tuple[str, str]:
    dt = dt or datetime.now()
    weekday = dt.weekday()  # 0=Mon .. 2=Wed
    days_since_wed = (weekday - 2) % 7
    week_start = (dt - timedelta(days=days_since_wed)).date()
    week_end = week_start + timedelta(days=6)
    return week_start.isoformat(), week_end.isoformat()


# =========================
# 4. atomic JSON storage
# =========================
def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dirs()
    tmp_fd, tmp_name = tempfile.mkstemp(prefix="accounts_", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as tmpf:
            json.dump(payload, tmpf, ensure_ascii=False, indent=2)
            tmpf.flush()
            os.fsync(tmpf.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def load_db_or_recover() -> Dict[str, Any]:
    ensure_dirs()
    default_db = {"accounts": []}

    if not ACCOUNTS_PATH.exists():
        atomic_write_json(ACCOUNTS_PATH, default_db)
        return default_db

    try:
        with ACCOUNTS_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
            if not isinstance(raw, dict) or "accounts" not in raw or not isinstance(raw["accounts"], list):
                raise ValueError("accounts.json has invalid shape")
            return raw
    except Exception as exc:
        logging.exception("Failed to load accounts.json. Recovering. Error=%s", exc)
        backup_corrupted_accounts()
        atomic_write_json(ACCOUNTS_PATH, default_db)
        return default_db


# =========================
# 5. backup functions
# =========================
def backup_accounts() -> Optional[Path]:
    if not ACCOUNTS_PATH.exists():
        return None
    ensure_dirs()
    dst = BACKUPS_DIR / f"accounts_backup_{now_timestamp_ms()}.json"
    shutil.copy2(ACCOUNTS_PATH, dst)
    return dst


def backup_corrupted_accounts() -> Optional[Path]:
    if not ACCOUNTS_PATH.exists():
        return None
    ensure_dirs()
    dst = BACKUPS_DIR / f"accounts_corrupted_{now_timestamp_ms()}.json"
    shutil.copy2(ACCOUNTS_PATH, dst)
    return dst


# =========================
# 6. account model helpers
# =========================
def infer_default_status(steam_login: str, email_login: str) -> str:
    if steam_login.strip():
        return "NO_LOGIN"
    if email_login.strip():
        return "EMAIL_ONLY"
    return "NO_LOGIN"


def calc_income_sum(account: Dict[str, Any]) -> float:
    total = 0.0
    for item in account.get("income_history", []):
        if isinstance(item, dict):
            total += parse_float(item.get("value_usd", 0.0), 0.0)
    return round(total, 2)


def recalc_total_profit(account: Dict[str, Any]) -> float:
    income_sum = calc_income_sum(account)
    inventory = parse_float(account.get("inventory_value_usd", 0.0), 0.0)
    purchase = parse_float(account.get("purchase_price_usd", 15.0), 15.0)
    sold = bool(account.get("sold", False))
    sold_price = parse_float(account.get("sold_price_usd", 0.0), 0.0)
    if sold:
        total = sold_price + income_sum + inventory - purchase
    else:
        total = income_sum + inventory - purchase
    account["total_profit_usd"] = round(total, 2)
    return account["total_profit_usd"]


def calc_stars(account: Dict[str, Any]) -> str:
    steam_level = int(parse_float(account.get("steam_level", 0), 0))
    econ = parse_float(account.get("inventory_value_usd", 0.0), 0.0) + calc_income_sum(account)
    base = parse_float(account.get("base_cost_usd", 15.0), 15.0)

    if steam_level >= 10 and econ >= base:
        return "⭐⭐⭐"
    if econ >= base:
        return "⭐⭐"
    if steam_level >= 1:
        return "⭐"
    return ""


def normalize_account(raw: Dict[str, Any]) -> Dict[str, Any]:
    created = raw.get("created_at") or now_iso()
    steam_login = str(raw.get("steam_login", "") or "").strip()
    email_login = str(raw.get("email_login", "") or "").strip()
    status = raw.get("status") or infer_default_status(steam_login, email_login)

    account = {
        "id": raw.get("id") or str(uuid.uuid4()),
        "sync_id": raw.get("sync_id") or "",  # Future sync can be implemented with backend + database, not in local MVP.
        "steam_login": steam_login,
        "steam_password": str(raw.get("steam_password", "") or ""),
        "email_login": email_login,
        "email_password": str(raw.get("email_password", "") or ""),
        "nickname": str(raw.get("nickname", "") or ""),
        "status": status if status in STATUSES else infer_default_status(steam_login, email_login),
        "steam_level": int(parse_float(raw.get("steam_level", 0), 0)),
        "inventory_value_usd": round(parse_float(raw.get("inventory_value_usd", 0.0), 0.0), 2),
        "weekly_value_usd": round(parse_float(raw.get("weekly_value_usd", 0.0), 0.0), 2),
        "farmed_this_week": bool(raw.get("farmed_this_week", False)),
        "base_cost_usd": round(parse_float(raw.get("base_cost_usd", 15.0), 15.0), 2),
        "purchase_price_usd": round(parse_float(raw.get("purchase_price_usd", 15.0), 15.0), 2),
        "sold": bool(raw.get("sold", False)),
        "sold_at": raw.get("sold_at") or "",
        "sold_price_usd": round(parse_float(raw.get("sold_price_usd", 0.0), 0.0), 2),
        "total_profit_usd": round(parse_float(raw.get("total_profit_usd", 0.0), 0.0), 2),
        "pair_group": str(raw.get("pair_group", "") or ""),
        "mafile_path": str(raw.get("mafile_path", "") or ""),
        "shared_secret": str(raw.get("shared_secret", "") or ""),
        "notes": str(raw.get("notes", "") or ""),
        "expanded": bool(raw.get("expanded", False)),
        "created_at": created,
        "updated_at": raw.get("updated_at") or created,
        "income_history": raw.get("income_history") if isinstance(raw.get("income_history"), list) else [],
    }
    recalc_total_profit(account)
    return account


# =========================
# 7. duplicate validation
# =========================
def build_index(accounts: List[Dict[str, Any]]) -> Tuple[set, set]:
    steam_set = {a.get("steam_login", "").strip().lower() for a in accounts if a.get("steam_login", "").strip()}
    email_set = {a.get("email_login", "").strip().lower() for a in accounts if a.get("email_login", "").strip()}
    return steam_set, email_set


def is_duplicate(accounts: List[Dict[str, Any]], steam_login: str, email_login: str, exclude_id: Optional[str] = None) -> Tuple[bool, str]:
    s = steam_login.strip().lower()
    e = email_login.strip().lower()
    for acc in accounts:
        if exclude_id and acc.get("id") == exclude_id:
            continue
        if s and acc.get("steam_login", "").strip().lower() == s:
            return True, "steam_login"
        if e and acc.get("email_login", "").strip().lower() == e:
            return True, "email_login"
    return False, ""


# =========================
# 8. TXT import functions
# =========================
def parse_import_line(line: str) -> Optional[Dict[str, str]]:
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    parts = [p.strip() for p in text.split(":")]

    if len(parts) == 2:
        a, b = parts
        if "@" in a:
            return {"steam_login": "", "steam_password": "", "email_login": a, "email_password": b, "nickname": ""}
        return {"steam_login": a, "steam_password": b, "email_login": "", "email_password": "", "nickname": ""}
    if len(parts) == 4:
        s1, s2, e1, e2 = parts
        return {"steam_login": s1, "steam_password": s2, "email_login": e1, "email_password": e2, "nickname": ""}
    if len(parts) == 5:
        s1, s2, e1, e2, nick = parts
        return {"steam_login": s1, "steam_password": s2, "email_login": e1, "email_password": e2, "nickname": nick}
    return None


def parse_email_only_line(line: str) -> Optional[Dict[str, str]]:
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    parts = [p.strip() for p in text.split(":")]
    if len(parts) != 2:
        return None
    return {"email_login": parts[0], "email_password": parts[1]}


# =========================
# 9. maFile parsing
# =========================
def parse_mafile(mafile_path: Path) -> Tuple[Optional[str], Optional[str]]:
    with mafile_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("maFile JSON root must be object")
    account_name = data.get("account_name")
    shared_secret = data.get("shared_secret")
    return account_name, shared_secret


# =========================
# 10. Steam Guard 2FA generation
# =========================
def generate_steam_guard_code(shared_secret: str, timestamp: Optional[int] = None) -> str:
    if not shared_secret:
        return "No 2FA"
    timestamp = timestamp or int(time.time())
    time_slice = timestamp // 30
    secret = base64.b64decode(shared_secret)
    msg = struct.pack(">Q", time_slice)
    digest = hmac.new(secret, msg, hashlib.sha1).digest()
    start = digest[19] & 0x0F
    full_code = struct.unpack(">I", digest[start:start + 4])[0] & 0x7FFFFFFF

    out = ""
    for _ in range(5):
        out += STEAM_GUARD_ALPHABET[full_code % len(STEAM_GUARD_ALPHABET)]
        full_code //= len(STEAM_GUARD_ALPHABET)
    return out


def seconds_to_next_code() -> int:
    return 30 - (int(time.time()) % 30)


@dataclass
class RowWidgets:
    frame: ctk.CTkFrame
    twofa_label: ctk.CTkLabel


# =========================
# 11. UI class AccountManagerApp
# =========================
class AccountManagerApp(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.title(APP_NAME)
        self.geometry(APP_GEOMETRY)

        setup_logging()
        self.db = load_db_or_recover()
        self.accounts = []
        for raw in self.db.get("accounts", []):
            try:
                self.accounts.append(normalize_account(raw))
            except Exception as exc:
                logging.exception("Bad account skipped id=%s err=%s", raw.get("id", "unknown"), exc)

        self.current_tab = "All"
        self.search_var = ctk.StringVar(value="")
        self.sort_var = ctk.StringVar(value="updated_at")
        self.sort_desc_var = ctk.BooleanVar(value=True)
        self.page = 1
        self.total_pages = 1
        self.leader_sort_var = ctk.StringVar(value="total_profit_usd")
        self.row_widgets: List[RowWidgets] = []

        self._build_ui()
        self.refresh_table()
        self._schedule_2fa_refresh()

    def _build_ui(self) -> None:
        top = ctk.CTkFrame(self)
        top.pack(fill="x", padx=10, pady=(10, 6))

        for tab in TABS:
            btn = ctk.CTkButton(top, text=tab, width=120, command=lambda t=tab: self.set_tab(t))
            btn.pack(side="left", padx=3, pady=4)

        toolbar = ctk.CTkFrame(self)
        toolbar.pack(fill="x", padx=10, pady=6)

        ctk.CTkEntry(toolbar, textvariable=self.search_var, width=260, placeholder_text="Search login/email/nickname/pair/notes").pack(side="left", padx=6)
        self.search_var.trace_add("write", lambda *_: self.on_filter_change())

        ctk.CTkOptionMenu(toolbar, variable=self.sort_var, values=SORT_FIELDS, command=lambda *_: self.on_filter_change()).pack(side="left", padx=6)
        ctk.CTkCheckBox(toolbar, text="Desc", variable=self.sort_desc_var, command=self.on_filter_change).pack(side="left", padx=6)

        ctk.CTkButton(toolbar, text="Add account", command=self.open_add_dialog, width=110).pack(side="left", padx=4)
        ctk.CTkButton(toolbar, text="Import TXT", command=self.import_txt, width=100).pack(side="left", padx=4)
        ctk.CTkButton(toolbar, text="Import Emails TXT", command=self.import_emails_txt, width=140).pack(side="left", padx=4)

        self.table_host = ctk.CTkScrollableFrame(self)
        self.table_host.pack(fill="both", expand=True, padx=10, pady=6)

        pager = ctk.CTkFrame(self)
        pager.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkButton(pager, text="Previous", width=90, command=self.prev_page).pack(side="left", padx=6, pady=6)
        ctk.CTkButton(pager, text="Next", width=90, command=self.next_page).pack(side="left", padx=6, pady=6)
        self.page_label = ctk.CTkLabel(pager, text="Page 1 / 1")
        self.page_label.pack(side="left", padx=12)

    def persist(self) -> bool:
        try:
            backup_accounts()
            atomic_write_json(ACCOUNTS_PATH, {"accounts": self.accounts})
            return True
        except Exception as exc:
            logging.exception("Save failed err=%s", exc)
            messagebox.showerror("Save error", "Could not save accounts.json atomically.")
            return False

    def set_tab(self, tab: str) -> None:
        self.current_tab = tab
        self.page = 1
        self.refresh_table()

    def on_filter_change(self) -> None:
        self.page = 1
        self.refresh_table()

    def get_filtered_accounts(self) -> List[Dict[str, Any]]:
        query = self.search_var.get().strip().lower()
        result = self.accounts

        if self.current_tab in TAB_TO_STATUS:
            status = TAB_TO_STATUS[self.current_tab]
            result = [a for a in result if a.get("status") == status]

        if query:
            def match(a: Dict[str, Any]) -> bool:
                pool = [
                    a.get("steam_login", ""),
                    a.get("email_login", ""),
                    a.get("nickname", ""),
                    a.get("pair_group", ""),
                    a.get("notes", ""),
                ]
                return any(query in str(x).lower() for x in pool)
            result = [a for a in result if match(a)]

        sort_field = self.sort_var.get()
        reverse = bool(self.sort_desc_var.get())

        def sort_key(a: Dict[str, Any]) -> Any:
            v = a.get(sort_field)
            if sort_field in {"steam_level"}:
                return int(parse_float(v, 0))
            if sort_field in {"inventory_value_usd", "weekly_value_usd", "total_profit_usd"}:
                return parse_float(v, 0.0)
            return str(v or "").lower()

        result = sorted(result, key=sort_key, reverse=reverse)
        return result

    def refresh_table(self) -> None:
        for child in self.table_host.winfo_children():
            child.destroy()
        self.row_widgets.clear()

        if self.current_tab == "Leaderboard":
            self._render_leaderboard()
            return

        filtered = self.get_filtered_accounts()
        self.total_pages = max(1, math.ceil(len(filtered) / PAGE_SIZE))
        self.page = min(max(1, self.page), self.total_pages)
        start = (self.page - 1) * PAGE_SIZE
        page_accounts = filtered[start:start + PAGE_SIZE]

        for local_i, account in enumerate(page_accounts, start=1):
            self._render_account_row(account, local_i)

        self.page_label.configure(text=f"Page {self.page} / {self.total_pages}")

    def _render_account_row(self, account: Dict[str, Any], local_num: int) -> None:
        row = ctk.CTkFrame(self.table_host, fg_color="#151922")
        row.pack(fill="x", padx=5, pady=5)

        compact = ctk.CTkFrame(row, fg_color="transparent")
        compact.pack(fill="x", padx=8, pady=6)

        exp_text = "▼" if account.get("expanded") else "▶"
        ctk.CTkButton(compact, text=exp_text, width=28, command=lambda a=account: self.toggle_expand(a)).pack(side="left", padx=4)

        ctk.CTkLabel(compact, text=f"{local_num:03d}", width=44).pack(side="left")
        ctk.CTkLabel(compact, text=calc_stars(account), width=48).pack(side="left", padx=5)
        ctk.CTkLabel(compact, text=account.get("nickname", ""), width=140, anchor="w").pack(side="left", padx=4)
        ctk.CTkLabel(compact, text=account.get("steam_login", ""), width=180, anchor="w").pack(side="left", padx=4)
        ctk.CTkLabel(compact, text=account.get("steam_password", ""), width=150, anchor="w").pack(side="left", padx=4)

        status = account.get("status", "NO_LOGIN")
        status_info = STATUSES.get(status, STATUSES["NO_LOGIN"])
        ctk.CTkLabel(compact, text=status_info["label"], fg_color=status_info["color"], corner_radius=8, width=90).pack(side="left", padx=4)

        twofa_text = "No 2FA"
        if account.get("shared_secret"):
            twofa_text = f"{generate_steam_guard_code(account['shared_secret'])} ({seconds_to_next_code()}s)"
        twofa_label = ctk.CTkLabel(compact, text=twofa_text, width=110)
        twofa_label.pack(side="left", padx=4)

        ctk.CTkButton(compact, text="Copy Login", width=88, command=lambda a=account: self.copy_to_clipboard(a.get("steam_login", ""))).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Copy SPass", width=88, command=lambda a=account: self.copy_to_clipboard(a.get("steam_password", ""))).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Copy Email", width=88, command=lambda a=account: self.copy_to_clipboard(a.get("email_login", ""))).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Copy EPass", width=88, command=lambda a=account: self.copy_to_clipboard(a.get("email_password", ""))).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Copy 2FA", width=80, command=lambda a=account: self.copy_2fa(a)).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Copy Full Pack", width=112, command=lambda a=account: self.copy_full_pack(a)).pack(side="left", padx=2)

        ctk.CTkButton(compact, text="Edit", width=55, command=lambda a=account: self.open_edit_dialog(a)).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Income", width=60, command=lambda a=account: self.open_income_dialog(a)).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Attach maFile", width=95, command=lambda a=account: self.attach_mafile(a)).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Mark Sold", width=75, command=lambda a=account: self.mark_sold(a)).pack(side="left", padx=2)
        ctk.CTkButton(compact, text="Delete", width=60, fg_color="#B91C1C", command=lambda a=account: self.delete_account(a)).pack(side="left", padx=2)

        if account.get("expanded"):
            expanded = ctk.CTkFrame(row, fg_color="#1E2533")
            expanded.pack(fill="x", padx=8, pady=(0, 8))
            line1 = (
                f"Email: {account.get('email_login','')} | Email pass: {account.get('email_password','')} | "
                f"Lvl: {account.get('steam_level',0)} | Inv: ${account.get('inventory_value_usd',0):.2f} | "
                f"Weekly: ${account.get('weekly_value_usd',0):.2f} | Profit: ${account.get('total_profit_usd',0):.2f}"
            )
            ctk.CTkLabel(expanded, text=line1, anchor="w").pack(fill="x", padx=8, pady=4)
            line2 = (
                f"Pair: {account.get('pair_group','')} | Sold: {account.get('sold', False)} | "
                f"Sold at: {account.get('sold_at','')} | Sold price: ${account.get('sold_price_usd',0):.2f}"
            )
            ctk.CTkLabel(expanded, text=line2, anchor="w").pack(fill="x", padx=8, pady=4)
            ctk.CTkLabel(expanded, text=f"Notes: {account.get('notes','')}", anchor="w", wraplength=1300, justify="left").pack(fill="x", padx=8, pady=4)
            if int(parse_float(account.get("steam_level", 0), 0)) >= 10:
                ctk.CTkLabel(expanded, text="Potential base value: $20", text_color="#FBBF24").pack(anchor="w", padx=8, pady=(0, 6))

        self.row_widgets.append(RowWidgets(frame=row, twofa_label=twofa_label))

    def _render_leaderboard(self) -> None:
        head = ctk.CTkFrame(self.table_host)
        head.pack(fill="x", padx=5, pady=5)
        ctk.CTkLabel(head, text="Leaderboard sort:").pack(side="left", padx=6)
        ctk.CTkOptionMenu(head, variable=self.leader_sort_var, values=["total_profit_usd", "weekly_value_usd", "inventory_value_usd"], command=lambda *_: self.refresh_table()).pack(side="left", padx=6)

        sort_key = self.leader_sort_var.get()
        ranked = sorted(self.accounts, key=lambda a: parse_float(a.get(sort_key, 0.0), 0.0), reverse=True)

        for i, acc in enumerate(ranked, start=1):
            r = ctk.CTkFrame(self.table_host, fg_color="#161E2D")
            r.pack(fill="x", padx=5, pady=2)
            txt = (
                f"#{i:03d} | {acc.get('nickname','')} | {acc.get('steam_login','')} | "
                f"Profit ${acc.get('total_profit_usd',0):.2f} | Weekly ${acc.get('weekly_value_usd',0):.2f} | "
                f"Inv ${acc.get('inventory_value_usd',0):.2f} | Lvl {acc.get('steam_level',0)} | {calc_stars(acc)} | "
                f"{acc.get('status','')} | Sold={acc.get('sold',False)}"
            )
            ctk.CTkLabel(r, text=txt, anchor="w").pack(fill="x", padx=8, pady=4)

        self.page_label.configure(text="Page 1 / 1")

    def toggle_expand(self, account: Dict[str, Any]) -> None:
        account["expanded"] = not bool(account.get("expanded", False))
        account["updated_at"] = now_iso()
        if self.persist():
            self.refresh_table()

    def copy_to_clipboard(self, value: str) -> None:
        self.clipboard_clear()
        self.clipboard_append(value or "")

    def copy_2fa(self, account: Dict[str, Any]) -> None:
        if not account.get("shared_secret"):
            messagebox.showinfo("2FA", "No 2FA for this account")
            return
        try:
            code = generate_steam_guard_code(account["shared_secret"])
            self.copy_to_clipboard(code)
        except Exception as exc:
            logging.exception("2FA generation error for account=%s err=%s", account.get("id"), exc)
            messagebox.showerror("2FA", "Failed to generate 2FA code.")

    def copy_full_pack(self, account: Dict[str, Any]) -> None:
        text = "\n".join([
            account.get("steam_login", ""),
            account.get("steam_password", ""),
            account.get("email_login", ""),
            account.get("email_password", ""),
        ])
        self.copy_to_clipboard(text)

    def _save_and_refresh(self) -> None:
        for acc in self.accounts:
            recalc_total_profit(acc)
        if self.persist():
            self.refresh_table()

    def open_add_dialog(self) -> None:
        self._open_account_dialog(mode="add")

    def open_edit_dialog(self, account: Dict[str, Any]) -> None:
        self._open_account_dialog(mode="edit", account=account)

    def _open_account_dialog(self, mode: str, account: Optional[Dict[str, Any]] = None) -> None:
        dlg = ctk.CTkToplevel(self)
        dlg.title("Add account" if mode == "add" else "Edit account")
        dlg.geometry("580x640")

        fields = {
            "steam_login": ctk.StringVar(value=(account or {}).get("steam_login", "")),
            "steam_password": ctk.StringVar(value=(account or {}).get("steam_password", "")),
            "email_login": ctk.StringVar(value=(account or {}).get("email_login", "")),
            "email_password": ctk.StringVar(value=(account or {}).get("email_password", "")),
            "nickname": ctk.StringVar(value=(account or {}).get("nickname", "")),
            "status": ctk.StringVar(value=(account or {}).get("status", "NO_LOGIN")),
            "base_cost_usd": ctk.StringVar(value=str((account or {}).get("base_cost_usd", 15))),
            "pair_group": ctk.StringVar(value=(account or {}).get("pair_group", "")),
        }
        notes_box = ctk.CTkTextbox(dlg, width=540, height=130)
        notes_box.insert("1.0", (account or {}).get("notes", ""))

        for key, var in fields.items():
            row = ctk.CTkFrame(dlg)
            row.pack(fill="x", padx=12, pady=5)
            ctk.CTkLabel(row, text=key, width=130, anchor="w").pack(side="left")
            if key == "status":
                ctk.CTkOptionMenu(row, variable=var, values=list(STATUSES.keys())).pack(side="left", padx=6)
            else:
                ctk.CTkEntry(row, textvariable=var, width=370).pack(side="left", padx=6)

        ctk.CTkLabel(dlg, text="notes", anchor="w").pack(fill="x", padx=14, pady=(8, 0))
        notes_box.pack(padx=12, pady=4)

        def on_save() -> None:
            payload = {k: v.get().strip() for k, v in fields.items()}
            payload["notes"] = notes_box.get("1.0", "end").strip()

            dup, key = is_duplicate(
                self.accounts,
                payload["steam_login"],
                payload["email_login"],
                exclude_id=account.get("id") if account else None,
            )
            if dup:
                messagebox.showwarning("Duplicate", f"Duplicate {key} detected.")
                return

            if mode == "add":
                new_acc = normalize_account({
                    "id": str(uuid.uuid4()),
                    "steam_login": payload["steam_login"],
                    "steam_password": payload["steam_password"],
                    "email_login": payload["email_login"],
                    "email_password": payload["email_password"],
                    "nickname": payload["nickname"],
                    "status": payload["status"] or infer_default_status(payload["steam_login"], payload["email_login"]),
                    "base_cost_usd": parse_float(payload["base_cost_usd"], 15.0),
                    "purchase_price_usd": parse_float(payload["base_cost_usd"], 15.0),
                    "pair_group": payload["pair_group"],
                    "notes": payload["notes"],
                    "created_at": now_iso(),
                    "updated_at": now_iso(),
                })
                self.accounts.append(new_acc)
                logging.info("Account added %s", safe_log_account(new_acc))
            else:
                account["steam_login"] = payload["steam_login"]
                account["steam_password"] = payload["steam_password"]
                account["email_login"] = payload["email_login"]
                account["email_password"] = payload["email_password"]
                account["nickname"] = payload["nickname"]
                account["status"] = payload["status"] or infer_default_status(payload["steam_login"], payload["email_login"])
                account["base_cost_usd"] = round(parse_float(payload["base_cost_usd"], 15.0), 2)
                account["pair_group"] = payload["pair_group"]
                account["notes"] = payload["notes"]
                account["updated_at"] = now_iso()
                recalc_total_profit(account)
                logging.info("Account edited %s", safe_log_account(account))

            self._save_and_refresh()
            dlg.destroy()

        ctk.CTkButton(dlg, text="Save", command=on_save).pack(pady=10)

    def delete_account(self, account: Dict[str, Any]) -> None:
        if not messagebox.askyesno("Confirm", "Delete account permanently from accounts.json?"):
            return
        self.accounts = [a for a in self.accounts if a.get("id") != account.get("id")]
        logging.info("Account deleted id=%s steam_login=%s email_login=%s", account.get("id"), account.get("steam_login", ""), account.get("email_login", ""))
        self._save_and_refresh()

    def mark_sold(self, account: Dict[str, Any]) -> None:
        win = ctk.CTkInputDialog(text="Sold price USD:", title="Mark Sold")
        value = win.get_input()
        if value is None:
            return
        account["sold"] = True
        account["sold_at"] = now_iso()
        account["sold_price_usd"] = round(parse_float(value, 0.0), 2)
        account["updated_at"] = now_iso()
        recalc_total_profit(account)
        logging.info("Account marked sold id=%s steam_login=%s", account.get("id"), account.get("steam_login", ""))
        self._save_and_refresh()

    def open_income_dialog(self, account: Dict[str, Any]) -> None:
        win = ctk.CTkToplevel(self)
        win.title(f"Income History :: {account.get('steam_login','')}")
        win.geometry("760x560")

        list_frame = ctk.CTkScrollableFrame(win)
        list_frame.pack(fill="both", expand=True, padx=10, pady=10)

        def render_list() -> None:
            for c in list_frame.winfo_children():
                c.destroy()
            history = account.get("income_history", [])
            for idx, item in enumerate(history):
                line = ctk.CTkFrame(list_frame)
                line.pack(fill="x", padx=4, pady=4)
                txt = f"{idx+1}. {item.get('week_start','')} - {item.get('week_end','')} | ${parse_float(item.get('value_usd',0),0):.2f} | {item.get('note','')}"
                ctk.CTkLabel(line, text=txt, anchor="w").pack(side="left", padx=6)
                ctk.CTkButton(line, text="Edit", width=60, command=lambda i=idx: edit_record(i)).pack(side="right", padx=4)
                ctk.CTkButton(line, text="Delete", width=70, fg_color="#B91C1C", command=lambda i=idx: delete_record(i)).pack(side="right", padx=4)

        def add_weekly_income() -> None:
            if not account.get("farmed_this_week", False):
                messagebox.showwarning("Blocked", "Set farmed_this_week=true before adding weekly income.")
                return
            val_dialog = ctk.CTkInputDialog(text="Weekly income USD:", title="Add weekly income")
            value = val_dialog.get_input()
            if value is None:
                return
            note_dialog = ctk.CTkInputDialog(text="Note:", title="Weekly note")
            note = note_dialog.get_input() or ""
            week_start, week_end = week_bounds_wed_start()
            item = {
                "week_start": week_start,
                "week_end": week_end,
                "value_usd": round(parse_float(value, 0.0), 2),
                "note": note,
                "created_at": now_iso(),
            }
            account.setdefault("income_history", []).append(item)
            account["weekly_value_usd"] = round(parse_float(item["value_usd"], 0.0), 2)
            account["updated_at"] = now_iso()
            recalc_total_profit(account)
            self._save_and_refresh()
            render_list()

        def edit_record(index: int) -> None:
            history = account.get("income_history", [])
            if index < 0 or index >= len(history):
                return
            old = history[index]
            vd = ctk.CTkInputDialog(text=f"Value USD (old {old.get('value_usd',0)}):", title="Edit record")
            v = vd.get_input()
            if v is None:
                return
            nd = ctk.CTkInputDialog(text="Note:", title="Edit note")
            n = nd.get_input()
            old["value_usd"] = round(parse_float(v, 0.0), 2)
            if n is not None:
                old["note"] = n
            account["updated_at"] = now_iso()
            recalc_total_profit(account)
            self._save_and_refresh()
            render_list()

        def delete_record(index: int) -> None:
            history = account.get("income_history", [])
            if index < 0 or index >= len(history):
                return
            del history[index]
            account["updated_at"] = now_iso()
            account["weekly_value_usd"] = 0.0
            recalc_total_profit(account)
            self._save_and_refresh()
            render_list()

        bottom = ctk.CTkFrame(win)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkButton(bottom, text="Add weekly income", command=add_weekly_income).pack(side="left", padx=6)
        ctk.CTkButton(bottom, text="Close", command=win.destroy).pack(side="right", padx=6)

        render_list()

    def attach_mafile(self, account: Dict[str, Any]) -> None:
        path = filedialog.askopenfilename(filetypes=[("maFile", "*.maFile"), ("JSON", "*.json"), ("All files", "*.*")])
        if not path:
            return
        p = Path(path)
        try:
            account_name, shared_secret = parse_mafile(p)
            account["mafile_path"] = str(p)
            if shared_secret:
                account["shared_secret"] = shared_secret
            if account_name and not account.get("steam_login"):
                account["steam_login"] = account_name
            elif account_name and account.get("steam_login") and account.get("steam_login") != account_name:
                messagebox.showwarning("maFile", f"account_name ({account_name}) differs from steam_login.")
            account["updated_at"] = now_iso()
            logging.info("maFile attached id=%s steam_login=%s mafile=%s", account.get("id"), account.get("steam_login"), p.name)
            self._save_and_refresh()
        except Exception as exc:
            logging.exception("Failed to parse maFile file=%s err=%s", p.name, exc)
            messagebox.showerror("maFile", "Invalid or corrupted maFile.")

    def import_txt(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("Text", "*.txt"), ("All files", "*.*")])
        if not path:
            return
        added, skipped = 0, 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    parsed = parse_import_line(line)
                    if parsed is None:
                        if line.strip() and not line.strip().startswith("#"):
                            logging.warning("Unknown TXT import line skipped: %s", line.strip().split(":")[0])
                        continue

                    dup, key = is_duplicate(self.accounts, parsed["steam_login"], parsed["email_login"])
                    if dup:
                        skipped += 1
                        logging.info("Import duplicate skipped by %s steam_login=%s email_login=%s", key, parsed.get("steam_login", ""), parsed.get("email_login", ""))
                        continue

                    acc = normalize_account({
                        "id": str(uuid.uuid4()),
                        "steam_login": parsed["steam_login"],
                        "steam_password": parsed["steam_password"],
                        "email_login": parsed["email_login"],
                        "email_password": parsed["email_password"],
                        "nickname": parsed.get("nickname", ""),
                        "status": infer_default_status(parsed["steam_login"], parsed["email_login"]),
                        "created_at": now_iso(),
                        "updated_at": now_iso(),
                    })
                    self.accounts.append(acc)
                    added += 1

            self._save_and_refresh()
            messagebox.showinfo("Import TXT", f"Added: {added}\nSkipped: {skipped}")
        except Exception as exc:
            logging.exception("TXT import failed err=%s", exc)
            messagebox.showerror("Import TXT", "Failed to import TXT.")

    def import_emails_txt(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("Text", "*.txt"), ("All files", "*.*")])
        if not path:
            return
        added, skipped = 0, 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    parsed = parse_email_only_line(line)
                    if parsed is None:
                        continue
                    dup, _ = is_duplicate(self.accounts, "", parsed["email_login"])
                    if dup:
                        skipped += 1
                        logging.info("Email import duplicate skipped email=%s", parsed["email_login"])
                        continue

                    acc = normalize_account({
                        "id": str(uuid.uuid4()),
                        "steam_login": "",
                        "steam_password": "",
                        "email_login": parsed["email_login"],
                        "email_password": parsed["email_password"],
                        "nickname": "",
                        "status": "EMAIL_ONLY",
                        "created_at": now_iso(),
                        "updated_at": now_iso(),
                    })
                    self.accounts.append(acc)
                    added += 1

            self._save_and_refresh()
            messagebox.showinfo("Import Emails TXT", f"Added: {added}\nSkipped: {skipped}")
        except Exception as exc:
            logging.exception("Email TXT import failed err=%s", exc)
            messagebox.showerror("Import Emails TXT", "Failed to import emails TXT.")

    def prev_page(self) -> None:
        self.page = max(1, self.page - 1)
        self.refresh_table()

    def next_page(self) -> None:
        self.page = min(self.total_pages, self.page + 1)
        self.refresh_table()

    def _schedule_2fa_refresh(self) -> None:
        try:
            for row in self.row_widgets:
                # Label text is rebuilt on full refresh; this keeps live timer visible between refreshes.
                txt = row.twofa_label.cget("text")
                if "(" in txt and txt.endswith("s)"):
                    left = seconds_to_next_code()
                    base = txt.split("(")[0].strip()
                    row.twofa_label.configure(text=f"{base} ({left}s)")
            if seconds_to_next_code() in {30, 29}:
                self.refresh_table()
        except Exception as exc:
            logging.exception("2FA refresh loop error err=%s", exc)
        finally:
            self.after(1000, self._schedule_2fa_refresh)


# =========================
# 12. main guard
# =========================
if __name__ == "__main__":
    try:
        app = AccountManagerApp()
        app.mainloop()
    except Exception as e:
        setup_logging()
        logging.exception("Fatal app error err=%s", e)
        raise
