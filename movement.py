#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
movement.py — загрузка треков из Wialon-подобного интерфейса (ASP.NET) в MongoDB.

Рабочая схема (из твоего ноутбука):
- GET  {BASE}/login.aspx  -> вытащить __VIEWSTATE (+ optional __EVENTVALIDATION, __VIEWSTATEGENERATOR)
- POST {BASE}/login.aspx  __EVENTTARGET=lbEnter + tbLogin/tbPassword + TimeZone=3 + ...
- Сохранить cookies (.ASPXAUTH и ASP.NET_SessionId) в cookie.txt
- GET  {BASE}/api/Api.svc/track?oid=...&from=...&to=...  с заголовком Cookie и Referer MileageReportData.aspx

Особенности этой версии:
- chunking по времени (по умолчанию 6 часов)
- sync_state хранится по каждому oid: _id = f"{STATE_ID}:{oid}"
- upsert ключ точек: (oid, tm)  -> стабильно при перезагрузке
- track_key общий на весь запуск: f"{oid}|{RUN_FROM}|{RUN_TO}"
"""

from __future__ import annotations

import os
import re
import json
import time
import argparse
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError


# =========================
# CONFIG (ENV)
# =========================

BASE = os.getenv("BASE", "http://109.195.2.91").strip().rstrip("/")
LOGIN = os.getenv("LOGIN", "volovo").strip()
PASSWORD = os.getenv("PASSWORD", "Vol170717").strip()

# Где хранить cookie
COOKIE_PATH = Path(os.getenv("COOKIE_PATH", str(Path.home() / "Документы" / "cookie.txt")))

# Mongo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017").strip()
DB_NAME = os.getenv("DB_NAME", "volovo").strip()
COL_POINTS = os.getenv("COL_POINTS", "track_points").strip()

STATE_COL = os.getenv("STATE_COL", "sync_state").strip()
STATE_ID = os.getenv("STATE_ID", "track_points_sync").strip()

# По умолчанию OIDs из ENV (через запятую)
OIDS_ENV = os.getenv("OIDS", "").strip()

# Чанки / сеть
DEFAULT_CHUNK_HOURS = int(os.getenv("CHUNK_HOURS", "6").strip() or "6")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60").strip() or "60")
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3").strip() or "3")
HTTP_RETRY_SLEEP = float(os.getenv("HTTP_RETRY_SLEEP", "2").strip() or "2")
REQUEST_SLEEP = float(os.getenv("REQUEST_SLEEP", "0").strip() or "0")

# Отладка
DEBUG = os.getenv("DEBUG", "0").strip().lower() in ("1", "true", "yes", "y")


# =========================
# UTILS
# =========================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(s: str) -> datetime:
    """
    Ожидаемый формат системы: "YYYY-MM-DD HH:MM:SS"
    (как у тебя в запросах).
    """
    s = s.strip().replace("T", " ")
    # убрать миллисекунды/таймзоны если вдруг есть
    s = re.sub(r"\.\d+.*$", "", s)
    s = re.sub(r"(Z|[+-]\d{2}:?\d{2})$", "", s).strip()
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def fmt_dt(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def start_of_month_local() -> datetime:
    # используем локальный "наивный" формат как в системе
    n = datetime.now()
    return n.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def iter_chunks(dt_from: datetime, dt_to: datetime, hours: int) -> List[Tuple[datetime, datetime]]:
    out: List[Tuple[datetime, datetime]] = []
    step = timedelta(hours=max(1, hours))
    cur = dt_from
    while cur < dt_to:
        nxt = min(dt_to, cur + step)
        out.append((cur, nxt))
        cur = nxt
    return out


def parse_oids(s: str) -> List[int]:
    s = s.strip()
    if not s:
        return []
    oids: List[int] = []
    for x in re.split(r"[,\s;]+", s):
        x = x.strip()
        if not x:
            continue
        if not x.isdigit():
            raise ValueError(f"OID не число: {x}")
        oids.append(int(x))
    return sorted(set(oids))


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None


def _hidden(html: str, name: str) -> Optional[str]:
    m = re.search(
        rf'<input[^>]+name="{re.escape(name)}"[^>]+value="([^"]*)"',
        html,
        flags=re.I,
    )
    return m.group(1) if m else None


def cookie_line_from_jar(jar: requests.cookies.RequestsCookieJar) -> str:
    return "; ".join([f"{c.name}={c.value}" for c in jar])


def load_cookie_line() -> Optional[str]:
    try:
        s = COOKIE_PATH.read_text(encoding="utf-8").strip()
        return s if s else None
    except FileNotFoundError:
        return None


def save_cookie_line(cookie_line: str) -> None:
    COOKIE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COOKIE_PATH.write_text(cookie_line, encoding="utf-8")


# =========================
# AUTH + FETCH
# =========================

def login_and_get_cookie_line(session: requests.Session) -> str:
    """
    Логин по схеме из твоего ноутбука. Возвращает строку cookie "a=b; c=d".
    """
    r1 = session.get(f"{BASE}/login.aspx", timeout=HTTP_TIMEOUT)
    r1.raise_for_status()
    html = r1.text

    viewstate = _hidden(html, "__VIEWSTATE")
    eventvalidation = _hidden(html, "__EVENTVALIDATION")
    viewstategenerator = _hidden(html, "__VIEWSTATEGENERATOR")

    if not viewstate:
        raise RuntimeError("Не нашёл __VIEWSTATE на login.aspx (форма изменилась?)")

    data = {
        "__EVENTTARGET": "lbEnter",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "TimeZone": "3",
        "tbLogin": LOGIN,
        "tbPassword": PASSWORD,
        "ddlLanguage": "ru-ru",
        "CheckNewInterface": "on",
    }
    if eventvalidation:
        data["__EVENTVALIDATION"] = eventvalidation
    if viewstategenerator:
        data["__VIEWSTATEGENERATOR"] = viewstategenerator

    r2 = session.post(
        f"{BASE}/login.aspx",
        data=data,
        headers={
            "Origin": BASE,
            "Referer": f"{BASE}/login.aspx",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0",
        },
        allow_redirects=False,
        timeout=HTTP_TIMEOUT,
    )

    if r2.status_code not in (302, 303):
        raise RuntimeError(f"Логин неуспешен: HTTP {r2.status_code}")

    loc = r2.headers.get("Location")
    if loc:
        # добираем редирект, чтобы докрутить сессию
        session.get(f"{BASE}{loc}", timeout=HTTP_TIMEOUT)

    cookie_line = cookie_line_from_jar(session.cookies)

    if "ASP.NET_SessionId=" not in cookie_line:
        raise RuntimeError("Не получил ASP.NET_SessionId — сессия не установилась.")
    if ".ASPXAUTH=" not in cookie_line:
        raise RuntimeError("Не получил .ASPXAUTH — проверь логин/пароль.")

    save_cookie_line(cookie_line)
    return cookie_line


def ensure_cookie_line(session: requests.Session, cookie_line: Optional[str]) -> str:
    """
    Если cookie нет или она протухла — перелогиниваемся.
    """
    if not cookie_line:
        if DEBUG:
            print("[DEBUG] cookie отсутствуют — логинюсь")
        return login_and_get_cookie_line(session)

    # Быстрый тест: открываем страницу отчёта (часто требует авторизацию)
    try:
        r = session.get(
            f"{BASE}/MileageReportData.aspx",
            headers={"Cookie": cookie_line, "User-Agent": "Mozilla/5.0"},
            timeout=HTTP_TIMEOUT,
            allow_redirects=False,
        )
        # Если редирект на login или 401/403 — перелогин
        if r.status_code in (401, 403) or ("login.aspx" in (r.headers.get("Location") or "").lower()):
            if DEBUG:
                print("[DEBUG] cookie протухли — перелогинюсь")
            return login_and_get_cookie_line(session)
        return cookie_line
    except Exception:
        # На всякий — перелогин
        if DEBUG:
            print("[DEBUG] не смог проверить cookie — перелогинюсь")
        return login_and_get_cookie_line(session)


def http_get_retry(url: str, headers: Dict[str, str]) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt < HTTP_RETRIES:
                time.sleep(HTTP_RETRY_SLEEP)
            else:
                raise
    raise last_err  # type: ignore


def fetch_track(cookie_line: str, oid: int, dt_from: str, dt_to: str) -> Dict[str, Any]:
    """
    Реальный endpoint из твоего ноутбука:
      {BASE}/api/Api.svc/track?oid=...&from=...&to=...
    """
    url = (
        f"{BASE}/api/Api.svc/track"
        f"?oid={oid}"
        f"&from={quote(dt_from)}"
        f"&to={quote(dt_to)}"
    )

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{BASE}/MileageReportData.aspx",
        "Cookie": cookie_line,
        "User-Agent": "Mozilla/5.0",
    }
    r = http_get_retry(url, headers=headers)
    return r.json()


# =========================
# MONGO
# =========================

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
points_col = db[COL_POINTS]
state_col = db[STATE_COL]

# Уникальность по (oid, tm) — чтобы повторные загрузки не плодили дубликаты
# (tm приходит из источника и стабилен)
try:
    points_col.create_index([("oid", ASCENDING), ("tm", ASCENDING)], unique=True, name="uniq_oid_tm")
except Exception:
    pass


def state_key_for_oid(oid: int) -> str:
    return f"{STATE_ID}:{oid}"


def get_last_dt_for_oid(oid: int) -> datetime:
    row = state_col.find_one({"_id": state_key_for_oid(oid)})
    if not row or not row.get("last_dt"):
        dt = start_of_month_local()
        state_col.update_one({"_id": state_key_for_oid(oid)}, {"$set": {"last_dt": fmt_dt(dt)}}, upsert=True)
        return dt
    try:
        return parse_dt(str(row["last_dt"]))
    except Exception:
        dt = start_of_month_local()
        return dt


def set_last_dt_for_oid(oid: int, dt: datetime) -> None:
    state_col.update_one(
        {"_id": state_key_for_oid(oid)},
        {"$set": {"last_dt": fmt_dt(dt)}},
        upsert=True,
    )


@dataclass
class ParsedPoint:
    lat: float
    lon: float
    tm: str
    speed: Optional[float]
    st: Any
    dir_: Any
    dst: Any
    width: Any


def parse_coords(coords: Any) -> List[ParsedPoint]:
    """
    coords обычно list[list] формата:
      [dir, dst, lat, lon, speed, st, tm, width]
    либо list[dict]
    """
    out: List[ParsedPoint] = []
    if not coords:
        return out

    for row in coords:
        if isinstance(row, list):
            dir_ = row[0] if len(row) > 0 else None
            dst = row[1] if len(row) > 1 else None
            lat = _to_float(row[2] if len(row) > 2 else None)
            lon = _to_float(row[3] if len(row) > 3 else None)
            speed = _to_float(row[4] if len(row) > 4 else None)
            st = row[5] if len(row) > 5 else None
            tm = row[6] if len(row) > 6 else None
            width = row[7] if len(row) > 7 else None
        elif isinstance(row, dict):
            dir_ = row.get("dir")
            dst = row.get("dst")
            lat = _to_float(row.get("lat"))
            lon = _to_float(row.get("lon"))
            speed = _to_float(row.get("speed"))
            st = row.get("st")
            tm = row.get("tm")
            width = row.get("width")
        else:
            continue

        if lat is None or lon is None or tm in (None, ""):
            continue

        out.append(
            ParsedPoint(
                lat=float(lat),
                lon=float(lon),
                tm=str(tm),
                speed=speed,
                st=st,
                dir_=dir_,
                dst=dst,
                width=width,
            )
        )
    return out


def build_ops(
    oid: int,
    run_from: str,
    run_to: str,
    src_from: str,
    src_to: str,
    pts: List[ParsedPoint],
) -> List[UpdateOne]:
    """
    Upsert по (oid, tm) — стабильно между перезапусками.
    track_key общий для всего запуска (run_from/run_to), чтобы удобно фильтровать.
    """
    now = now_utc()
    track_key = f"{oid}|{run_from}|{run_to}"

    ops: List[UpdateOne] = []
    for idx, p in enumerate(pts):
        doc_set = {
            "track_key": track_key,
            "oid": int(oid),
            "tm": p.tm,
            "lat": p.lat,
            "lon": p.lon,
            "speed": p.speed,
            "st": p.st,
            "dir": p.dir_,
            "dst": p.dst,
            "width": p.width,
            # окна источника (чанк)
            "src_window_from": src_from,
            "src_window_to": src_to,
            "updated_at": now,
        }
        ops.append(
            UpdateOne(
                {"oid": int(oid), "tm": p.tm},
                {"$set": doc_set, "$setOnInsert": {"created_at": now}},
                upsert=True,
            )
        )
    return ops


def bulk_write_safe(ops: List[UpdateOne]) -> Tuple[int, int, int]:
    if not ops:
        return (0, 0, 0)
    try:
        res = points_col.bulk_write(ops, ordered=False)
        upserted = len(res.upserted_ids) if res.upserted_ids else 0
        return (res.matched_count, res.modified_count, upserted)
    except BulkWriteError as e:
        # возможны дубликаты tm, если источник отдаёт повторения в стыке чанков
        if DEBUG:
            print("[WARN] BulkWriteError (trim):", str(e.details)[:1200])
        details = e.details or {}
        return (
            int(details.get("nMatched", 0)),
            int(details.get("nModified", 0)),
            int(details.get("nUpserted", 0)),
        )


# =========================
# MAIN
# =========================

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--oids", default="", help="Список oid через запятую (перекрывает ENV OIDS)")
    ap.add_argument("--from", dest="dt_from", default="", help='Напр: "2026-02-01 00:00:00"')
    ap.add_argument("--to", dest="dt_to", default="", help='Напр: "2026-02-08 23:59:59"')
    ap.add_argument("--reset-state", action="store_true", help="Сбросить sync_state и загрузить заново")
    ap.add_argument("--chunk-hours", type=int, default=DEFAULT_CHUNK_HOURS, help="Размер чанка в часах")
    ap.add_argument("--save-raw", action="store_true", help="Сохранять JSON-ответы в ./tracks_raw/")
    args = ap.parse_args()

    oids = parse_oids(args.oids) if args.oids.strip() else parse_oids(OIDS_ENV)
    if not oids:
        print("❌ Нет OID. Укажи ENV OIDS='182,716' или --oids '182,716'")
        return 2

    forced_from = parse_dt(args.dt_from) if args.dt_from else None
    forced_to = parse_dt(args.dt_to) if args.dt_to else None

    if forced_from and forced_to and forced_to <= forced_from:
        print("❌ Некорректный период: to <= from")
        return 2

    # Подготовка HTTP / cookie
    session = requests.Session()
    cookie_line = load_cookie_line()
    cookie_line = ensure_cookie_line(session, cookie_line)
    print(f"✅ Cookie готовы: {COOKIE_PATH}")

    # Опционально папка raw
    raw_dir = Path("tracks_raw")
    if args.save_raw:
        raw_dir.mkdir(parents=True, exist_ok=True)

    total_pts = 0
    total_upserted = 0
    total_matched = 0
    total_modified = 0
    per_oid: Dict[int, int] = {}

    for oid in oids:
        # определяем период для oid
        if args.reset_state:
            dt_from = forced_from or start_of_month_local()
        else:
            dt_from = forced_from or get_last_dt_for_oid(oid)

        dt_to = forced_to or datetime.now().replace(microsecond=0)

        run_from = fmt_dt(dt_from)
        run_to = fmt_dt(dt_to)

        print(f"\n=== OID {oid} ===")
        print(f"PERIOD: {run_from} → {run_to} | chunk_hours={args.chunk_hours}")

        chunks = iter_chunks(dt_from, dt_to, args.chunk_hours)
        ops_buf: List[UpdateOne] = []
        BUF_LIMIT = 5000
        cnt_oid = 0

        for (c_from, c_to) in chunks:
            c_from_s = fmt_dt(c_from)
            c_to_s = fmt_dt(c_to)

            # cookie может протухнуть в процессе — проверяем мягко
            cookie_line = ensure_cookie_line(session, cookie_line)

            if DEBUG:
                print(f"[DEBUG] fetch oid={oid} {c_from_s} → {c_to_s}")

            data = fetch_track(cookie_line, oid, c_from_s, c_to_s)

            if args.save_raw:
                (raw_dir / f"track_{oid}_{c_from_s.replace(':','-')}_{c_to_s.replace(':','-')}.json").write_text(
                    json.dumps(data, ensure_ascii=False),
                    encoding="utf-8",
                )

            coords = data.get("coords", [])
            pts = parse_coords(coords)
            cnt_oid += len(pts)
            total_pts += len(pts)

            if DEBUG:
                print(f"[DEBUG] coords_len={len(coords)} parsed_pts={len(pts)} result={data.get('result')}")

            if pts:
                ops_buf.extend(build_ops(oid, run_from, run_to, c_from_s, c_to_s, pts))

            if len(ops_buf) >= BUF_LIMIT:
                matched, modified, upserted = bulk_write_safe(ops_buf)
                total_matched += matched
                total_modified += modified
                total_upserted += upserted
                ops_buf.clear()

            if REQUEST_SLEEP > 0:
                time.sleep(REQUEST_SLEEP)

        # финальный flush
        if ops_buf:
            matched, modified, upserted = bulk_write_safe(ops_buf)
            total_matched += matched
            total_modified += modified
            total_upserted += upserted
            ops_buf.clear()

        per_oid[oid] = cnt_oid
        # сдвигаем last_dt для oid
        set_last_dt_for_oid(oid, dt_to)

        print(f"OID {oid}: points={cnt_oid}")

    print("\n=== DONE ===")
    print("points per oid:", per_oid)
    print(f"✅ Saved points total={total_pts} | matched={total_matched} modified={total_modified} upserted={total_upserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
