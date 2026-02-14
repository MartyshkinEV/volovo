#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from pymongo import MongoClient, UpdateOne


# ===================== CONFIG =====================
BASE = os.getenv("FM_BASE", "http://109.195.2.91").strip().rstrip("/")

# OID'ы по умолчанию
DEFAULT_OIDS = "182,716,717,719,432"
OIDS_ENV = os.getenv("OIDS", DEFAULT_OIDS).strip()

# логин/пароль (лучше хранить в ENV, но оставляю дефолты как ты просил)
FM_LOGIN = os.getenv("FM_LOGIN", "volovo").strip()
FM_PASSWORD = os.getenv("FM_PASSWORD", "Vol170717").strip()

# Mongo (на сервер укажи MONGO_URI через ENV)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017").strip()
DB_NAME = os.getenv("DB_NAME", "volovo").strip()
COL_POINTS = os.getenv("COL_POINTS", "track_points").strip()

COOKIE_TXT = Path(os.getenv("COOKIE_TXT", "/opt/volovo/cookie.txt"))

STATE_ID = os.getenv("STATE_ID", "track_points_sync").strip()
STATE_COL = os.getenv("STATE_COL", "sync_state").strip()

# endpoint точек (подтверждён тестом)
POINTS_URL_TEMPLATE = "{BASE}/api/Api.svc/track?oid={OID}&from={DT1}&to={DT2}"

DEBUG = os.getenv("DEBUG", "0").strip() in ("1", "true", "True", "yes", "YES")


# ===================== HELPERS =====================
def now_dt() -> datetime:
    return datetime.now().replace(microsecond=0)


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_dt_any(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s[: len(f)], f)
        except Exception:
            pass
    return None


def env_oids() -> List[int]:
    out: List[int] = []
    for x in OIDS_ENV.split(","):
        x = x.strip()
        if x.isdigit():
            out.append(int(x))
    return out


def safe_json(resp: requests.Response) -> Any:
    ct = (resp.headers.get("content-type") or "").lower()
    if "json" not in ct:
        head = (resp.text or "")[:300].replace("\n", " ")
        raise RuntimeError(f"Ответ не JSON (content-type={ct}). Первые 300 символов: {head!r}")
    return resp.json()


def read_cookie_file() -> Dict[str, str]:
    if not COOKIE_TXT.exists():
        return {}
    raw = COOKIE_TXT.read_text(encoding="utf-8", errors="ignore").strip().strip(";")
    if not raw:
        return {}
    out: Dict[str, str] = {}
    for part in [p.strip() for p in raw.split(";") if p.strip()]:
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def save_cookie_line(cookie_line: str) -> None:
    COOKIE_TXT.parent.mkdir(parents=True, exist_ok=True)
    COOKIE_TXT.write_text(cookie_line.strip(), encoding="utf-8")


def make_session() -> requests.Session:
    s = requests.Session()
    cookies = read_cookie_file()
    if cookies:
        s.cookies.update(cookies)
    s.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "volovo-sync/1.0",
        }
    )
    return s


# ===================== LOGIN (обновление cookie) =====================
def _hidden(html: str, name: str) -> Optional[str]:
    m = re.search(rf'<input[^>]+name="{re.escape(name)}"[^>]+value="([^"]*)"', html, flags=re.I)
    return m.group(1) if m else None


def cookie_line_from_jar(jar: requests.cookies.RequestsCookieJar) -> str:
    return "; ".join([f"{c.name}={c.value}" for c in jar])


def login_and_save_cookie() -> str:
    if not FM_LOGIN or not FM_PASSWORD:
        raise RuntimeError("Не заданы FM_LOGIN/FM_PASSWORD")

    s = requests.Session()

    r1 = s.get(f"{BASE}/login.aspx", timeout=30)
    r1.raise_for_status()
    html = r1.text

    viewstate = _hidden(html, "__VIEWSTATE")
    eventvalidation = _hidden(html, "__EVENTVALIDATION")
    viewstategenerator = _hidden(html, "__VIEWSTATEGENERATOR")
    if not viewstate:
        raise RuntimeError("Не нашёл __VIEWSTATE на login.aspx")

    data = {
        "__EVENTTARGET": "lbEnter",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "TimeZone": "3",
        "tbLogin": FM_LOGIN,
        "tbPassword": FM_PASSWORD,
        "ddlLanguage": "ru-ru",
        "CheckNewInterface": "on",
    }
    if eventvalidation:
        data["__EVENTVALIDATION"] = eventvalidation
    if viewstategenerator:
        data["__VIEWSTATEGENERATOR"] = viewstategenerator

    r2 = s.post(
        f"{BASE}/login.aspx",
        data=data,
        headers={
            "Origin": BASE,
            "Referer": f"{BASE}/login.aspx",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "volovo-sync/1.0",
        },
        allow_redirects=False,
        timeout=30,
    )
    if r2.status_code not in (302, 303):
        raise RuntimeError(f"Логин неуспешен: HTTP {r2.status_code}")

    loc = r2.headers.get("Location")
    if loc:
        try:
            s.get(f"{BASE}{loc}", timeout=30)
        except Exception:
            pass

    cookie_line = cookie_line_from_jar(s.cookies)
    if "ASP.NET_SessionId=" not in cookie_line:
        raise RuntimeError("Не получил ASP.NET_SessionId — сессия не установилась.")

    save_cookie_line(cookie_line)
    return cookie_line


def refresh_session_cookie(session: requests.Session) -> None:
    login_and_save_cookie()
    session.cookies.clear()
    session.cookies.update(read_cookie_file())


# ===================== MONGO =====================
mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
points_col = db[COL_POINTS]
state_col = db[STATE_COL]


def get_last_dt() -> datetime:
    row = state_col.find_one({"_id": STATE_ID})
    if not row or not row.get("last_dt"):
        dt = now_dt().replace(day=1, hour=0, minute=0, second=0)
        state_col.update_one({"_id": STATE_ID}, {"$set": {"last_dt": fmt_dt(dt)}}, upsert=True)
        return dt
    dt = parse_dt_any(row["last_dt"])
    if not dt:
        dt = now_dt().replace(day=1, hour=0, minute=0, second=0)
    return dt


def set_last_dt(dt: datetime) -> None:
    state_col.update_one({"_id": STATE_ID}, {"$set": {"last_dt": fmt_dt(dt)}}, upsert=True)


# ===================== POINTS FETCH =====================
def fetch_points_for_oid(session: requests.Session, oid: int, dt_from: datetime, dt_to: datetime) -> List[Dict[str, Any]]:
    dt1 = quote(fmt_dt(dt_from))
    dt2 = quote(fmt_dt(dt_to))
    url = POINTS_URL_TEMPLATE.format(BASE=BASE.rstrip("/"), OID=oid, DT1=dt1, DT2=dt2)

    def _do() -> Any:
        r = session.get(
            url,
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json,*/*"},
            timeout=180,
        )
        r.raise_for_status()
        return safe_json(r)

    if DEBUG:
        print(f"DEBUG points url: {url}")

    data = _do()

    # ✅ важно: бывает HTTP 200, но result=NoAuth
    if isinstance(data, dict) and str(data.get("result", "")).lower() == "noauth":
        refresh_session_cookie(session)
        data = _do()

    if not isinstance(data, dict):
        return []

    # если всё равно NoAuth — просто нет доступа (чтобы не падать)
    if str(data.get("result", "")).lower() == "noauth":
        if DEBUG:
            print(f"DEBUG oid={oid}: still NoAuth")
        return []

    coords = data.get("coords", [])
    if not isinstance(coords, list):
        return []

    out: List[Dict[str, Any]] = []
    for p in coords:
        if not isinstance(p, dict):
            continue
        lat = p.get("lat")
        lon = p.get("lon")
        tm = p.get("tm") or p.get("dt") or p.get("time")
        if lat is None or lon is None or not tm:
            continue
        out.append(
            {
                "lat": float(lat),
                "lon": float(lon),
                "tm": str(tm),
                "speed": p.get("speed"),
                "dir": p.get("dir"),
                "dst": p.get("dst"),
                "st": p.get("st"),
                "width": p.get("width"),
            }
        )
    return out


def normalize_point(oid: int, dt_from: datetime, dt_to: datetime, idx: int, p: Dict[str, Any]) -> Dict[str, Any]:
    track_key = f"{oid}|{fmt_dt(dt_from)}|{fmt_dt(dt_to)}"
    doc: Dict[str, Any] = {
        "_id": f"{track_key}|{idx}",
        "track_key": track_key,
        "oid": int(oid),
        "idx": int(idx),
        "lat": float(p["lat"]),
        "lon": float(p["lon"]),
        "tm": str(p["tm"]),
        "dt_from": fmt_dt(dt_from),
        "dt_to": fmt_dt(dt_to),
        "created_at": now_dt(),
        "updated_at": now_dt(),
    }
    for k in ("speed", "dir", "dst", "st", "width"):
        if k in p and p[k] is not None:
            doc[k] = p[k]
    return doc


# ===================== MAIN =====================
def main():
    print("=== START SYNC ===")

    oids = env_oids()
    if not oids:
        print("❌ OIDS пустые")
        return

    dt_from = get_last_dt()
    dt_to = now_dt()
    print(f"Диапазон загрузки: {fmt_dt(dt_from)} → {fmt_dt(dt_to)}")

    session = make_session()

    # ✅ ключевой фикс: ВСЕГДА обновляем авторизацию перед синком
    refresh_session_cookie(session)
    print("✅ Авторизация обновлена (cookie.txt перезаписан)")

    ops: List[UpdateOne] = []
    total_pts = 0
    per_oid: Dict[int, int] = {}

    for oid in oids:
        pts = fetch_points_for_oid(session, oid=oid, dt_from=dt_from, dt_to=dt_to)
        per_oid[oid] = len(pts)
        if DEBUG:
            print(f"DEBUG oid={oid} points={len(pts)}")
        if not pts:
            continue

        for i, p in enumerate(pts):
            doc = normalize_point(oid=oid, dt_from=dt_from, dt_to=dt_to, idx=i, p=p)
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
            total_pts += 1

    print("points per oid:", per_oid)

    if ops:
        res = points_col.bulk_write(ops, ordered=False)
        upserted = len(res.upserted_ids) if res.upserted_ids else 0
        print(
            f"✅ Сохранено точек: {total_pts} | "
            f"matched={res.matched_count} modified={res.modified_count} upserted={upserted}"
        )
        set_last_dt(dt_to)
    else:
        print("❌ Точек не получено ни по одному oid. last_dt НЕ сдвигаю (чтобы не потерять период).")

    print("=== DONE ===")


if __name__ == "__main__":
    main()
