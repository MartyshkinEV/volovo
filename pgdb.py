# /opt/volovo/pgdb.py
from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    # psycopg 3
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb as JSONB

    PG3 = True
except Exception:  # fallback to psycopg2
    import psycopg2 as psycopg  # type: ignore
    from psycopg2 import extras  # type: ignore

    PG3 = False
    JSONB = extras.Json  # type: ignore
    # Ensure json/jsonb are decoded to Python objects
    extras.register_default_json(loads=json.loads, globally=True)  # type: ignore
    extras.register_default_jsonb(loads=json.loads, globally=True)  # type: ignore


# Можно задать в окружении:
# export DATABASE_URL="postgresql://volovo_pg@127.0.0.1:5432/volovo"
# или с паролем:
# export DATABASE_URL="postgresql://volovo_pg:PASSWORD@127.0.0.1:5432/volovo"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://volovo_pg@127.0.0.1:5432/volovo")


def _conn():
    # Унифицированное подключение для psycopg3/psycopg2
    return psycopg.connect(DATABASE_URL)


def _dict_cursor(conn):
    # Возвращает cursor с dict-строками независимо от версии драйвера
    if PG3:
        return conn.cursor(row_factory=dict_row)
    else:
        return conn.cursor(cursor_factory=extras.RealDictCursor)  # type: ignore


# ----------------------------
# Routes catalog
# ----------------------------
def fetch_routes() -> List[Dict[str, Any]]:
    sql = """
        SELECT
            id,
            name,
            road_width_m,
            road_length_km,
            pss_tonnage_t
        FROM tracking_routecatalog
        ORDER BY name
    """
    with _conn() as conn, _dict_cursor(conn) as cur:
        cur.execute(sql)
        rows = cur.fetchall()  # list[dict]
    # фронту обычно достаточно name + параметров; id тоже полезен
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "road_width_m": r["road_width_m"],
            "road_length_km": r["road_length_km"],
            "pss_tonnage_t": r["pss_tonnage_t"],
        }
        for r in rows
    ]


# ----------------------------
# OIDs list (distinct trackers)
# ----------------------------
def fetch_oids(limit: int = 5000) -> List[Dict[str, Any]]:
    sql = """
        SELECT oid, COUNT(*)::bigint AS points_cnt
        FROM tracking_trackpoint
        GROUP BY oid
        ORDER BY oid
        LIMIT %s
    """
    with _conn() as conn, _dict_cursor(conn) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    return [{"oid": int(r["oid"]), "points_cnt": int(r["points_cnt"])} for r in rows]


# ----------------------------
# Points
# ----------------------------
def fetch_points(
    oid: int,
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    limit: int = 500_000,
) -> List[Tuple[datetime, float, float, Optional[int]]]:
    """
    Возвращает (tm, lon, lat, idx)
    """

    where = ["oid = %s"]
    params: List[Any] = [oid]

    if dt_from is not None:
        where.append("tm >= %s")
        params.append(dt_from)

    if dt_to is not None:
        where.append("tm <= %s")
        params.append(dt_to)

    params.append(limit)

    sql = f"""
        SELECT
            tm,
            ST_X(geom::geometry) AS lon,
            ST_Y(geom::geometry) AS lat,
            idx
        FROM tracking_trackpoint
        WHERE {" AND ".join(where)}
        ORDER BY tm
        LIMIT %s
    """

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

# ----------------------------
# Forms (putevoy_forms)
# ----------------------------
def insert_form(
    oid: int,
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    payload: Dict[str, Any],
    mongo_id: Optional[str] = None,
) -> int:
    """
    Сохраняет JSON payload в putevoy_forms.payload (jsonb)
    Возвращает ID новой формы.
    """
    sql = """
        INSERT INTO putevoy_forms (created_at, oid, dt_from, dt_to, mongo_id, payload, updated_at)
        VALUES (NOW(), %s, %s, %s, %s, %s::jsonb, NOW())
        RETURNING id
    """
    with _conn() as conn, _dict_cursor(conn) as cur:
        cur.execute(sql, (oid, dt_from, dt_to, mongo_id, JSONB(payload)))
        new_id = cur.fetchone()["id"]
        conn.commit()
        return int(new_id)


def get_form(form_id: int) -> Optional[Dict[str, Any]]:
    """
    Возвращает полную форму из putevoy_forms:
      id, created_at, oid, dt_from, dt_to, mongo_id, payload, updated_at
    """
    sql = """
        SELECT id, created_at, oid, dt_from, dt_to, mongo_id, payload, updated_at
        FROM putevoy_forms
        WHERE id = %s
    """
    with _conn() as conn, _dict_cursor(conn) as cur:
        cur.execute(sql, (form_id,))
        row = cur.fetchone()

    if not row:
        return None

    # row уже dict-подобный
    return {
        "id": int(row["id"]),
        "created_at": row["created_at"],
        "oid": int(row["oid"]) if row["oid"] is not None else None,
        "dt_from": row["dt_from"],
        "dt_to": row["dt_to"],
        "mongo_id": row["mongo_id"],
        "payload": row["payload"] or {},
        "updated_at": row["updated_at"],
    }


def list_forms(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Возвращает список последних форм (упрощённый),
    чтобы фронт мог показать список/выбор.
    """
    sql = """
        SELECT id, created_at, oid, dt_from, dt_to, mongo_id, payload, updated_at
        FROM putevoy_forms
        ORDER BY id DESC
        LIMIT %s
    """
    with _conn() as conn, _dict_cursor(conn) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        payload = r["payload"] or {}
        out.append(
            {
                "id": int(r["id"]),
                "created_at": r["created_at"],
                "oid": int(r["oid"]) if r["oid"] is not None else None,
                "dt_from": r["dt_from"],
                "dt_to": r["dt_to"],
                "mongo_id": r["mongo_id"],
                "payload": payload,
                "updated_at": r["updated_at"],
            }
        )
    return out