"""SQLite read/write helpers for the NPS seasonal model."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pandas as pd

DEFAULT_DB = Path(__file__).parent.parent / "data" / "nps.db"

DDL = """
CREATE TABLE IF NOT EXISTS parks (
    unit_code TEXT PRIMARY KEY,
    name      TEXT NOT NULL,
    state     TEXT,
    type      TEXT
);

CREATE TABLE IF NOT EXISTS monthly_visits (
    unit_code   TEXT    NOT NULL,
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    visit_count INTEGER,
    PRIMARY KEY (unit_code, year, month),
    FOREIGN KEY (unit_code) REFERENCES parks (unit_code)
);

CREATE INDEX IF NOT EXISTS idx_mv_unit ON monthly_visits (unit_code);
CREATE INDEX IF NOT EXISTS idx_mv_year ON monthly_visits (year);
"""


@contextmanager
def get_conn(db_path: Path | str = DEFAULT_DB) -> Generator[sqlite3.Connection, None, None]:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path | str = DEFAULT_DB) -> None:
    with get_conn(db_path) as conn:
        conn.executescript(DDL)


# ── Parks ─────────────────────────────────────────────────────────────────────

def upsert_park(
    conn: sqlite3.Connection,
    unit_code: str,
    name: str,
    state: str | None = None,
    park_type: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO parks (unit_code, name, state, type)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (unit_code) DO UPDATE SET
            name  = excluded.name,
            state = COALESCE(excluded.state, parks.state),
            type  = COALESCE(excluded.type,  parks.type)
        """,
        (unit_code.upper(), name, state, park_type),
    )


def get_all_parks(db_path: Path | str = DEFAULT_DB) -> pd.DataFrame:
    with get_conn(db_path) as conn:
        return pd.read_sql_query("SELECT * FROM parks ORDER BY name", conn)


def get_park(unit_code: str, db_path: Path | str = DEFAULT_DB) -> dict | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM parks WHERE unit_code = ?", (unit_code.upper(),)
        ).fetchone()
    return dict(row) if row else None


# ── Monthly visits ─────────────────────────────────────────────────────────────

def upsert_monthly_visit(
    conn: sqlite3.Connection,
    unit_code: str,
    year: int,
    month: int,
    visit_count: int | None,
) -> None:
    conn.execute(
        """
        INSERT INTO monthly_visits (unit_code, year, month, visit_count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (unit_code, year, month) DO UPDATE SET
            visit_count = excluded.visit_count
        """,
        (unit_code.upper(), year, month, visit_count),
    )


def get_monthly_visits(
    unit_code: str,
    db_path: Path | str = DEFAULT_DB,
    exclude_years: list[int] | None = None,
) -> pd.DataFrame:
    exclude_years = exclude_years or []
    with get_conn(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM monthly_visits WHERE unit_code = ? ORDER BY year, month",
            conn,
            params=(unit_code.upper(),),
        )
    if exclude_years and not df.empty:
        df = df[~df["year"].isin(exclude_years)]
    return df


def get_all_monthly_visits(
    db_path: Path | str = DEFAULT_DB,
    exclude_years: list[int] | None = None,
) -> pd.DataFrame:
    exclude_years = exclude_years or []
    with get_conn(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM monthly_visits ORDER BY unit_code, year, month",
            conn,
        )
    if exclude_years and not df.empty:
        df = df[~df["year"].isin(exclude_years)]
    return df


def get_available_years(
    unit_code: str,
    db_path: Path | str = DEFAULT_DB,
) -> list[int]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT year FROM monthly_visits WHERE unit_code = ? ORDER BY year",
            (unit_code.upper(),),
        ).fetchall()
    return [r[0] for r in rows]


def bulk_upsert_visits(
    rows: list[tuple[str, int, int, int | None]],
    db_path: Path | str = DEFAULT_DB,
) -> int:
    """rows: list of (unit_code, year, month, visit_count)"""
    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO monthly_visits (unit_code, year, month, visit_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (unit_code, year, month) DO UPDATE SET
                visit_count = excluded.visit_count
            """,
            [(r[0].upper(), r[1], r[2], r[3]) for r in rows],
        )
    return len(rows)
