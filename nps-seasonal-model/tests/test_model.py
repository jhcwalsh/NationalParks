"""
Tests for the busyness model logic.

Run with:  pytest tests/test_model.py -v
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

import db
import clean
import model as mdl
from ingest import generate_seed_records, parse_wide_csv, parse_long_csv


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.db"
    db.init_db(db_path)
    return db_path


@pytest.fixture
def populated_db(tmp_db: Path) -> Path:
    """DB with seed data for YOSE, GRCA for 2015–2019 (5 non-COVID years)."""
    years = list(range(2015, 2020))
    park_records, visit_records = generate_seed_records(years)
    with db.get_conn(tmp_db) as conn:
        for p in park_records:
            if p["unit_code"] in ("YOSE", "GRCA", "ZION"):
                db.upsert_park(conn, p["unit_code"], p["name"], p["state"], p["type"])
        target = {"YOSE", "GRCA", "ZION"}
        for v in visit_records:
            if v["unit_code"] in target:
                db.upsert_monthly_visit(
                    conn, v["unit_code"], v["year"], v["month"], v["visit_count"]
                )
    return tmp_db


# ── db.py tests ───────────────────────────────────────────────────────────────

class TestDb:
    def test_init_creates_tables(self, tmp_db: Path):
        with db.get_conn(tmp_db) as conn:
            tables = {
                r[0] for r in
                conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        assert "parks" in tables
        assert "monthly_visits" in tables

    def test_upsert_park(self, tmp_db: Path):
        with db.get_conn(tmp_db) as conn:
            db.upsert_park(conn, "YOSE", "Yosemite National Park", "CA", "National Park")
        park = db.get_park("YOSE", tmp_db)
        assert park is not None
        assert park["name"] == "Yosemite National Park"
        assert park["state"] == "CA"

    def test_upsert_park_case_insensitive(self, tmp_db: Path):
        with db.get_conn(tmp_db) as conn:
            db.upsert_park(conn, "yose", "Yosemite National Park", "CA")
        assert db.get_park("YOSE", tmp_db) is not None
        assert db.get_park("yose", tmp_db) is not None

    def test_upsert_monthly_visit(self, tmp_db: Path):
        with db.get_conn(tmp_db) as conn:
            db.upsert_park(conn, "YOSE", "Yosemite NP", "CA")
            db.upsert_monthly_visit(conn, "YOSE", 2023, 7, 825000)
        visits = db.get_monthly_visits("YOSE", tmp_db)
        assert len(visits) == 1
        assert int(visits.iloc[0]["visit_count"]) == 825000

    def test_upsert_visit_overwrite(self, tmp_db: Path):
        with db.get_conn(tmp_db) as conn:
            db.upsert_park(conn, "YOSE", "Yosemite NP", "CA")
            db.upsert_monthly_visit(conn, "YOSE", 2023, 7, 100)
            db.upsert_monthly_visit(conn, "YOSE", 2023, 7, 999)
        visits = db.get_monthly_visits("YOSE", tmp_db)
        assert len(visits) == 1
        assert int(visits.iloc[0]["visit_count"]) == 999

    def test_get_available_years(self, populated_db: Path):
        years = db.get_available_years("YOSE", populated_db)
        assert 2015 in years
        assert 2019 in years

    def test_exclude_years(self, tmp_db: Path):
        with db.get_conn(tmp_db) as conn:
            db.upsert_park(conn, "YOSE", "Yosemite NP", "CA")
            for year in [2019, 2020, 2021, 2022]:
                db.upsert_monthly_visit(conn, "YOSE", year, 7, 100000)
        visits = db.get_monthly_visits("YOSE", tmp_db, exclude_years=[2020, 2021])
        assert set(visits["year"].astype(int).tolist()) == {2019, 2022}


# ── clean.py tests ────────────────────────────────────────────────────────────

class TestClean:
    def _make_df(self, rows: list[dict]) -> pd.DataFrame:
        return pd.DataFrame(rows)

    def test_standardise_columns(self):
        df = self._make_df([{"UnitCode": "YOSE", "Year": 2023, "Month": 7, "RecreationVisits": 100}])
        cleaned = clean.clean_visits(df)
        assert "unit_code" in cleaned.columns
        assert "year" in cleaned.columns
        assert "visit_count" in cleaned.columns

    def test_removes_negative_visits(self):
        df = self._make_df([
            {"unit_code": "YOSE", "year": 2023, "month": 7, "visit_count": -500},
            {"unit_code": "YOSE", "year": 2023, "month": 8, "visit_count": 100000},
        ])
        cleaned = clean.clean_visits(df)
        assert cleaned[cleaned["month"] == 7]["visit_count"].isna().all()
        assert int(cleaned[cleaned["month"] == 8]["visit_count"].iloc[0]) == 100000

    def test_covid_flag(self):
        df = self._make_df([
            {"unit_code": "YOSE", "year": 2019, "month": 7, "visit_count": 800000},
            {"unit_code": "YOSE", "year": 2020, "month": 7, "visit_count": 300000},
            {"unit_code": "YOSE", "year": 2021, "month": 7, "visit_count": 500000},
            {"unit_code": "YOSE", "year": 2022, "month": 7, "visit_count": 750000},
        ])
        cleaned = clean.clean_visits(df)
        covid_rows = cleaned[cleaned["covid_year"]]
        assert set(covid_rows["year"].astype(int).tolist()) == {2020, 2021}
        non_covid = cleaned[~cleaned["covid_year"]]
        assert set(non_covid["year"].astype(int).tolist()) == {2019, 2022}

    def test_deduplication(self):
        df = self._make_df([
            {"unit_code": "YOSE", "year": 2023, "month": 7, "visit_count": 100},
            {"unit_code": "YOSE", "year": 2023, "month": 7, "visit_count": 999},
        ])
        cleaned = clean.clean_visits(df)
        assert len(cleaned[(cleaned["year"] == 2023) & (cleaned["month"] == 7)]) == 1

    def test_exclude_covid(self):
        df = self._make_df([
            {"unit_code": "YOSE", "year": 2020, "month": 7, "visit_count": 100},
            {"unit_code": "YOSE", "year": 2022, "month": 7, "visit_count": 100},
        ])
        cleaned = clean.clean_visits(df)
        excluded = clean.exclude_covid(cleaned)
        assert 2020 not in excluded["year"].astype(int).tolist()
        assert 2022 in excluded["year"].astype(int).tolist()

    def test_flag_outliers(self):
        rows = [
            {"unit_code": "YOSE", "year": y, "month": 7, "visit_count": 800000, "covid_year": False}
            for y in range(2014, 2020)
        ]
        rows.append({"unit_code": "YOSE", "year": 2023, "month": 7, "visit_count": 50_000_000, "covid_year": False})
        df = pd.DataFrame(rows)
        flagged = clean.flag_outliers(df, k=3.0)
        assert flagged[flagged["year"] == 2023]["outlier"].all()
        assert not flagged[flagged["year"] == 2014]["outlier"].all()


# ── ingest.py CSV parsers ─────────────────────────────────────────────────────

class TestParsers:
    WIDE_CSV = (
        "Year,January,February,March,April,May,June,July,August,September,October,November,December,Total\n"
        "2019,41000,39000,75000,120000,250000,380000,820000,810000,400000,200000,65000,50000,3250000\n"
        "2020,15000,20000,28000,0,80000,150000,340000,320000,160000,80000,25000,20000,1238000\n"
    )

    LONG_CSV = (
        "UnitCode,ParkName,Year,Month,RecreationVisits\n"
        "YOSE,Yosemite National Park,2019,July,820000\n"
        "YOSE,Yosemite National Park,2019,August,810000\n"
        "GRCA,Grand Canyon National Park,2019,July,650000\n"
    )

    def test_parse_wide_basic(self):
        df = parse_wide_csv(self.WIDE_CSV, "YOSE")
        assert len(df) == 24  # 2 years × 12 months
        july_2019 = df[(df["year"] == 2019) & (df["month"] == 7)]
        assert int(july_2019["visit_count"].iloc[0]) == 820000

    def test_parse_wide_year_range(self):
        df = parse_wide_csv(self.WIDE_CSV, "YOSE")
        assert 2019 in df["year"].tolist()
        assert 2020 in df["year"].tolist()

    def test_parse_long_basic(self):
        df = parse_long_csv(self.LONG_CSV)
        assert len(df) == 3
        assert "YOSE" in df["unit_code"].tolist()
        assert "GRCA" in df["unit_code"].tolist()

    def test_parse_long_month_mapping(self):
        df = parse_long_csv(self.LONG_CSV)
        yose_july = df[(df["unit_code"] == "YOSE") & (df["month"] == 7)]
        assert len(yose_july) == 1
        assert int(yose_july["visit_count"].iloc[0]) == 820000

    def test_parse_wide_handles_commas_in_numbers(self):
        csv = (
            "Year,January,February,March,April,May,June,July,August,September,October,November,December\n"
            '2019,41000,39000,75000,120000,250000,380000,"820,000",810000,400000,200000,65000,50000\n'
        )
        df = parse_wide_csv(csv, "YOSE")
        july = df[df["month"] == 7]
        assert int(july["visit_count"].iloc[0]) == 820000

    def test_parse_wide_handles_missing_months(self):
        csv = "Year,January,July,December\n2019,41000,820000,50000\n"
        df = parse_wide_csv(csv, "YOSE")
        assert len(df) == 3
        assert all(v > 0 for v in df["visit_count"].dropna())


# ── Seed data ─────────────────────────────────────────────────────────────────

class TestSeedData:
    def test_generates_correct_park_count(self):
        parks, visits = generate_seed_records([2019])
        assert len(parks) == 20

    def test_generates_12_months_per_park_year(self):
        _, visits = generate_seed_records([2019])
        yose = [v for v in visits if v["unit_code"] == "YOSE"]
        assert len(yose) == 12
        months = {v["month"] for v in yose}
        assert months == set(range(1, 13))

    def test_covid_year_lower_visits(self):
        _, visits_2019 = generate_seed_records([2019])
        _, visits_2020 = generate_seed_records([2020])
        yose_2019 = sum(v["visit_count"] for v in visits_2019 if v["unit_code"] == "YOSE")
        yose_2020 = sum(v["visit_count"] for v in visits_2020 if v["unit_code"] == "YOSE")
        assert yose_2020 < yose_2019 * 0.6

    def test_all_visit_counts_non_negative(self):
        _, visits = generate_seed_records(list(range(2014, 2025)))
        assert all(v["visit_count"] >= 0 for v in visits)


# ── model.py tests ────────────────────────────────────────────────────────────

class TestModel:
    def test_build_model_returns_none_for_missing_park(self, populated_db: Path):
        result = mdl.build_busyness_model("ZZZZ", populated_db)
        assert result is None

    def test_build_model_yosemite(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        assert result is not None
        assert result.unit_code == "YOSE"
        assert len(result.monthly_scores) == 12

    def test_monthly_scores_0_to_100(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        for ms in result.monthly_scores:
            assert 0 <= ms.score <= 100

    def test_peak_month_has_score_100(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        max_score = max(ms.score for ms in result.monthly_scores)
        assert abs(max_score - 100.0) < 1.0

    def test_peak_months_count(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        assert len(result.peak_months) <= 3
        assert all(1 <= m <= 12 for m in result.peak_months)

    def test_quiet_months_below_threshold(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        score_map = {ms.month: ms.score for ms in result.monthly_scores}
        for m in result.quiet_months:
            assert score_map[m] < 30

    def test_low_confidence_flag(self, tmp_db: Path):
        # Only 3 years of data → low_confidence
        years = [2016, 2017, 2018]
        parks, visits = generate_seed_records(years)
        with db.get_conn(tmp_db) as conn:
            for p in parks:
                if p["unit_code"] == "YOSE":
                    db.upsert_park(conn, p["unit_code"], p["name"], p["state"], p["type"])
            for v in visits:
                if v["unit_code"] == "YOSE":
                    db.upsert_monthly_visit(conn, v["unit_code"], v["year"], v["month"], v["visit_count"])
        result = mdl.build_busyness_model("YOSE", tmp_db)
        assert result.low_confidence is True

    def test_no_low_confidence_with_5_years(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        assert result.low_confidence is False

    def test_yoy_trend_format(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        assert result.yoy_trend == "N/A" or result.yoy_trend[0] in ("+", "-")

    def test_best_windows_scores_below_threshold(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        for w in result.best_visit_windows:
            assert w.score <= 55

    def test_to_dict_schema(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        d = result.to_dict()
        assert "unit_code" in d
        assert "monthly_scores" in d
        assert "peak_months" in d
        assert "shoulder_months" in d
        assert "quiet_months" in d
        assert "best_visit_windows" in d
        assert "yoy_trend" in d
        assert "data_years" in d
        assert "excluded_years" in d
        assert "low_confidence" in d
        assert "weekend_multiplier" in d
        # Each monthly_score entry has required fields
        for ms in d["monthly_scores"]:
            assert {"month", "month_name", "score", "label", "avg_visits"} <= ms.keys()

    def test_excluded_years_in_model(self, populated_db: Path):
        result = mdl.build_busyness_model("YOSE", populated_db)
        assert 2020 in result.excluded_years
        assert 2021 in result.excluded_years

    def test_compare_parks(self, populated_db: Path):
        results = mdl.compare_parks(["YOSE", "GRCA"], month=7, db_path=populated_db)
        assert len(results) == 2
        codes = {r["unit_code"] for r in results}
        assert codes == {"YOSE", "GRCA"}

    def test_recommend_parks(self, populated_db: Path):
        recs = mdl.recommend_parks(db_path=populated_db, month=1, max_score=50)
        for r in recs:
            assert r["score"] <= 50

    def test_recommend_parks_state_filter(self, populated_db: Path):
        recs = mdl.recommend_parks(db_path=populated_db, state="CA", month=1, max_score=100)
        codes = {r["unit_code"] for r in recs}
        assert "YOSE" in codes
        assert "GRCA" not in codes  # AZ, not CA

    def test_get_month_busyness(self, populated_db: Path):
        result = mdl.get_month_busyness("YOSE", 7, populated_db)
        assert result is not None
        assert result["month"] == 7
        assert 0 <= result["score"] <= 100
