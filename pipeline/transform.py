from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import duckdb

OUTPUT_DIR = Path(__file__).parent.parent / "public" / "data"
STATUS_LABELS = {
    "canadian_born": "Canadian-born Citizens",
    "naturalized": "Naturalized Citizens",
    "permanent_resident": "Permanent Residents",
    "non_permanent_resident": "Temporary Residents",
}
COUNTRY_ALIASES = {
    "China, People's Republic of": "China",
    "Korea, Republic of": "South Korea",
    "United States of America": "United States",
    "United Kingdom and Overseas Territories": "United Kingdom",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "Viet Nam": "Vietnam",
    "Korea, South": "South Korea",
    "Cameroon, Federal Republic of": "Cameroon",
}
GEO_AGGREGATES = {
    "Total", "Other Countries", "Southeast Asia", "West Central Asia and the Middle East",
    "Caribbean and Bermuda", "Sub-Saharan Africa", "Northern and Western Africa",
    "Central America", "South America", "Eastern Europe", "Western Europe",
    "Northern Europe", "Southern Europe", "Central Asia", "Eastern Africa",
    "Western Africa", "North America", "Central Africa",
}


def _write(filename: str, data: object) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / filename).write_text(json.dumps(data, indent=2, default=str))
    print(f"  Wrote {filename}")


def _latest_source_records(conn: duckdb.DuckDBPyConnection, dataset_keys: list[str]) -> list[dict]:
    if not dataset_keys:
        return []
    placeholders = ", ".join(["?"] * len(dataset_keys))
    rows = conn.execute(f"""
        WITH ranked AS (
            SELECT dataset_key, source_family, source_id, resource_id, reference_period, cadence,
                   status, loaded_at, notes,
                   ROW_NUMBER() OVER (PARTITION BY dataset_key ORDER BY loaded_at DESC) AS rn
            FROM source_loads
            WHERE dataset_key IN ({placeholders})
        )
        SELECT dataset_key, source_family, source_id, resource_id, reference_period,
               cadence, status, loaded_at, notes
        FROM ranked WHERE rn = 1 ORDER BY dataset_key
    """, dataset_keys).fetchall()
    return [{
        "dataset_key": row[0],
        "source_family": row[1],
        "source_id": row[2],
        "resource_id": row[3],
        "reference_period": row[4],
        "cadence": row[5],
        "status": row[6],
        "loaded_at": str(row[7]) if row[7] else None,
        "notes": row[8],
    } for row in rows]


def _freshness(conn: duckdb.DuckDBPyConnection, *, dataset_keys: list[str], generated_at: datetime | None, source_failures: dict[str, str] | None) -> dict:
    source_failures = source_failures or {}
    records = _latest_source_records(conn, dataset_keys)
    failed_sources = sorted(key for key in dataset_keys if key in source_failures)
    latest_ref = next((record["reference_period"] for record in reversed(records) if record.get("reference_period")), None)
    return {
        "generated_at": generated_at.isoformat() if generated_at else None,
        "latest_source_reference": latest_ref,
        "stale": bool(failed_sources),
        "failed_sources": [{"dataset_key": key, "error": source_failures[key]} for key in failed_sources],
        "sources": records,
    }


def _latest_complete_year(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    row = conn.execute(f"""
        SELECT ref_year FROM {table}
        WHERE ref_year < YEAR(current_date)
        GROUP BY ref_year ORDER BY ref_year DESC LIMIT 1
    """).fetchone()
    return int(row[0]) if row else int(conn.execute(f"SELECT MAX(ref_year) FROM {table}").fetchone()[0])


def _canonical_country(name: str) -> str:
    clean = re.sub(r"^\(\d+\)\s*", "", str(name or "").strip())
    return COUNTRY_ALIASES.get(clean, clean)


def _include_country(country: str) -> bool:
    normalized = _canonical_country(country)
    lower = normalized.lower()
    return bool(
        normalized and normalized not in GEO_AGGREGATES and
        lower not in {"unknown", "other", "all countries", "_"} and
        not lower.startswith("other")
    )


def _scale_distribution(rows: list[tuple[str, int]], target_total: int) -> tuple[list[dict], int]:
    combined: dict[str, int] = {}
    for country, count in rows:
        if count and count > 0 and _include_country(country):
            canonical = _canonical_country(country)
            combined[canonical] = combined.get(canonical, 0) + int(count)
    if not combined or target_total <= 0:
        return [], max(target_total, 0)
    source_total = sum(combined.values())
    scaled = []
    floor_total = 0
    for country, count in combined.items():
        exact = target_total * count / source_total
        base = int(exact)
        scaled.append([country, base, exact - base])
        floor_total += base
    scaled.sort(key=lambda item: item[2], reverse=True)
    for i in range(target_total - floor_total):
        scaled[i % len(scaled)][1] += 1
    top10 = sorted(
        [{"country": country, "count": base} for country, base, _ in scaled if base > 0],
        key=lambda row: row["count"],
        reverse=True,
    )[:10]
    return top10, max(target_total - sum(row["count"] for row in top10), 0)


def _country_rows_for_year(conn: duckdb.DuckDBPyConnection, table: str, year: int) -> list[tuple[str, int]]:
    rows = conn.execute(f"""
        SELECT country, SUM(count) AS total
        FROM {table}
        WHERE ref_year = ?
        GROUP BY country ORDER BY total DESC
    """, [year]).fetchall()
    return [(str(row[0]), int(row[1])) for row in rows if row[1] is not None]


def _build_status_payload(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None, source_failures: dict[str, str] | None) -> dict:
    population_row = conn.execute("""
        SELECT population, ref_date FROM population_quarterly
        WHERE geo = 'Canada' ORDER BY ref_date DESC LIMIT 1
    """).fetchone()
    npr_row = conn.execute("""
        SELECT SUM(count), MAX(ref_date) FROM npr_by_type_quarterly
        WHERE geo = 'Canada'
          AND ref_date = (SELECT MAX(ref_date) FROM npr_by_type_quarterly WHERE geo = 'Canada')
    """).fetchone()
    census_rows = conn.execute("""
        SELECT census_year, status, count FROM status_breakdown_census
        WHERE geo = 'Canada' ORDER BY census_year, status
    """).fetchall()
    snapshots = [{"year": int(r[0]), "status": r[1], "count": int(r[2])} for r in census_rows if r[2] is not None]
    latest_census_year = max((row["year"] for row in snapshots), default=2021)
    census_map = {row["status"]: row["count"] for row in snapshots if row["year"] == latest_census_year}
    total_population = int(population_row[0]) if population_row else 0
    pop_ref_date = str(population_row[1]) if population_row else None
    npr_count = int(npr_row[0]) if npr_row and npr_row[0] else 0
    npr_ref_date = str(npr_row[1]) if npr_row and npr_row[1] else None
    resident_census_total = sum(census_map.get(key, 0) for key in ["canadian_born", "naturalized", "permanent_resident"])
    current_resident_total = max(total_population - npr_count, 0)
    exacts = {
        key: (current_resident_total * census_map.get(key, 0) / resident_census_total) if resident_census_total else 0
        for key in ["canadian_born", "naturalized", "permanent_resident"]
    }
    floors = {key: int(value) for key, value in exacts.items()}
    fractions = sorted(exacts, key=lambda key: exacts[key] - floors[key], reverse=True)
    for i in range(current_resident_total - sum(floors.values())):
        floors[fractions[i % len(fractions)]] += 1
    actual_rows = [
        {"status": key, "label": STATUS_LABELS[key], "count": int(census_map.get(key, 0)), "ref_date": f"{latest_census_year}-01-01", "source": "Statistics Canada 98-10-0302-01"}
        for key in ["canadian_born", "naturalized", "permanent_resident"]
    ] + [{
        "status": "non_permanent_resident",
        "label": STATUS_LABELS["non_permanent_resident"],
        "count": npr_count,
        "ref_date": npr_ref_date,
        "source": "Statistics Canada 17-10-0121-01",
    }]
    estimated_rows = [
        {"status": key, "label": STATUS_LABELS[key], "count": int(floors.get(key, 0)), "ref_date": pop_ref_date, "source": "Estimated from latest population + census resident shares"}
        for key in ["canadian_born", "naturalized", "permanent_resident"]
    ] + [{
        "status": "non_permanent_resident",
        "label": STATUS_LABELS["non_permanent_resident"],
        "count": npr_count,
        "ref_date": npr_ref_date,
        "source": "Statistics Canada 17-10-0121-01",
    }]
    return {
        "actual_latest": {"rows": actual_rows, "total": sum(row["count"] for row in actual_rows)},
        "estimated_latest": {
            "rows": estimated_rows,
            "total": sum(row["count"] for row in estimated_rows),
            "reconciles_to_population_total": total_population,
            "ref_date": pop_ref_date,
        },
        "census_snapshots": snapshots,
        "latest_census_year": latest_census_year,
        "source_dates": {
            "population_current": pop_ref_date,
            "npr_current": npr_ref_date,
            "census_status": str(latest_census_year),
        },
        "methodology": {
            "summary": "Estimated status values refresh automatically on every pipeline run. Temporary residents use the latest quarterly total; the remaining resident categories are scaled from the latest census resident shares so the estimate reconciles exactly to the latest Canada population total.",
            "actual_definition": "Actual uses the latest published figure for each category independently.",
            "estimated_definition": "Estimated uses a common-date snapshot anchored to the latest total population and temporary resident total.",
        },
        "freshness": _freshness(conn, dataset_keys=["population_quarterly", "npr_by_type_quarterly", "status_breakdown_census"], generated_at=generated_at, source_failures=source_failures),
    }


def export_population_current(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    row = conn.execute("""
        SELECT population, ref_date FROM population_quarterly
        WHERE geo = 'Canada' ORDER BY ref_date DESC LIMIT 1
    """).fetchone()
    if not row:
        print("  SKIP population_current.json — no data")
        return
    _write("population_current.json", {
        "population": int(row[0]),
        "ref_date": str(row[1]),
        "source": "Statistics Canada 17-10-0009-01",
        "frequency": "Quarterly",
        "freshness": _freshness(conn, dataset_keys=["population_quarterly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_population_history(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    rows = conn.execute("""
        SELECT ref_date, population FROM population_quarterly
        WHERE geo = 'Canada' AND ref_date >= '2000-01-01'
        ORDER BY ref_date
    """).fetchall()
    _write("population_history.json", {
        "data": [{"date": str(row[0]), "population": int(row[1])} for row in rows],
        "source": "Statistics Canada 17-10-0009-01",
        "frequency": "Quarterly",
        "freshness": _freshness(conn, dataset_keys=["population_quarterly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_status_breakdown(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    _write("status_breakdown.json", _build_status_payload(conn, generated_at=generated_at, source_failures=source_failures))


def export_visa_streams_current(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    rows = conn.execute("""
        SELECT npr_type, SUM(count) AS count
        FROM npr_by_type_quarterly
        WHERE geo = 'Canada'
          AND ref_date = (SELECT MAX(ref_date) FROM npr_by_type_quarterly WHERE geo = 'Canada')
        GROUP BY npr_type ORDER BY count DESC
    """).fetchall()
    ref_date = conn.execute("SELECT MAX(ref_date) FROM npr_by_type_quarterly WHERE geo = 'Canada'").fetchone()
    if not rows:
        print("  SKIP visa_streams_current.json — no data")
        return
    total = sum(int(row[1] or 0) for row in rows)
    _write("visa_streams_current.json", {
        "ref_date": str(ref_date[0]) if ref_date and ref_date[0] else None,
        "total_npr": total,
        "streams": [{
            "stream": row[0],
            "count": int(row[1]),
            "pct": round((int(row[1]) / total * 100), 1) if total else None,
        } for row in rows],
        "source": "Statistics Canada 17-10-0121-01",
        "frequency": "Quarterly",
        "freshness": _freshness(conn, dataset_keys=["npr_by_type_quarterly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_visa_streams_history(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    quarterly_rows = conn.execute("""
        SELECT ref_date, npr_type, SUM(count) AS count
        FROM npr_by_type_quarterly
        WHERE geo = 'Canada'
        GROUP BY ref_date, npr_type
        ORDER BY ref_date, npr_type
    """).fetchall()
    data = [{
        "date": str(row[0]),
        "stream": row[1],
        "count": int(row[2]),
        "cadence": "quarterly",
        "source_origin": "statcan",
    } for row in quarterly_rows]
    for (year,) in conn.execute("""
        SELECT DISTINCT ref_year FROM study_permits_monthly
        WHERE ref_year >= 2015 AND ref_year < 2021
        ORDER BY ref_year
    """).fetchall():
        study_total = int(conn.execute("SELECT COALESCE(SUM(count), 0) FROM study_permits_monthly WHERE ref_year = ?", [year]).fetchone()[0] or 0)
        work_total = int(conn.execute("SELECT COALESCE(SUM(count), 0) FROM work_permits_monthly WHERE ref_year = ?", [year]).fetchone()[0] or 0)
        asylum_total = int(conn.execute("SELECT COALESCE(SUM(count), 0) FROM asylum_claimants_monthly WHERE ref_year = ?", [year]).fetchone()[0] or 0)
        data.extend([
            {"date": f"{int(year)}-01-01", "stream": "annual_study_permit_holders", "count": study_total, "cadence": "annual", "source_origin": "ircc"},
            {"date": f"{int(year)}-01-01", "stream": "annual_work_permit_holders", "count": work_total, "cadence": "annual", "source_origin": "ircc"},
            {"date": f"{int(year)}-01-01", "stream": "annual_asylum_claimants", "count": asylum_total, "cadence": "annual", "source_origin": "ircc"},
        ])
    data.sort(key=lambda row: (row["date"], row["stream"]))
    _write("visa_streams_history.json", {
        "data": data,
        "series_meta": {
            "permit_work_only": {"label": "Work Permit Only", "group": "Quarterly"},
            "permit_study_only": {"label": "Study Permit Only", "group": "Quarterly"},
            "permit_work_and_study": {"label": "Work and Study Permit", "group": "Quarterly"},
            "permit_other": {"label": "Other Permit Holders", "group": "Quarterly"},
            "asylum_work_permit_only": {"label": "Asylum: Work Permit Only", "group": "Quarterly"},
            "asylum_study_permit_only": {"label": "Asylum: Study Permit Only", "group": "Quarterly"},
            "asylum_work_and_study": {"label": "Asylum: Work and Study Permit", "group": "Quarterly"},
            "asylum_no_permit": {"label": "Asylum: No Permit", "group": "Quarterly"},
            "annual_study_permit_holders": {"label": "Study Permit Holders (Annual Fallback)", "group": "Annual fallback"},
            "annual_work_permit_holders": {"label": "Work Permit Holders (Annual Fallback)", "group": "Annual fallback"},
            "annual_asylum_claimants": {"label": "Asylum Claimants (Annual Fallback)", "group": "Annual fallback"},
        },
        "note": "Quarterly StatsCan data is used where available. Earlier years fall back to annual IRCC series and are labeled as blended history.",
        "freshness": _freshness(conn, dataset_keys=["npr_by_type_quarterly", "study_permits_monthly", "work_permits_monthly", "asylum_claimants_monthly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_work_permit_sources_current(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    latest_year_row = conn.execute("SELECT MAX(ref_year) FROM work_permit_programs_monthly").fetchone()
    latest_year = int(latest_year_row[0]) if latest_year_row and latest_year_row[0] is not None else None
    streams: list[dict] = []
    total = 0
    if latest_year is not None:
        rows = conn.execute("""
            SELECT program_stream, SUM(count) AS total
            FROM work_permit_programs_monthly
            WHERE ref_year = ?
            GROUP BY program_stream ORDER BY total DESC
        """, [latest_year]).fetchall()
        total = sum(int(row[1] or 0) for row in rows)
        streams = [{
            "stream": row[0],
            "count": int(row[1]),
            "pct": round((int(row[1]) / total * 100), 1) if total else None,
        } for row in rows]
    _write("work_permit_sources_current.json", {
        "available": latest_year is not None and bool(streams),
        "ref_year": latest_year,
        "total": total,
        "streams": streams,
        "source": "IRCC work permit holders by program",
        "frequency": "Monthly aggregated to annual",
        "note": "This component refreshes automatically when an IRCC program-level work permit resource is available.",
        "freshness": _freshness(conn, dataset_keys=["work_permit_programs_monthly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_origin_pr_current(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    max_year = _latest_complete_year(conn, "permanent_residents_monthly")
    rows = conn.execute("""
        SELECT country, SUM(count) AS total
        FROM permanent_residents_monthly
        WHERE ref_year = ?
          AND country NOT IN ('Unknown', 'Other', 'All countries')
        GROUP BY country ORDER BY total DESC LIMIT 10
    """, [max_year]).fetchall()
    if not rows:
        print("  SKIP origin_pr_current.json — no data")
        return
    _write("origin_pr_current.json", {
        "ref_year": max_year,
        "top10": [{"country": _canonical_country(row[0]), "count": int(row[1])} for row in rows],
        "source": "IRCC Permanent Residents — open.canada.ca",
        "frequency": "Monthly",
        "freshness": _freshness(conn, dataset_keys=["permanent_residents_monthly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_origin_pr_history(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    rows = conn.execute("""
        WITH ranked AS (
            SELECT country, SUM(count) AS total
            FROM permanent_residents_monthly
            WHERE country NOT IN ('Unknown', 'Other', 'All countries')
            GROUP BY country ORDER BY total DESC LIMIT 10
        )
        SELECT p.ref_year, p.country, SUM(p.count) AS count
        FROM permanent_residents_monthly p
        INNER JOIN ranked r ON p.country = r.country
        GROUP BY p.ref_year, p.country
        ORDER BY p.ref_year, count DESC
    """).fetchall()
    _write("origin_pr_history.json", {
        "data": [{"year": int(row[0]), "country": _canonical_country(row[1]), "count": int(row[2])} for row in rows],
        "note": "Top 10 countries by all-time cumulative admissions",
        "source": "IRCC Permanent Residents — open.canada.ca",
        "frequency": "Annual aggregation of monthly data",
        "freshness": _freshness(conn, dataset_keys=["permanent_residents_monthly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_origin_temp_current(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    study_year = _latest_complete_year(conn, "study_permits_monthly")
    work_year = _latest_complete_year(conn, "work_permits_monthly")
    asylum_year = _latest_complete_year(conn, "asylum_claimants_monthly")
    study_rows = _country_rows_for_year(conn, "study_permits_monthly", study_year)[:10]
    work_rows = _country_rows_for_year(conn, "work_permits_monthly", work_year)[:10]
    asylum_rows = _country_rows_for_year(conn, "asylum_claimants_monthly", asylum_year)[:10]
    _write("origin_temp_current.json", {
        "study_permits": {
            "ref_year": study_year,
            "top10": [{"country": _canonical_country(country), "count": count} for country, count in study_rows],
        },
        "work_permits": {
            "ref_year": work_year,
            "top10": [{"country": _canonical_country(country), "count": count} for country, count in work_rows],
        },
        "asylum_claimants": {
            "ref_year": asylum_year,
            "top10": [{"country": _canonical_country(country), "count": count} for country, count in asylum_rows],
            "note": "IRCC publishes top-country asylum data and the site scales this into the temporary resident estimate when needed.",
        },
        "source": "IRCC open.canada.ca",
        "frequency": "Monthly",
        "freshness": _freshness(conn, dataset_keys=["study_permits_monthly", "work_permits_monthly", "asylum_claimants_monthly"], generated_at=generated_at, source_failures=source_failures),
    })


def export_origin_naturalized(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    rows = conn.execute("""
        SELECT census_year, country, count
        FROM naturalized_by_country_census
        ORDER BY census_year DESC, count DESC
    """).fetchall()
    if not rows:
        print("  SKIP origin_naturalized.json — no data")
        return
    latest_year = int(rows[0][0])
    top10 = [row for row in rows if int(row[0]) == latest_year][:10]
    _write("origin_naturalized.json", {
        "latest_census_year": latest_year,
        "top10": [{"country": _canonical_country(row[1]), "count": int(row[2])} for row in top10],
        "all_census_data": [{"year": int(row[0]), "country": _canonical_country(row[1]), "count": int(row[2])} for row in rows],
        "source": "Statistics Canada 98-10-0302-01",
        "frequency": "Census (every 5 years)",
        "freshness": _freshness(conn, dataset_keys=["naturalized_by_country_census"], generated_at=generated_at, source_failures=source_failures),
    })


def export_origin_overview(conn: duckdb.DuckDBPyConnection, *, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    status_payload = _build_status_payload(conn, generated_at=generated_at, source_failures=source_failures)
    estimated_map = {row["status"]: int(row["count"]) for row in status_payload["estimated_latest"]["rows"]}
    naturalized_year = conn.execute("SELECT MAX(census_year) FROM naturalized_by_country_census").fetchone()[0]
    naturalized_rows = conn.execute("""
        SELECT country, count FROM naturalized_by_country_census
        WHERE census_year = ? ORDER BY count DESC
    """, [naturalized_year]).fetchall()
    pr_year = _latest_complete_year(conn, "permanent_residents_monthly")
    study_year = _latest_complete_year(conn, "study_permits_monthly")
    work_year = _latest_complete_year(conn, "work_permits_monthly")
    asylum_year = _latest_complete_year(conn, "asylum_claimants_monthly")
    pr_rows = _country_rows_for_year(conn, "permanent_residents_monthly", pr_year)
    temp_mix: dict[str, int] = {}
    for table, year in [("study_permits_monthly", study_year), ("work_permits_monthly", work_year), ("asylum_claimants_monthly", asylum_year)]:
        for country, count in _country_rows_for_year(conn, table, year):
            canonical = _canonical_country(country)
            temp_mix[canonical] = temp_mix.get(canonical, 0) + count
    nat_top10, nat_other = _scale_distribution([(str(row[0]), int(row[1])) for row in naturalized_rows], estimated_map.get("naturalized", 0))
    pr_top10, pr_other = _scale_distribution(pr_rows, estimated_map.get("permanent_resident", 0))
    temp_top10, temp_other = _scale_distribution(list(temp_mix.items()), estimated_map.get("non_permanent_resident", 0))
    foreign_mix: dict[str, int] = {}
    for row in nat_top10 + pr_top10 + temp_top10:
        foreign_mix[row["country"]] = foreign_mix.get(row["country"], 0) + row["count"]
    foreign_mix["Non-top-10 Naturalized"] = nat_other
    foreign_mix["Non-top-10 Permanent Residents"] = foreign_mix.get("Non-top-10 Permanent Residents", 0) + pr_other
    foreign_mix["Non-top-10 Temporary Residents"] = foreign_mix.get("Non-top-10 Temporary Residents", 0) + temp_other
    foreign_total = estimated_map.get("naturalized", 0) + estimated_map.get("permanent_resident", 0) + estimated_map.get("non_permanent_resident", 0)
    foreign_top10 = sorted(
        [{"country": country, "count": count} for country, count in foreign_mix.items() if count > 0],
        key=lambda row: row["count"],
        reverse=True,
    )[:10]
    foreign_other = max(foreign_total - sum(row["count"] for row in foreign_top10), 0)
    _write("origin_overview.json", {
        "categories": [
            {
                "category": "foreign_born_total",
                "label": "Foreign-born Total",
                "total": foreign_total,
                "top10": foreign_top10,
                "non_top_10": foreign_other,
                "source_period": f"Naturalized {naturalized_year}, PR {pr_year}, Temporary {max(study_year, work_year, asylum_year)}",
                "estimation_basis": "Sum of estimated naturalized, permanent resident, and temporary resident category outputs.",
                "is_estimated": True,
            },
            {
                "category": "naturalized",
                "label": "Naturalized Citizens",
                "total": estimated_map.get("naturalized", 0),
                "top10": nat_top10,
                "non_top_10": nat_other,
                "source_period": f"{naturalized_year} census distribution scaled to current estimated stock",
                "estimation_basis": "Latest naturalized census distribution scaled to the current estimated naturalized stock.",
                "is_estimated": True,
            },
            {
                "category": "permanent_resident",
                "label": "Permanent Residents",
                "total": estimated_map.get("permanent_resident", 0),
                "top10": pr_top10,
                "non_top_10": pr_other,
                "source_period": f"{pr_year} IRCC annual distribution scaled to current estimated stock",
                "estimation_basis": "Latest complete IRCC permanent resident distribution scaled to the current estimated stock.",
                "is_estimated": True,
            },
            {
                "category": "temporary_resident",
                "label": "Temporary Residents",
                "total": estimated_map.get("non_permanent_resident", 0),
                "top10": temp_top10,
                "non_top_10": temp_other,
                "source_period": f"Study {study_year}, work {work_year}, asylum {asylum_year}",
                "estimation_basis": "Combined latest study, work, and asylum country distributions scaled to the current temporary resident total.",
                "is_estimated": True,
            },
        ],
        "methodology": {
            "summary": "Country-of-origin totals refresh automatically on every pipeline run. Each category reuses the newest available country distribution and scales it to the newest estimated stock total.",
            "foreign_born_definition": "Foreign-born total on this site is naturalized citizens plus permanent residents plus temporary residents.",
        },
        "freshness": _freshness(conn, dataset_keys=["naturalized_by_country_census", "permanent_residents_monthly", "study_permits_monthly", "work_permits_monthly", "asylum_claimants_monthly", "population_quarterly", "npr_by_type_quarterly", "status_breakdown_census"], generated_at=generated_at, source_failures=source_failures),
    })


def export_all(conn: duckdb.DuckDBPyConnection, *, run_id: str | None = None, generated_at: datetime | None = None, source_failures: dict[str, str] | None = None) -> None:
    export_population_current(conn, generated_at=generated_at, source_failures=source_failures)
    export_population_history(conn, generated_at=generated_at, source_failures=source_failures)
    export_status_breakdown(conn, generated_at=generated_at, source_failures=source_failures)
    export_visa_streams_current(conn, generated_at=generated_at, source_failures=source_failures)
    export_visa_streams_history(conn, generated_at=generated_at, source_failures=source_failures)
    export_work_permit_sources_current(conn, generated_at=generated_at, source_failures=source_failures)
    export_origin_pr_current(conn, generated_at=generated_at, source_failures=source_failures)
    export_origin_pr_history(conn, generated_at=generated_at, source_failures=source_failures)
    export_origin_temp_current(conn, generated_at=generated_at, source_failures=source_failures)
    export_origin_naturalized(conn, generated_at=generated_at, source_failures=source_failures)
    export_origin_overview(conn, generated_at=generated_at, source_failures=source_failures)
