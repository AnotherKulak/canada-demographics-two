"""
Pipeline entrypoint.
Usage: python run_pipeline.py [--skip-fetch] [--skip-export]
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import traceback
from datetime import datetime

import pandas as pd

from db import get_conn
from sources.ircc import (
    fetch_asylum_claimants,
    fetch_permanent_residents,
    fetch_study_permits,
    fetch_work_permits,
)
from sources.statcan import (
    fetch_naturalized_by_country_census,
    fetch_npr_by_type_quarterly,
    fetch_population_quarterly,
    fetch_status_breakdown_census,
)
from transform import export_all

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")


def _record_source_load(
    conn,
    *,
    run_id: str,
    dataset_key: str,
    source_family: str,
    source_id: str,
    resource_id: str | None = None,
    reference_period: str | None = None,
    cadence: str | None = None,
    status: str,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO source_loads (
            run_id, dataset_key, source_family, source_id, resource_id,
            reference_period, cadence, status, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id, dataset_key, source_family, source_id, resource_id,
            reference_period, cadence, status, notes,
        ],
    )


def _source_meta(frame: pd.DataFrame, default_source_id: str) -> dict[str, str | None]:
    return {
        "source_family": frame.attrs.get("source_family", "unknown"),
        "source_id": frame.attrs.get("source_id", default_source_id),
        "resource_id": frame.attrs.get("resource_id"),
        "reference_period": frame.attrs.get("reference_period"),
        "cadence": frame.attrs.get("cadence"),
    }


def _month_to_int(value: object) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0
    if isinstance(value, (int, float)):
        return int(value)

    lookup = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    normalized = str(value).strip().lower().replace(".", "")
    return lookup.get(normalized, 0)


def _ircc_country_csv_to_annual(df: pd.DataFrame, stream: str = "_") -> pd.DataFrame:
    """
    Convert an IRCC TSV with EN_YEAR / EN_COUNTRY_OF_CITIZENSHIP / TOTAL
    into annual aggregates.
    """
    out = df[["EN_YEAR", "EN_COUNTRY_OF_CITIZENSHIP", "TOTAL"]].copy()
    out = out.rename(columns={
        "EN_YEAR": "ref_year",
        "EN_COUNTRY_OF_CITIZENSHIP": "country",
        "TOTAL": "count",
    })
    out["ref_year"] = pd.to_numeric(out["ref_year"], errors="coerce").astype("Int64")
    out["count"] = pd.to_numeric(out["count"], errors="coerce").astype("Int64")
    out["program_stream"] = stream
    out["province"] = "_"
    out["ref_month"] = 0
    out = out.dropna(subset=["ref_year", "country"])
    return out.groupby(["ref_year", "country", "program_stream", "province"], as_index=False).agg(
        count=("count", "sum"),
        ref_month=("ref_month", "first"),
    )


def _normalize_program_stream(value: object) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not normalized:
        return "other"
    if "temporary foreign worker" in normalized or "tfwp" in normalized:
        return "tfwp"
    if "international mobility" in normalized or normalized == "imp" or "imp " in normalized:
        return "imp"
    if "seasonal agricultural worker" in normalized:
        return "tfwp"
    if "post-graduation" in normalized or "open work permit" in normalized:
        return "imp"
    return "other"


def _ircc_program_csv_to_annual(df: pd.DataFrame) -> pd.DataFrame:
    column_candidates = [
        "EN_PROGRAM",
        "EN_PROGRAM_TYPE",
        "EN_PROGRAM_STREAM",
        "EN_PROGRAM_CATEGORY",
        "EN_CATEGORY",
        "EN_STREAM",
    ]
    program_column = next((column for column in column_candidates if column in df.columns), None)
    if not program_column:
        raise RuntimeError("Work permit program resource does not expose a recognizable program column")

    out = df[["EN_YEAR", program_column, "TOTAL"]].copy()
    out = out.rename(columns={
        "EN_YEAR": "ref_year",
        program_column: "program_stream",
        "TOTAL": "count",
    })
    out["ref_year"] = pd.to_numeric(out["ref_year"], errors="coerce").astype("Int64")
    out["count"] = pd.to_numeric(out["count"], errors="coerce").astype("Int64")
    out["program_stream"] = out["program_stream"].map(_normalize_program_stream)
    out["ref_month"] = 0
    out = out.dropna(subset=["ref_year", "program_stream"])
    return out.groupby(["ref_year", "ref_month", "program_stream"], as_index=False).agg(
        count=("count", "sum"),
    )


def _parse_asylum_country_xlsx(top_country_df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the asylum claimants XLSX country pivot table.
    """
    df = top_country_df.copy()
    quarter_row = df.iloc[0]
    country_col = df.columns[0]
    subtype_col = df.columns[1]

    year_total_cols: dict[int, object] = {}
    for column in df.columns:
        cell = str(quarter_row[column]).strip()
        if cell.endswith(" Total") and len(cell) == 10:
            try:
                year_total_cols[int(cell[:4])] = column
            except ValueError:
                pass

    data = df.iloc[2:].reset_index(drop=True)
    total_rows = data[data[subtype_col].isna()].copy()

    rows_out = []
    for _, row in total_rows.iterrows():
        country = str(row[country_col]).strip()
        if not country or country == "nan":
            continue
        for year, column in year_total_cols.items():
            value = row[column]
            try:
                count = int(float(str(value).replace(",", "")))
            except (ValueError, TypeError):
                count = None
            rows_out.append({
                "ref_year": year,
                "ref_month": 0,
                "country": country,
                "province": "_",
                "count": count,
            })

    return pd.DataFrame(rows_out)


def run(skip_fetch: bool = False, skip_export: bool = False) -> None:
    conn = get_conn()
    run_id = conn.execute(
        "INSERT INTO pipeline_runs (status) VALUES ('running') RETURNING run_id"
    ).fetchone()[0]
    fetched: list[str] = []
    failures: dict[str, str] = {}

    try:
        if not skip_fetch:
            print("\n-- StatsCan ---------------------------------------------")

            print("-> Population quarterly (17-10-0009-01)")
            try:
                pop = fetch_population_quarterly()
                conn.execute("DELETE FROM population_quarterly WHERE geo = 'Canada'")
                conn.register("pop_df", pop)
                conn.execute("""
                    INSERT INTO population_quarterly
                    SELECT ref_date, geo, population, '17-10-0009-01', now() FROM pop_df
                """)
                fetched.append("population_quarterly")
                _record_source_load(conn, run_id=run_id, dataset_key="population_quarterly", status="success", **_source_meta(pop, "17-10-0009-01"))
                print(f"  Loaded {len(pop)} rows")
            except Exception as exc:
                failures["population_quarterly"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="population_quarterly",
                    source_family="statcan",
                    source_id="17-10-0009-01",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: Population fetch failed: {exc}")

            print("-> NPR by type quarterly (17-10-0121-01)")
            try:
                npr = fetch_npr_by_type_quarterly()
                conn.execute("DELETE FROM npr_by_type_quarterly")
                conn.register("npr_df", npr)
                conn.execute("""
                    INSERT INTO npr_by_type_quarterly
                    SELECT ref_date, geo, npr_type, count, '17-10-0121-01', now() FROM npr_df
                """)
                fetched.append("npr_by_type_quarterly")
                _record_source_load(conn, run_id=run_id, dataset_key="npr_by_type_quarterly", status="success", **_source_meta(npr, "17-10-0121-01"))
                print(f"  Loaded {len(npr)} rows")
            except Exception as exc:
                failures["npr_by_type_quarterly"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="npr_by_type_quarterly",
                    source_family="statcan",
                    source_id="17-10-0121-01",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: NPR fetch failed: {exc}")

            print("-> Census status breakdown (98-10-0302-01)")
            try:
                status = fetch_status_breakdown_census()
                if not status.empty:
                    conn.execute("DELETE FROM status_breakdown_census WHERE geo = 'Canada'")
                    conn.register("status_df", status)
                    conn.execute("""
                        INSERT INTO status_breakdown_census
                        SELECT census_year, geo, status, count, '98-10-0302-01', now() FROM status_df
                    """)
                    fetched.append("status_breakdown_census")
                    _record_source_load(conn, run_id=run_id, dataset_key="status_breakdown_census", status="success", **_source_meta(status, "98-10-0302-01"))
                    print(f"  Loaded {len(status)} rows")
            except Exception as exc:
                failures["status_breakdown_census"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="status_breakdown_census",
                    source_family="statcan",
                    source_id="98-10-0302-01",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: Census status breakdown failed: {exc}")

            print("-> Naturalized by birth country (98-10-0302-01)")
            try:
                naturalized = fetch_naturalized_by_country_census()
                if not naturalized.empty:
                    conn.execute("DELETE FROM naturalized_by_country_census")
                    conn.register("nat_df", naturalized)
                    conn.execute("""
                        INSERT INTO naturalized_by_country_census
                        SELECT census_year, country, count, '98-10-0302-01', now() FROM nat_df
                    """)
                    fetched.append("naturalized_by_country_census")
                    _record_source_load(conn, run_id=run_id, dataset_key="naturalized_by_country_census", status="success", **_source_meta(naturalized, "98-10-0302-01"))
                    print(f"  Loaded {len(naturalized)} rows")
            except Exception as exc:
                failures["naturalized_by_country_census"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="naturalized_by_country_census",
                    source_family="statcan",
                    source_id="98-10-0302-01",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: Naturalized by country failed: {exc}")

            print("\n-- IRCC -------------------------------------------------")

            print("-> Study permits")
            try:
                study = fetch_study_permits()
                annual = _ircc_country_csv_to_annual(study["by_country"], stream="study_permit")
                conn.execute("DELETE FROM study_permits_monthly")
                conn.register("sp_df", annual)
                conn.execute("""
                    INSERT INTO study_permits_monthly (ref_year, ref_month, country, study_level, province, count, source_dataset)
                    SELECT ref_year, ref_month, country, '_', province, count, 'IRCC-90115b00' FROM sp_df
                """)
                fetched.append("study_permits")
                _record_source_load(conn, run_id=run_id, dataset_key="study_permits_monthly", status="success", **_source_meta(study["by_country"], "IRCC-90115b00"))
                print(f"  Loaded {len(annual)} rows")
            except Exception as exc:
                failures["study_permits_monthly"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="study_permits_monthly",
                    source_family="ircc",
                    source_id="IRCC-90115b00",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: Study permits load failed: {exc}")
                traceback.print_exc()

            print("-> Work permits")
            try:
                work = fetch_work_permits()
                annual = _ircc_country_csv_to_annual(work["by_country"], stream="work_permit")
                conn.execute("DELETE FROM work_permits_monthly")
                conn.register("wp_df", annual)
                conn.execute("""
                    INSERT INTO work_permits_monthly (ref_year, ref_month, program_stream, country, province, count, source_dataset)
                    SELECT ref_year, ref_month, program_stream, country, province, count, 'IRCC-360024f2' FROM wp_df
                """)
                fetched.append("work_permits")
                _record_source_load(conn, run_id=run_id, dataset_key="work_permits_monthly", status="success", **_source_meta(work["by_country"], "IRCC-360024f2"))
                print(f"  Loaded {len(annual)} rows")

                if work.get("by_program") is not None:
                    try:
                        program_annual = _ircc_program_csv_to_annual(work["by_program"])
                        conn.execute("DELETE FROM work_permit_programs_monthly")
                        conn.register("wpp_df", program_annual)
                        conn.execute("""
                            INSERT INTO work_permit_programs_monthly (ref_year, ref_month, program_stream, count, source_dataset)
                            SELECT ref_year, ref_month, program_stream, count, 'IRCC-360024f2' FROM wpp_df
                        """)
                        fetched.append("work_permit_programs")
                        _record_source_load(conn, run_id=run_id, dataset_key="work_permit_programs_monthly", status="success", **_source_meta(work["by_program"], "IRCC-360024f2"))
                        print(f"  Loaded {len(program_annual)} work permit program rows")
                    except Exception as exc:
                        failures["work_permit_programs_monthly"] = str(exc)
                        _record_source_load(
                            conn,
                            run_id=run_id,
                            dataset_key="work_permit_programs_monthly",
                            source_family="ircc",
                            source_id="IRCC-360024f2",
                            status="failed",
                            notes=str(exc),
                        )
                        print(f"  WARNING: Work permit program load failed: {exc}")
                else:
                    _record_source_load(
                        conn,
                        run_id=run_id,
                        dataset_key="work_permit_programs_monthly",
                        source_family="ircc",
                        source_id="IRCC-360024f2",
                        status="skipped",
                        notes="No program-level resource matched dataset keywords",
                    )
                    print("  Work permit program resource not found; keeping prior program-level data if available")
            except Exception as exc:
                failures["work_permits_monthly"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="work_permits_monthly",
                    source_family="ircc",
                    source_id="IRCC-360024f2",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: Work permits load failed: {exc}")
                traceback.print_exc()

            print("-> Permanent residents")
            try:
                pr = fetch_permanent_residents()
                annual = _ircc_country_csv_to_annual(pr["by_country"], stream="permanent_resident")
                conn.execute("DELETE FROM permanent_residents_monthly")
                conn.register("pr_df", annual)
                conn.execute("""
                    INSERT INTO permanent_residents_monthly (ref_year, ref_month, immigration_category, country, province, count, source_dataset)
                    SELECT ref_year, ref_month, program_stream, country, province, count, 'IRCC-f7e5498e' FROM pr_df
                """)
                fetched.append("permanent_residents")
                _record_source_load(conn, run_id=run_id, dataset_key="permanent_residents_monthly", status="success", **_source_meta(pr["by_country"], "IRCC-f7e5498e"))
                print(f"  Loaded {len(annual)} rows")
            except Exception as exc:
                failures["permanent_residents_monthly"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="permanent_residents_monthly",
                    source_family="ircc",
                    source_id="IRCC-f7e5498e",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: PR load failed: {exc}")
                traceback.print_exc()

            print("-> Asylum claimants")
            try:
                asylum = fetch_asylum_claimants()
                asylum_annual = _parse_asylum_country_xlsx(asylum["top_countries"])
                if not asylum_annual.empty:
                    conn.execute("DELETE FROM asylum_claimants_monthly")
                    conn.register("ac_df", asylum_annual)
                    conn.execute("""
                        INSERT INTO asylum_claimants_monthly (ref_year, ref_month, country, province, count, source_dataset)
                        SELECT ref_year, ref_month, country, province, count, 'IRCC-b6cbcf4d' FROM ac_df
                    """)
                    fetched.append("asylum_claimants")
                    _record_source_load(conn, run_id=run_id, dataset_key="asylum_claimants_monthly", status="success", **_source_meta(asylum["top_countries"], "IRCC-b6cbcf4d"))
                    print(f"  Loaded {len(asylum_annual)} rows")
            except Exception as exc:
                failures["asylum_claimants_monthly"] = str(exc)
                _record_source_load(
                    conn,
                    run_id=run_id,
                    dataset_key="asylum_claimants_monthly",
                    source_family="ircc",
                    source_id="IRCC-b6cbcf4d",
                    status="failed",
                    notes=str(exc),
                )
                print(f"  WARNING: Asylum claimants load failed: {exc}")
                traceback.print_exc()

        if not skip_export:
            print("\n-- Exporting JSON ---------------------------------------")
            export_all(
                conn,
                run_id=run_id,
                generated_at=datetime.now(),
                source_failures=failures,
            )

        conn.execute(
            "UPDATE pipeline_runs SET finished_at=?, status='success', sources_fetched=?, notes=? WHERE run_id=?",
            [datetime.now(), fetched, None if not failures else str(failures)[:2000], run_id],
        )
        print(f"\nPipeline complete. Run ID: {run_id}")

    except Exception:
        conn.execute(
            "UPDATE pipeline_runs SET finished_at=?, status='failed', notes=? WHERE run_id=?",
            [datetime.now(), traceback.format_exc()[:2000], run_id],
        )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--skip-export", action="store_true")
    args = parser.parse_args()
    run(skip_fetch=args.skip_fetch, skip_export=args.skip_export)
