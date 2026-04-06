"""
Statistics Canada Web Data Service fetcher.
Docs: https://www.statcan.gc.ca/en/developers/wds/user-guide
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import requests

BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
TIMEOUT = 60
CACHE_DIR = Path(__file__).parent.parent / "warehouse" / "raw_cache"


def _get_csv_download_url(product_id: int) -> str:
    url = f"{BASE}/getFullTableDownloadCSV/{product_id}/en"
    response = requests.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "SUCCESS":
        raise RuntimeError(f"WDS error for {product_id}: {payload}")
    return payload["object"]


def download_statcan_table(product_id: int, cache_dir: Path) -> pd.DataFrame:
    """Download the latest full StatsCan table as a DataFrame and cache the CSV locally."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{product_id}.csv"

    if not cache_file.exists():
        zip_url = _get_csv_download_url(product_id)
        print(f"  Downloading StatsCan {product_id} from {zip_url}")
        response = requests.get(zip_url, timeout=300, stream=True)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            csv_name = next(
                name for name in archive.namelist()
                if name.endswith(".csv") and "MetaData" not in name
            )
            with archive.open(csv_name) as handle:
                cache_file.write_bytes(handle.read())

    return pd.read_csv(cache_file, encoding="utf-8-sig", low_memory=False)


def _attach_attrs(
    df: pd.DataFrame,
    *,
    source_table: str,
    cadence: str,
    reference_period: str,
    resource_id: str | None = None,
) -> pd.DataFrame:
    df.attrs.update({
        "source_family": "statcan",
        "source_id": source_table,
        "resource_id": resource_id,
        "cadence": cadence,
        "reference_period": reference_period,
    })
    return df


def fetch_population_quarterly() -> pd.DataFrame:
    """
    17-10-0009-01 — Population estimates, quarterly.
    Returns columns: ref_date, geo, population
    """
    df = download_statcan_table(17100009, CACHE_DIR / "17100009")
    df = df[df["GEO"].notna() & df["VALUE"].notna()]
    df = df[df["GEO"] == "Canada"]
    result = pd.DataFrame({
        "ref_date": pd.to_datetime(df["REF_DATE"]).dt.to_period("Q").dt.start_time,
        "geo": df["GEO"].str.strip(),
        "population": df["VALUE"].astype("int64"),
    }).drop_duplicates(["ref_date", "geo"]).reset_index(drop=True)

    period = f"{result['ref_date'].min().date()} to {result['ref_date'].max().date()}"
    return _attach_attrs(
        result,
        source_table="17-10-0009-01",
        cadence="Quarterly",
        reference_period=period,
    )


def fetch_npr_by_type_quarterly() -> pd.DataFrame:
    """
    17-10-0121-01 — Non-permanent residents by type, quarterly.
    Returns columns: ref_date, geo, npr_type, count
    """
    df = download_statcan_table(17100121, CACHE_DIR / "17100121")

    type_map = {
        "Asylum claimants, protected persons and related groups with work permit only": "asylum_work_permit_only",
        "Asylum claimants, protected persons and related groups with study permit only": "asylum_study_permit_only",
        "Asylum claimants, protected persons and related groups with work and study permits": "asylum_work_and_study",
        "Asylum claimants, protected persons and related groups without work or study permits": "asylum_no_permit",
        "Work permit holders only": "permit_work_only",
        "Study permit holders only": "permit_study_only",
        "Work and study permit holders": "permit_work_and_study",
        "Other": "permit_other",
    }

    char_col = "Non-permanent resident types"
    df = df[df[char_col].isin(type_map.keys()) & df["VALUE"].notna() & df["GEO"].notna()]
    result = pd.DataFrame({
        "ref_date": pd.to_datetime(df["REF_DATE"]).dt.to_period("Q").dt.start_time,
        "geo": df["GEO"].str.strip(),
        "npr_type": df[char_col].map(type_map),
        "count": df["VALUE"].astype("int64"),
    }).drop_duplicates(["ref_date", "geo", "npr_type"]).reset_index(drop=True)

    period = f"{result['ref_date'].min().date()} to {result['ref_date'].max().date()}"
    return _attach_attrs(
        result,
        source_table="17-10-0121-01",
        cadence="Quarterly",
        reference_period=period,
    )


def fetch_status_breakdown_census() -> pd.DataFrame:
    """
    98-10-0302-01 — Immigrant status and period of immigration.
    Returns columns: census_year, geo, status, count
    """
    cache_file = CACHE_DIR / "98100302" / "98100302.csv"
    if not cache_file.exists():
        download_statcan_table(98100302, CACHE_DIR / "98100302")

    df = pd.read_csv(
        cache_file,
        encoding="utf-8-sig",
        low_memory=False,
        usecols=lambda column: column in {
            "REF_DATE", "GEO", "Citizenship (9)", "Age (8D)",
            "Gender (3)", "Statistics (3)", "Place of birth (290)",
        } or (column.startswith("Immigrant status") and ("[1]" in column or "[3]" in column or "[11]" in column)),
    )

    citizenship_targets = {
        "Canadian citizens by birth",
        "Canadian citizens by naturalization",
        "Not Canadian citizens",
    }

    total_col = next(column for column in df.columns if column.startswith("Immigrant status") and "[1]" in column)
    immigrant_col = next((column for column in df.columns if column.startswith("Immigrant status") and "[3]" in column), None)
    npr_col = next((column for column in df.columns if column.startswith("Immigrant status") and "[11]" in column), None)

    df = df[
        (df["GEO"] == "Canada")
        & (df["Statistics (3)"] == "Count")
        & (df["Age (8D)"] == "Total - Age")
        & (df["Gender (3)"] == "Total - Gender")
        & (df["Place of birth (290)"].str.startswith("Total", na=False))
        & (df["Citizenship (9)"].isin(citizenship_targets))
    ]

    rows_out: list[dict[str, int | str]] = []
    for _, row in df.iterrows():
        census_year = int(str(row["REF_DATE"])[:4])
        citizenship = row["Citizenship (9)"]

        if citizenship == "Canadian citizens by birth" and not pd.isna(row[total_col]):
            rows_out.append({
                "census_year": census_year,
                "geo": "Canada",
                "status": "canadian_born",
                "count": int(row[total_col]),
            })
        elif citizenship == "Canadian citizens by naturalization" and not pd.isna(row[total_col]):
            rows_out.append({
                "census_year": census_year,
                "geo": "Canada",
                "status": "naturalized",
                "count": int(row[total_col]),
            })
        elif citizenship == "Not Canadian citizens":
            if immigrant_col and not pd.isna(row.get(immigrant_col)):
                rows_out.append({
                    "census_year": census_year,
                    "geo": "Canada",
                    "status": "permanent_resident",
                    "count": int(row[immigrant_col]),
                })
            if npr_col and not pd.isna(row.get(npr_col)):
                rows_out.append({
                    "census_year": census_year,
                    "geo": "Canada",
                    "status": "non_permanent_resident",
                    "count": int(row[npr_col]),
                })

    result = pd.DataFrame(rows_out).sort_values(["census_year", "status"]).reset_index(drop=True)
    years = sorted(result["census_year"].unique().tolist()) if not result.empty else [2021]
    period = f"{years[0]} to {years[-1]}"
    return _attach_attrs(
        result,
        source_table="98-10-0302-01",
        cadence="Census (every 5 years)",
        reference_period=period,
    )


def fetch_naturalized_by_country_census() -> pd.DataFrame:
    """
    98-10-0302-01 — Place of birth for naturalized citizens.
    Returns columns: census_year, country, count
    """
    cache_file = CACHE_DIR / "98100302" / "98100302.csv"
    if not cache_file.exists():
        download_statcan_table(98100302, CACHE_DIR / "98100302")

    df = pd.read_csv(
        cache_file,
        encoding="utf-8-sig",
        low_memory=False,
        usecols=lambda column: column in {
            "REF_DATE", "GEO", "Citizenship (9)", "Age (8D)",
            "Gender (3)", "Statistics (3)", "Place of birth (290)",
        } or (column.startswith("Immigrant status") and "[1]" in column),
    )

    total_col = next(column for column in df.columns if column.startswith("Immigrant status") and "[1]" in column)

    df = df[
        (df["GEO"] == "Canada")
        & (df["Statistics (3)"] == "Count")
        & (df["Age (8D)"] == "Total - Age")
        & (df["Gender (3)"] == "Total - Gender")
        & (df["Citizenship (9)"] == "Canadian citizens by naturalization")
        & (~df["Place of birth (290)"].str.startswith("Total", na=True))
        & (df[total_col].notna())
    ]

    result = pd.DataFrame({
        "census_year": df["REF_DATE"].astype(str).str[:4].astype("int64"),
        "country": df["Place of birth (290)"].str.strip(),
        "count": df[total_col].astype("int64"),
    })

    exclude_substrings = [
        "outside canada", "africa", " asia", "asia ", "southeast asia",
        "south asia", "west central asia", "east asia", "south-east",
        "eastern asia", "southern asia", "western asia", "central asia",
        "americas", "europe", "oceania", "caribbean", "bermuda",
        "melanesia", "micronesia", "polynesia", "not stated", "total",
        "northern africa", "western africa", "eastern africa", "southern africa",
        "middle africa", "northern america", "central america", "south america",
    ]
    exclude_exact = {
        "inside canada",
        "north america",
        "other",
        "ontario",
        "quebec",
        "british columbia",
        "alberta",
        "saskatchewan",
        "manitoba",
        "nova scotia",
        "new brunswick",
        "newfoundland and labrador",
        "prince edward island",
    }

    def is_region(name: str) -> bool:
        normalized = name.lower()
        return any(substr in normalized for substr in exclude_substrings) or normalized in exclude_exact

    result = result[~result["country"].apply(is_region)]
    result = result[result["count"] > 0].sort_values(["census_year", "count"], ascending=[False, False]).reset_index(drop=True)
    years = sorted(result["census_year"].unique().tolist()) if not result.empty else [2021]
    period = f"{years[0]} to {years[-1]}"
    return _attach_attrs(
        result,
        source_table="98-10-0302-01",
        cadence="Census (every 5 years)",
        reference_period=period,
    )
