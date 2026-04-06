"""
IRCC open data fetcher.
Data lives on open.canada.ca as CKAN packages containing CSV/XLSX resources.

Key format notes:
- CSV files are TAB-separated.
- '--' means suppressed value (count 1-5); stored as None.
- XLSX files have metadata title rows before the actual data header.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

CKAN_API = "https://open.canada.ca/data/api/action"
TIMEOUT = 60
CACHE_DIR = Path(__file__).parent.parent / "warehouse" / "raw_cache"

DATASET_IDS = {
    "study_permits": "90115b00-f9b8-49e8-afa3-b4cff8facaee",
    "work_permits": "360024f2-17e9-4558-bfc1-3616485d65b9",
    "asylum_claimants": "b6cbcf4d-f763-4924-a2fb-8cc4a06e3de4",
    "permanent_residents": "f7e5498e-0ad8-4417-85c9-9b8aff9b9eda",
}


def _get_package_resources(dataset_id: str) -> list[dict]:
    response = requests.get(f"{CKAN_API}/package_show", params={"id": dataset_id}, timeout=TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN error for {dataset_id}: {payload}")
    return payload["result"]["resources"]


def _resource_sort_key(resource: dict) -> tuple:
    return (
        resource.get("last_modified") or "",
        resource.get("metadata_modified") or "",
        resource.get("created") or "",
        resource.get("name") or "",
    )


def _pick_resource(
    resources: list[dict],
    keywords: list[str],
    *,
    fmt_pref: str = "csv",
    required_keywords: list[str] | None = None,
    min_score: int = 1,
) -> dict | None:
    required_keywords = required_keywords or []
    scored: list[tuple[int, tuple, dict]] = []

    for resource in resources:
        haystack = " ".join([
            resource.get("name") or "",
            resource.get("description") or "",
        ]).lower()
        if any(keyword.lower() not in haystack for keyword in required_keywords):
            continue

        score = sum(2 for keyword in keywords if keyword.lower() in haystack)
        if resource.get("format", "").lower() == fmt_pref:
            score += 1
        if score >= min_score:
            scored.append((score, _resource_sort_key(resource), resource))

    if not scored:
        return None

    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return scored[0][2]


def _download_file(resource: dict, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    resource_id = resource["id"]
    fmt = resource.get("format", "csv").lower()
    cache_file = cache_dir / f"{resource_id}.{fmt}"
    if not cache_file.exists():
        url = resource["url"]
        print(f"  Downloading IRCC resource {resource_id} ({fmt}) from {url}")
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()
        cache_file.write_bytes(response.content)
    return cache_file


def read_ircc_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep="\t",
        encoding="utf-8-sig",
        low_memory=False,
        na_values=["--", "- -", "N/A", "n/a"],
    )


def _find_xlsx_header_row(path: Path) -> int:
    raw = pd.read_excel(path, engine="openpyxl", header=None, nrows=10)
    for index, row in raw.iterrows():
        non_null = row.dropna()
        if len(non_null) >= 3 and all(isinstance(value, str) for value in non_null.values):
            return index
    return 0


def read_ircc_xlsx(path: Path) -> pd.DataFrame:
    header_row = _find_xlsx_header_row(path)
    return pd.read_excel(
        path,
        engine="openpyxl",
        header=header_row,
        na_values=["--", "- -", "N/A", "n/a"],
    )


def _attach_attrs(df: pd.DataFrame, *, dataset_id: str, resource: dict | None, cadence: str, reference_period: str) -> pd.DataFrame:
    df.attrs.update({
        "source_family": "ircc",
        "source_id": dataset_id,
        "resource_id": resource.get("id") if resource else None,
        "resource_name": resource.get("name") if resource else None,
        "cadence": cadence,
        "reference_period": reference_period,
    })
    return df


def _reference_period_from_years(df: pd.DataFrame, year_col: str = "EN_YEAR") -> str:
    if year_col not in df.columns:
        return "Latest available"
    years = pd.to_numeric(df[year_col], errors="coerce").dropna().astype(int)
    if years.empty:
        return "Latest available"
    return f"{years.min()} to {years.max()}"


def fetch_study_permits() -> dict[str, pd.DataFrame]:
    resources = _get_package_resources(DATASET_IDS["study_permits"])
    cache_dir = CACHE_DIR / "study_permits"

    by_country_resource = _pick_resource(
        resources,
        ["country", "citizenship", "holders"],
        fmt_pref="csv",
        required_keywords=["country"],
    )
    if not by_country_resource:
        raise RuntimeError("No study permit resource matched country/citizenship keywords")

    by_country = read_ircc_csv(_download_file(by_country_resource, cache_dir))
    _attach_attrs(
        by_country,
        dataset_id=DATASET_IDS["study_permits"],
        resource=by_country_resource,
        cadence="Monthly",
        reference_period=_reference_period_from_years(by_country),
    )
    return {"by_country": by_country}


def fetch_work_permits() -> dict[str, pd.DataFrame | None]:
    resources = _get_package_resources(DATASET_IDS["work_permits"])
    cache_dir = CACHE_DIR / "work_permits"

    by_country_resource = _pick_resource(
        resources,
        ["country", "citizenship", "holders"],
        fmt_pref="csv",
        required_keywords=["country"],
    )
    if not by_country_resource:
        raise RuntimeError("No work permit resource matched country/citizenship keywords")

    by_program_resource = _pick_resource(
        resources,
        ["program", "mobility", "temporary foreign worker", "international mobility", "holders"],
        fmt_pref="csv",
        required_keywords=["program"],
    )

    by_country = read_ircc_csv(_download_file(by_country_resource, cache_dir))
    _attach_attrs(
        by_country,
        dataset_id=DATASET_IDS["work_permits"],
        resource=by_country_resource,
        cadence="Monthly",
        reference_period=_reference_period_from_years(by_country),
    )

    by_program = None
    if by_program_resource:
        by_program = read_ircc_csv(_download_file(by_program_resource, cache_dir))
        _attach_attrs(
            by_program,
            dataset_id=DATASET_IDS["work_permits"],
            resource=by_program_resource,
            cadence="Monthly",
            reference_period=_reference_period_from_years(by_program),
        )

    return {"by_country": by_country, "by_program": by_program}


def fetch_asylum_claimants() -> dict[str, pd.DataFrame]:
    resources = _get_package_resources(DATASET_IDS["asylum_claimants"])
    cache_dir = CACHE_DIR / "asylum_claimants"

    by_province_resource = _pick_resource(
        resources,
        ["province", "territory", "office"],
        fmt_pref="xlsx",
        min_score=2,
    )
    top_country_resource = _pick_resource(
        resources,
        ["country", "citizenship", "top", "25"],
        fmt_pref="xlsx",
        required_keywords=["country"],
        min_score=3,
    )
    if not by_province_resource or not top_country_resource:
        raise RuntimeError("Missing asylum claimant XLSX resources")

    by_province = read_ircc_xlsx(_download_file(by_province_resource, cache_dir))
    top_countries = read_ircc_xlsx(_download_file(top_country_resource, cache_dir))

    _attach_attrs(
        by_province,
        dataset_id=DATASET_IDS["asylum_claimants"],
        resource=by_province_resource,
        cadence="Monthly",
        reference_period="Latest available",
    )
    _attach_attrs(
        top_countries,
        dataset_id=DATASET_IDS["asylum_claimants"],
        resource=top_country_resource,
        cadence="Monthly",
        reference_period="Latest available",
    )

    return {"by_province": by_province, "top_countries": top_countries}


def fetch_permanent_residents() -> dict[str, pd.DataFrame]:
    resources = _get_package_resources(DATASET_IDS["permanent_residents"])
    cache_dir = CACHE_DIR / "permanent_residents"

    by_country_resource = _pick_resource(
        resources,
        ["country", "citizenship", "residents"],
        fmt_pref="csv",
        required_keywords=["country"],
    )
    if not by_country_resource:
        raise RuntimeError("No permanent resident resource matched country/citizenship keywords")

    by_country = read_ircc_csv(_download_file(by_country_resource, cache_dir))
    _attach_attrs(
        by_country,
        dataset_id=DATASET_IDS["permanent_residents"],
        resource=by_country_resource,
        cadence="Monthly",
        reference_period=_reference_period_from_years(by_country),
    )
    return {"by_country": by_country}
