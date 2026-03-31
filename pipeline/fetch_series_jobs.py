#!/usr/bin/env python3
"""
Step 1: Extract job postings for an occupational series from R2 parquet files.

Parses MatchedObjectDescriptor JSON to get MajorDuties, QualificationSummary,
JobSummary, and metadata. Combines text fields into a single combined_text column.
Filters out postings with insufficient text.

Usage:
    python fetch_series_jobs.py --series 0343
    python fetch_series_jobs.py --series 0343 --min-text-length 200
"""

import argparse
import json
import os
import re
from html import unescape
from typing import Optional

import duckdb
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# R2 parquet files to extract from (must be downloaded first)
PARQUET_FILES = [
    os.path.join(DATA_DIR, f"current_jobs_{y}.parquet")
    for y in range(2025, 2027)
]


def clean_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = " ".join(text.split())
    return text if text else None


def clean_duties(duties) -> Optional[str]:
    """MajorDuties comes as a list of strings."""
    if not duties:
        return None
    if isinstance(duties, list):
        duties = " ".join(str(d) for d in duties)
    return clean_text(duties)


def extract_from_mod(mod_json: str) -> dict:
    """Extract fields from a MatchedObjectDescriptor JSON string."""
    try:
        mod = json.loads(mod_json)
    except (json.JSONDecodeError, TypeError):
        return None

    user_area = mod.get("UserArea", {}).get("Details", {})

    grades = mod.get("JobGrade", [])
    pay_plan = grades[0].get("Code") if grades else None
    low_grade = user_area.get("LowGrade")
    high_grade = user_area.get("HighGrade")

    remuneration = mod.get("PositionRemuneration", [{}])
    min_salary = remuneration[0].get("MinimumRange") if remuneration else None
    max_salary = remuneration[0].get("MaximumRange") if remuneration else None

    position_uri = mod.get("PositionURI", "")
    control_number = None
    if position_uri and "/job/" in position_uri:
        try:
            control_number = int(position_uri.split("/job/")[-1])
        except (ValueError, IndexError):
            pass

    categories = mod.get("JobCategory", [])
    series_codes = [c.get("Code", "") for c in categories if isinstance(c, dict)]

    major_duties = clean_duties(user_area.get("MajorDuties"))
    qualification_summary = clean_text(mod.get("QualificationSummary"))
    job_summary = clean_text(user_area.get("JobSummary"))
    requirements = clean_text(user_area.get("Requirements"))
    education = clean_text(user_area.get("Education"))

    # Combine all text fields for LLM input
    text_parts = []
    if job_summary:
        text_parts.append(f"SUMMARY: {job_summary}")
    if major_duties:
        text_parts.append(f"DUTIES: {major_duties}")
    if qualification_summary:
        text_parts.append(f"QUALIFICATIONS: {qualification_summary}")
    combined_text = "\n\n".join(text_parts) if text_parts else None

    return {
        "control_number": control_number,
        "position_title": mod.get("PositionTitle"),
        "department": mod.get("DepartmentName"),
        "agency": mod.get("OrganizationName"),
        "sub_agency": mod.get("SubAgency"),
        "series_codes": json.dumps(series_codes),
        "pay_plan": pay_plan,
        "low_grade": low_grade,
        "high_grade": high_grade,
        "min_salary": float(min_salary) if min_salary else None,
        "max_salary": float(max_salary) if max_salary else None,
        "major_duties": major_duties,
        "qualification_summary": qualification_summary,
        "job_summary": job_summary,
        "requirements": requirements,
        "education": education,
        "combined_text": combined_text,
    }


def main():
    parser = argparse.ArgumentParser(description="Extract job postings for a series from R2 parquets")
    parser.add_argument("--series", default="0343", help="Occupational series code (default: 0343)")
    parser.add_argument("--min-text-length", type=int, default=100,
                        help="Min chars of combined text to keep a posting (default: 100)")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Find available parquet files
    available = [f for f in PARQUET_FILES if os.path.exists(f)]
    if not available:
        print("No parquet files found in data/. Download them first:")
        print("  python download_data.py --out-dir data/")
        return

    print(f"Reading from {len(available)} parquet file(s)...")

    # Use DuckDB to filter by series and extract MatchedObjectDescriptor
    con = duckdb.connect()
    all_mod_jsons = []
    for path in available:
        print(f"  {os.path.basename(path)}...", end=" ", flush=True)
        con.execute(f"""
            SELECT MatchedObjectDescriptor
            FROM read_parquet('{path}')
            WHERE JobCategories LIKE '%{args.series}%'
              AND MatchedObjectDescriptor IS NOT NULL
        """)
        rows = con.fetchall()
        print(f"{len(rows)} postings")
        all_mod_jsons.extend(r[0] for r in rows)
    con.close()

    print(f"\nTotal raw postings for series {args.series}: {len(all_mod_jsons)}")

    # Extract structured data from each MatchedObjectDescriptor
    records = []
    for mod_json in all_mod_jsons:
        rec = extract_from_mod(mod_json)
        if rec:
            records.append(rec)

    df = pd.DataFrame(records)

    # Deduplicate by control_number (same job may appear in multiple files)
    before_dedup = len(df)
    df = df.drop_duplicates(subset="control_number", keep="last").reset_index(drop=True)
    deduped = before_dedup - len(df)

    # Filter out postings with junk titles (numeric, empty, etc.)
    junk_titles = df["position_title"].fillna("").str.match(r'^\d+$')
    if junk_titles.sum() > 0:
        print(f"  ({junk_titles.sum()} postings dropped — numeric/junk title)")
        df = df[~junk_titles].reset_index(drop=True)

    # Filter out postings with insufficient text
    df["text_length"] = df["combined_text"].fillna("").str.len()
    insufficient = (df["text_length"] < args.min_text_length).sum()
    df = df[df["text_length"] >= args.min_text_length].reset_index(drop=True)
    df = df.drop(columns=["text_length"])

    out_path = os.path.join(DATA_DIR, f"{args.series}_raw.parquet")
    df.to_parquet(out_path, index=False)

    print(f"\nSaved {len(df)} postings to {out_path}")
    if deduped:
        print(f"  ({deduped} duplicates removed)")
    if insufficient:
        print(f"  ({insufficient} postings dropped — combined text < {args.min_text_length} chars)")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Top position titles:")
    for title, count in df["position_title"].value_counts().head(10).items():
        print(f"    {title}: {count}")


if __name__ == "__main__":
    main()
