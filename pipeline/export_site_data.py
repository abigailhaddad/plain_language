#!/usr/bin/env python3
"""
Step 7: Export site-ready JSON from classified parquet.

Reads the classified parquet and titles file, then writes the three JSON files
the site needs: {series}_site.json, {series}_coverage.json, and copies
{series}_titles_validated.json into the site/data/ directory.

Also updates site/data/series.json with the series entry if not already present.

Usage:
    python export_site_data.py --series 0343
    python export_site_data.py --series 0343 --duties-preview-len 300
"""

import argparse
import json
import os
import shutil

import pandas as pd

PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PIPELINE_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
SITE_DATA_DIR = os.path.join(REPO_ROOT, "site", "data")


def main():
    parser = argparse.ArgumentParser(description="Export site-ready JSON from classified data")
    parser.add_argument("--series", default="0343", help="Occupational series code")
    parser.add_argument("--duties-preview-len", type=int, default=300,
                        help="Max characters for duties preview (default: 300)")
    args = parser.parse_args()

    s = args.series
    classified_path = os.path.join(DATA_DIR, f"{s}_classified.parquet")
    coverage_path = os.path.join(DATA_DIR, f"{s}_coverage.json")
    titles_path = os.path.join(DATA_DIR, f"{s}_titles_validated.json")

    for path in [classified_path, coverage_path, titles_path]:
        if not os.path.exists(path):
            print(f"Missing: {path}. Run the pipeline first.")
            return

    os.makedirs(SITE_DATA_DIR, exist_ok=True)

    # --- Build site.json from classified parquet ---
    df = pd.read_parquet(classified_path)

    records = []
    for _, row in df.iterrows():
        title = row.get("best_title", "")
        if title in ("error", ""):
            continue

        duties = row.get("major_duties", "") or ""
        cn = int(row["control_number"])

        records.append({
            "control_number": cn,
            "url": f"https://www.usajobs.gov/job/{cn}",
            "position_title": row["position_title"],
            "department": row.get("department", ""),
            "agency": row.get("agency", ""),
            "grade": row.get("grade", ""),
            "plain_title": title,
            "fit_score": int(row.get("fit_score", 0)),
            "reasoning": row.get("classification_reasoning", ""),
            "duties": duties,
        })

    site_json_path = os.path.join(SITE_DATA_DIR, f"{s}_site.json")
    with open(site_json_path, "w") as f:
        json.dump(records, f)
    print(f"  Wrote {len(records)} postings to {site_json_path}")

    # --- Copy coverage and titles ---
    for filename in [f"{s}_coverage.json", f"{s}_titles_validated.json"]:
        src = os.path.join(DATA_DIR, filename)
        dst = os.path.join(SITE_DATA_DIR, filename)
        shutil.copy2(src, dst)
        print(f"  Copied {filename}")

    # --- Update series.json manifest ---
    manifest_path = os.path.join(SITE_DATA_DIR, "series.json")
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
    else:
        manifest = []

    existing_codes = {entry["code"] for entry in manifest}
    if s not in existing_codes:
        # Derive a name from the titles file or use the series code
        with open(titles_path) as f:
            titles_data = json.load(f)
        series_name = titles_data.get("series_name", f"Series {s}")
        fed_title = titles_data.get("fed_title", f"GS-{s}")

        manifest.append({"code": s, "name": series_name, "fedTitle": fed_title})
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=4)
        print(f"  Added {s} to series.json (you may want to edit the name/fedTitle)")
    else:
        print(f"  {s} already in series.json")

    print(f"\nDone. Site data for series {s} is ready.")


if __name__ == "__main__":
    main()
