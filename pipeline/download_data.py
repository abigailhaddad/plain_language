#!/usr/bin/env python3
"""
Step 0: Download current job posting parquets from the public R2 bucket.

Only downloads files that don't already exist locally.

Usage:
    python download_data.py
"""

import os
import urllib.request

PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(PIPELINE_DIR), "data")
R2_BASE = "https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev"
CURRENT_FILES = [f"current_jobs_{y}.parquet" for y in range(2025, 2027)]


def download_file(url, dest):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "plain-language-titles/1.0"})
        response = urllib.request.urlopen(req)
        size = int(response.headers.get("Content-Length", 0))
        size_mb = size / (1024 * 1024) if size else 0

        with open(dest, "wb") as f:
            downloaded = 0
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if size:
                    pct = downloaded / size * 100
                    print(f"\r  {downloaded / 1024 / 1024:.1f} / {size_mb:.1f} MB ({pct:.0f}%)", end="", flush=True)
        print()
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"  not found (skipping)")
            return False
        raise


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    downloaded = 0
    skipped = 0

    for fname in CURRENT_FILES:
        dest = os.path.join(DATA_DIR, fname)
        if os.path.exists(dest):
            print(f"  {fname}: already exists, skipping")
            skipped += 1
            continue
        url = f"{R2_BASE}/data/{fname}"
        print(f"  {fname}:")
        if download_file(url, dest):
            downloaded += 1

    print(f"\nDone. Downloaded: {downloaded}, Already had: {skipped}")


if __name__ == "__main__":
    main()
