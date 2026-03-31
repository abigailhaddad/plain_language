#!/usr/bin/env python3
"""
Orchestrator: Run the full plain language title pipeline.

Reads config.yaml for all settings (model, concurrency, series list, etc.)
and runs each series through the pipeline steps. Skips series that have
already been processed (checks for final output files) unless --force is set.

  1. fetch_series_jobs.py   — Extract postings from R2 parquets
  2. discover_titles.py     — LLM proposes titles from sampled postings
  3. consolidate_titles.py  — Programmatic dedup + per-title LLM validation
  4. validate_titles.py     — Check titles against OPM guidance
  5. enrich_new_titles.py   — LLM writes descriptions for new/replacement titles
  6. classify_postings.py   — LLM classifies postings against final titles
  7. measure_coverage.py    — Compute coverage stats
  8. export_site_data.py    — Build site-ready JSON

Usage:
    python run_pipeline.py                    # run new series only
    python run_pipeline.py --force            # rerun everything
    python run_pipeline.py --series 0343      # run one series (skip if done)
    python run_pipeline.py --series 0343 --force  # force rerun one series
"""

import argparse
import json
import subprocess
import sys
import os

import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
SITE_DATA_DIR = os.path.join(REPO_ROOT, "site", "data")
PYTHON = os.path.join(REPO_ROOT, "venv", "bin", "python3")

if not os.path.exists(PYTHON):
    PYTHON = sys.executable


def load_config(config_path):
    with open(config_path) as f:
        return yaml.safe_load(f)


def series_is_done(code: str) -> bool:
    """Check if a series has already been fully processed."""
    required_files = [
        os.path.join(DATA_DIR, f"{code}_classified.parquet"),
        os.path.join(DATA_DIR, f"{code}_coverage.json"),
        os.path.join(SITE_DATA_DIR, f"{code}_site.json"),
    ]
    return all(os.path.exists(f) for f in required_files)


def run_step(name: str, script: str, args: list[str]):
    print(f"\n{'=' * 60}")
    print(f"STEP: {name}")
    print(f"{'=' * 60}\n")

    cmd = [PYTHON, os.path.join(SCRIPT_DIR, script)] + args
    result = subprocess.run(cmd, cwd=SCRIPT_DIR)

    if result.returncode != 0:
        print(f"\n*** STEP FAILED: {name} (exit code {result.returncode}) ***")
        sys.exit(1)


def run_series(series_code: str, cfg: dict):
    model = cfg["model"]
    concurrency = str(cfg["concurrency"])
    discover_sample = str(cfg["discover_sample_size"])
    classify_sample = str(cfg["classify_sample_size"])
    min_text = str(cfg["min_text_length"])
    min_count = str(cfg["min_title_count"])
    duties_len = str(cfg["duties_preview_len"])
    s = series_code

    print(f"\n{'#' * 60}")
    print(f"  SERIES {s}  |  model={model}  concurrency={concurrency}")
    print(f"{'#' * 60}")

    run_step("1. Fetch postings",
             "fetch_series_jobs.py",
             ["--series", s, "--min-text-length", min_text])

    run_step("2. Discover titles (LLM)",
             "discover_titles.py",
             ["--series", s, "--sample-size", discover_sample,
              "--model", model, "--concurrency", concurrency])

    run_step("3. Consolidate & validate titles (LLM)",
             "consolidate_titles.py",
             ["--series", s, "--model", model, "--min-count", min_count])

    run_step("4. Validate against OPM guidance (LLM)",
             "validate_titles.py",
             ["--series", s, "--model", model])

    run_step("5. Enrich title descriptions (LLM)",
             "enrich_new_titles.py",
             ["--series", s, "--model", model])

    classify_args = ["--series", s, "--model", model, "--concurrency", concurrency]
    if int(classify_sample) > 0:
        classify_args += ["--sample-size", classify_sample]

    run_step("6. Classify postings (LLM)",
             "classify_postings.py",
             classify_args)

    run_step("7. Measure coverage",
             "measure_coverage.py",
             ["--series", s])

    run_step("8. Export site data",
             "export_site_data.py",
             ["--series", s, "--duties-preview-len", duties_len])

    print(f"\n  Series {s} complete.")


def main():
    parser = argparse.ArgumentParser(description="Run plain language title pipeline")
    parser.add_argument("--config", default=os.path.join(REPO_ROOT, "config.yaml"),
                        help="Path to config YAML (default: config.yaml)")
    parser.add_argument("--series", default=None,
                        help="Run a single series instead of all (e.g. 0343)")
    parser.add_argument("--force", action="store_true",
                        help="Force rerun even if series has already been processed")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if args.series:
        all_series = [args.series]
    else:
        all_series = [s["code"] for s in cfg["series"]]

    # Determine which series need processing
    to_run = []
    skipped = []
    for code in all_series:
        if not args.force and series_is_done(code):
            skipped.append(code)
        else:
            to_run.append(code)

    print(f"Plain Language Title Pipeline")
    print(f"  Config:  {args.config}")
    print(f"  Model:   {cfg['model']}")
    print(f"  Series:  {', '.join(all_series)}")
    if skipped:
        print(f"  Skipping (already done): {', '.join(skipped)}")
        print(f"  Use --force to rerun them.")
    if to_run:
        print(f"  Will process: {', '.join(to_run)}")
    else:
        print(f"\n  Nothing to do — all series already processed.")

    if to_run:
        run_step("0. Download source data",
                 "download_data.py", [])

    for code in to_run:
        run_series(code, cfg)

    # Always write series.json from config (covers all series, not just ones we ran)
    os.makedirs(SITE_DATA_DIR, exist_ok=True)
    series_manifest = [
        {"code": s["code"], "name": s["name"], "fedTitle": s["fedTitle"]}
        for s in cfg["series"]
    ]
    manifest_path = os.path.join(SITE_DATA_DIR, "series.json")
    with open(manifest_path, "w") as f:
        json.dump(series_manifest, f, indent=4)

    if to_run:
        print(f"\n{'=' * 60}")
        print(f"PIPELINE COMPLETE")
        print(f"{'=' * 60}")
        print(f"  Processed: {', '.join(to_run)}")
        if skipped:
            print(f"  Skipped:   {', '.join(skipped)}")
        print(f"  Site manifest: {manifest_path}")


if __name__ == "__main__":
    main()
