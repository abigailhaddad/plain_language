#!/usr/bin/env python3
"""
Step 7: Measure coverage of the title set.

Analyzes classification results to report coverage rates, fit quality,
and per-title statistics.

Usage:
    python measure_coverage.py --series 0343
"""

import argparse
import json
import os

import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def main():
    parser = argparse.ArgumentParser(description="Measure title coverage")
    parser.add_argument("--series", default="0343", help="Occupational series code")
    args = parser.parse_args()

    classified_path = os.path.join(DATA_DIR, f"{args.series}_classified.parquet")
    titles_path = os.path.join(DATA_DIR, f"{args.series}_titles_validated.json")
    if not os.path.exists(titles_path):
        titles_path = os.path.join(DATA_DIR, f"{args.series}_titles.json")

    if not os.path.exists(classified_path):
        print(f"Run classify_postings.py first to create {classified_path}")
        return

    df = pd.read_parquet(classified_path)
    with open(titles_path) as f:
        titles_data = json.load(f)

    title_names = {t["title"] for t in titles_data["titles"]}
    total = len(df)

    # Categorize outcomes
    errors = df["best_title"] == "error"
    from_list = df["best_title"].isin(title_names)
    proposed_new = ~errors & ~from_list

    valid = df[~errors]

    print("=" * 70)
    print(f"PLAIN LANGUAGE TITLE COVERAGE REPORT — Series {args.series}")
    print("=" * 70)

    print(f"\n{'OVERALL COVERAGE':=^70}")
    print(f"  Total postings:     {total}")
    print(f"  From title list:    {from_list.sum()} ({from_list.sum()/total*100:.1f}%)")
    print(f"  Proposed new title: {proposed_new.sum()} ({proposed_new.sum()/total*100:.1f}%)")
    print(f"  Errors:             {errors.sum()}")

    print(f"\n  Coverage by fit threshold:")
    for threshold in [1, 2, 3, 4, 5]:
        covered = (valid["fit_score"] >= threshold).sum()
        print(f"    Fit >= {threshold}: {covered:4d} / {total} ({covered/total*100:.1f}%)")

    avg_fit = valid["fit_score"].mean()
    median_fit = valid["fit_score"].median()
    print(f"\n  Mean fit score:   {avg_fit:.2f}")
    print(f"  Median fit score: {median_fit:.1f}")

    print(f"\n  Fit score distribution:")
    for score in range(1, 6):
        count = (valid["fit_score"] == score).sum()
        bar = "#" * (count // 5)
        print(f"    {score}: {count:4d} ({count/len(valid)*100:.1f}%)  {bar}")

    # Per-title breakdown (using resolved titles)
    print(f"\n{'PER-TITLE BREAKDOWN':=^70}")
    print(f"  {'Title':<35} {'Count':>5} {'%':>6} {'Avg Fit':>8} {'Source':>10}")
    print(f"  {'-'*35} {'-'*5} {'-'*6} {'-'*8} {'-'*10}")

    title_stats = []
    for title, group in valid.groupby("best_title"):
        count = len(group)
        pct = count / total * 100
        avg = group["fit_score"].mean()

        if title in title_names:
            source = "list"
        else:
            source = "new"

        title_stats.append({
            "title": title,
            "count": count,
            "pct": pct,
            "avg_fit": avg,
            "source": source,
            "fit_dist": {str(k): int(v) for k, v in group["fit_score"].value_counts().to_dict().items()},
        })
        print(f"  {title:<35} {count:5d} {pct:5.1f}% {avg:8.1f} {source:>10}")

    # Flag potential issues
    print(f"\n{'POTENTIAL ISSUES':=^70}")
    issues_found = False

    broad_titles = [t for t in title_stats if t["count"] > total * 0.25]
    if broad_titles:
        issues_found = True
        print(f"\n  Titles that may be too broad (>25% of postings):")
        for t in broad_titles:
            print(f"    {t['title']}: {t['count']} postings ({t['pct']:.1f}%)")

    narrow_titles = [t for t in title_stats if t["count"] < 10]
    if narrow_titles:
        issues_found = True
        print(f"\n  Titles that may be too narrow (<10 postings):")
        for t in narrow_titles:
            print(f"    {t['title']}: {t['count']} postings")

    low_fit = [t for t in title_stats if t["avg_fit"] < 3.5]
    if low_fit:
        issues_found = True
        print(f"\n  Titles with low average fit (<3.5):")
        for t in low_fit:
            print(f"    {t['title']}: avg fit {t['avg_fit']:.2f}")

    if not issues_found:
        print("  None detected!")

    # Low-fit postings analysis
    low_fit_postings = valid[valid["fit_score"] <= 2]
    if len(low_fit_postings) > 0:
        print(f"\n{'LOW-FIT POSTINGS (score <= 2)':=^70}")
        print(f"  {len(low_fit_postings)} postings with fit score <= 2:")
        for _, row in low_fit_postings.head(10).iterrows():
            print(f"\n    Federal title: {row['position_title']}")
            print(f"    Assigned: {row['best_title']} (fit: {row['fit_score']})")
            print(f"    Reason: {row['classification_reasoning']}")

    # Save report as JSON
    report = {
        "series": args.series,
        "total_postings": total,
        "from_list": int(from_list.sum()),
        "proposed_new": int(proposed_new.sum()),
        "errors": int(errors.sum()),
        "mean_fit_score": round(avg_fit, 2),
        "median_fit_score": round(float(median_fit), 1),
        "coverage_by_threshold": {
            str(t): int((valid["fit_score"] >= t).sum()) for t in range(1, 6)
        },
        "fit_distribution": {
            str(s): int((valid["fit_score"] == s).sum()) for s in range(1, 6)
        },
        "title_stats": title_stats,
    }

    report_path = os.path.join(DATA_DIR, f"{args.series}_coverage.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n{'':=^70}")
    print(f"Full report saved to {report_path}")


if __name__ == "__main__":
    main()
