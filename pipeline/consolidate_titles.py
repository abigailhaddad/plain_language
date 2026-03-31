#!/usr/bin/env python3
"""
Step 3: Consolidate discovered titles by deduplicating near-identical strings,
then using an LLM to validate each title individually.

Instead of asking the LLM to merge hundreds of titles (which always over-merges),
we do programmatic dedup first, then LLM validates each surviving title.

Usage:
    python consolidate_titles.py --series 0343
"""

import argparse
import json
import os
import re
from collections import Counter

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel

load_dotenv()

from config import load_config

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def normalize(title: str) -> str:
    """Normalize a title for dedup comparison."""
    t = title.lower().strip()
    # Remove common prefixes/suffixes
    for prefix in ["senior ", "lead ", "supervisory ", "chief ", "director of ", "deputy "]:
        t = t.removeprefix(prefix)
    for suffix in [" manager", " director", " lead", " supervisor", " chief"]:
        t = t.removesuffix(suffix)
    # Normalize separators
    t = t.replace(" & ", " and ").replace("/", " ").replace("-", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def programmatic_dedup(title_counts: dict[str, int], min_count: int = 2) -> dict[str, list[str]]:
    """Group titles by normalized form. Returns {canonical: [variants]}."""
    groups: dict[str, list[tuple[str, int]]] = {}

    for title, count in title_counts.items():
        key = normalize(title)
        if key not in groups:
            groups[key] = []
        groups[key].append((title, count))

    # Pick the highest-count variant as canonical
    result = {}
    for key, variants in groups.items():
        variants.sort(key=lambda x: -x[1])
        canonical = variants[0][0]
        total_count = sum(c for _, c in variants)
        if total_count >= min_count:
            result[canonical] = [v[0] for v in variants]

    return result




class TitleValidation(BaseModel):
    is_real_job_title: bool
    is_specific_enough: bool
    verdict: str  # "pass" or "fail"
    reason: str


def main():
    parser = argparse.ArgumentParser(description="Consolidate and validate discovered titles")
    parser.add_argument("--series", default="0343", help="Occupational series code")
    parser.add_argument("--model", default="gpt-5.4-mini", help="OpenAI model")
    parser.add_argument("--min-count", type=int, default=2,
                        help="Min times a title must appear to keep it (default: 2)")
    args = parser.parse_args()

    discovered_path = os.path.join(DATA_DIR, f"{args.series}_discovered.json")
    if not os.path.exists(discovered_path):
        print(f"Run discover_titles.py first to create {discovered_path}")
        return

    with open(discovered_path) as f:
        discovered = json.load(f)

    title_counts = discovered["title_counts"]
    print(f"Starting with {len(title_counts)} unique discovered titles")

    # Step 1: Programmatic dedup
    deduped = programmatic_dedup(title_counts, min_count=args.min_count)
    print(f"After dedup (min count {args.min_count}): {len(deduped)} titles")

    # Step 2: LLM validates each title
    cfg = load_config()
    validate_prompt = cfg["prompts"]["consolidate_validate"]

    client = OpenAI()
    passed = []
    failed = []

    print(f"\nValidating each title with {args.model}...")
    import time
    for title, variants in sorted(deduped.items()):
        total_count = sum(title_counts.get(v, 0) for v in variants)
        for attempt in range(3):
            try:
                response = client.beta.chat.completions.parse(
                    model=args.model,
                    messages=[
                        {"role": "system", "content": validate_prompt},
                        {"role": "user", "content": f'Title: "{title}"'},
                    ],
                    response_format=TitleValidation,
                    temperature=0.1,
                )
                result = response.choices[0].message.parsed

                if result.verdict.lower() == "pass":
                    passed.append({
                        "title": title,
                        "description": "",  # Will be filled by enrich step
                        "absorbs": variants,
                        "count": total_count,
                    })
                    print(f"  PASS ({total_count:2d}x)  {title}")
                else:
                    failed.append(title)
                    print(f"  FAIL ({total_count:2d}x)  {title} — {result.reason}")
                break
            except Exception as e:
                if attempt < 2:
                    wait = (attempt + 1) * 10
                    print(f"  ERROR on '{title}': {e} — retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  ERROR on '{title}': {e} — skipping")
                    failed.append(title)

    # Save
    out_path = os.path.join(DATA_DIR, f"{args.series}_titles.json")
    output = {
        "series": args.series,
        "model": args.model,
        "num_discovered": len(title_counts),
        "num_after_dedup": len(deduped),
        "num_passed": len(passed),
        "num_failed": len(failed),
        "titles": sorted(passed, key=lambda x: -x["count"]),
    }
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{len(passed)} titles passed, {len(failed)} failed")
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
