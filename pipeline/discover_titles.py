#!/usr/bin/env python3
"""
Step 2: Discover plain language titles by having an LLM read job duties.

Samples postings from the raw parquet and asks the LLM to propose
plain language titles for each. Outputs a JSON file of all proposed titles.

Usage:
    python discover_titles.py --series 0343
    python discover_titles.py --series 0343 --sample-size 200 --model gpt-4o-mini
"""

import argparse
import asyncio
import json
import os
import time

import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI
from pydantic import BaseModel

load_dotenv()

from config import load_config

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


class ProposedTitles(BaseModel):
    titles: list[str]
    reasoning: str


def scrub_title(text: str, title: str) -> str:
    """Remove the position title from the text to avoid biasing the LLM."""
    import re
    # Case-insensitive removal of the title string
    return re.sub(re.escape(title), '', text, flags=re.IGNORECASE).strip()


async def propose_titles(client: AsyncOpenAI, position_title: str, text: str, model: str, semaphore: asyncio.Semaphore, system_prompt: str, max_text: int) -> dict:
    clean_text = scrub_title(text, position_title)
    async with semaphore:
        try:
            response = await client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Propose plain language titles for this federal job posting:\n\n{clean_text[:max_text]}"},
                ],
                response_format=ProposedTitles,
                temperature=0.3,
            )
            result = response.choices[0].message.parsed
            return {"titles": result.titles, "reasoning": result.reasoning}
        except Exception as e:
            return {"titles": [], "reasoning": f"ERROR: {e}"}


async def run_discovery(df: pd.DataFrame, model: str, concurrency: int, system_prompt: str, max_text: int) -> list[dict]:
    client = AsyncOpenAI()
    semaphore = asyncio.Semaphore(concurrency)

    tasks = []
    for idx, row in df.iterrows():
        tasks.append(propose_titles(client, row["position_title"], row["combined_text"], model, semaphore, system_prompt, max_text))

    print(f"  Sending {len(tasks)} LLM calls (concurrency={concurrency})...")
    start = time.time()
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start
    print(f"  Done in {elapsed:.1f}s ({len(tasks) / elapsed:.1f} calls/sec)")

    records = []
    for idx, (result, (_, row)) in enumerate(zip(results, df.iterrows())):
        records.append({
            "control_number": row["control_number"],
            "position_title": row["position_title"],
            "proposed_titles": result["titles"],
            "reasoning": result["reasoning"],
        })
    return records


def main():
    parser = argparse.ArgumentParser(description="Discover plain language titles via LLM")
    parser.add_argument("--series", default="0343", help="Occupational series code")
    parser.add_argument("--sample-size", type=int, default=200,
                        help="Number of postings to sample (default: 200, 0=all)")
    parser.add_argument("--model", default="gpt-5.4-mini", help="OpenAI model")
    parser.add_argument("--concurrency", type=int, default=20, help="Max concurrent API calls")
    args = parser.parse_args()

    raw_path = os.path.join(DATA_DIR, f"{args.series}_raw.parquet")
    if not os.path.exists(raw_path):
        print(f"Run fetch_series_jobs.py first to create {raw_path}")
        return

    df = pd.read_parquet(raw_path)
    print(f"Loaded {len(df)} postings for series {args.series}")

    if args.sample_size > 0 and args.sample_size < len(df):
        df = df.sample(n=args.sample_size, random_state=42).reset_index(drop=True)
        print(f"Sampled {len(df)} postings")

    cfg = load_config()
    system_prompt = cfg["prompts"]["discover"]
    max_text = cfg["max_text_length"]
    records = asyncio.run(run_discovery(df, args.model, args.concurrency, system_prompt, max_text))

    # Collect all proposed titles with frequency counts
    title_counts = {}
    errors = 0
    for rec in records:
        if rec["reasoning"].startswith("ERROR:"):
            errors += 1
            continue
        for title in rec["proposed_titles"]:
            title_counts[title] = title_counts.get(title, 0) + 1

    out_path = os.path.join(DATA_DIR, f"{args.series}_discovered.json")
    output = {
        "series": args.series,
        "sample_size": len(df),
        "model": args.model,
        "errors": errors,
        "total_unique_titles": len(title_counts),
        "title_counts": dict(sorted(title_counts.items(), key=lambda x: -x[1])),
        "per_posting": records,
    }
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved to {out_path}")
    print(f"  {len(title_counts)} unique titles proposed across {len(records)} postings")
    if errors:
        print(f"  {errors} errors")
    print(f"\n  Top 20 proposed titles:")
    for title, count in sorted(title_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"    {count:3d}x  {title}")


if __name__ == "__main__":
    main()
