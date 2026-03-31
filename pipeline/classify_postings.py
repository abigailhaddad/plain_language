#!/usr/bin/env python3
"""
Step 6: Classify all postings against validated titles using an LLM.

Two-pass approach:
  Pass 1 (constrained enum): Pick from the title list (which includes the original title) or none.
  Pass 2 (free text):        For any "none" results, propose a new title.

Usage:
    python classify_postings.py --series 0343
    python classify_postings.py --series 0343 --model gpt-5.4-mini --concurrency 30
"""

import argparse
import asyncio
import json
import os
import time
from enum import Enum

import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI
from pydantic import BaseModel, Field

load_dotenv()

from config import load_config

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def scrub_title(text: str, title: str) -> str:
    """Remove the position title from the text to avoid biasing the LLM."""
    import re
    return re.sub(re.escape(title), '', text, flags=re.IGNORECASE).strip()


# --- Pass 1: Constrained enum classification ---

def build_system_prompt(titles: list[dict], prompt_template: str) -> str:
    title_descriptions = "\n".join(
        f"- **{t['title']}**: {t['description']}"
        for t in titles
    )
    return prompt_template.replace("{title_descriptions}", title_descriptions)


def make_enum_model(title_names: list[str]):
    """Build enum from title list + none. The original posting title is added
    per-call by the orchestrator so it's just another option, not special."""
    TitleEnum = Enum(
        "TitleEnum",
        {t.replace(" ", "_").replace("&", "and").replace("/", "_").replace("-", "_"): t
         for t in title_names + ["none"]}
    )

    class EnumClassification(BaseModel):
        best_title: TitleEnum = Field(description="The best-fitting title from the list, or 'none'")
        fit_score: int = Field(description="How well the title fits (1-5)", ge=1, le=5)
        reasoning: str = Field(description="Brief explanation of why this title fits (1-2 sentences)")

    return EnumClassification


def make_enum_model_with_extra(title_names: list[str], extra: str):
    """Build enum including an extra option (the original title) if it's not already in the list."""
    all_names = list(title_names) + ["none"]
    if extra and extra not in all_names:
        all_names.append(extra)

    TitleEnum = Enum(
        "TitleEnum",
        {t.replace(" ", "_").replace("&", "and").replace("/", "_").replace("-", "_"): t
         for t in all_names}
    )

    class EnumClassification(BaseModel):
        best_title: TitleEnum = Field(description="The best-fitting title from the list, or 'none'")
        fit_score: int = Field(description="How well the title fits (1-5)", ge=1, le=5)
        reasoning: str = Field(description="Brief explanation of why this title fits (1-2 sentences)")

    return EnumClassification


async def classify_posting_enum(
    client: AsyncOpenAI, position_title: str, text: str, system_prompt: str,
    model: str, semaphore: asyncio.Semaphore, title_names: list[str], max_text: int,
) -> dict:
    clean_text = scrub_title(text, position_title)
    enum_model = make_enum_model_with_extra(title_names, position_title)
    async with semaphore:
        try:
            response = await client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Classify this job posting:\n\n{clean_text[:max_text]}"},
                ],
                response_format=enum_model,
                temperature=0.1,
            )
            result = response.choices[0].message.parsed
            return {
                "best_title": result.best_title.value,
                "fit_score": result.fit_score,
                "reasoning": result.reasoning,
            }
        except Exception as e:
            return {
                "best_title": "error",
                "fit_score": 0,
                "reasoning": f"ERROR: {e}",
            }


# --- Pass 2: Free-text proposal for "none" results ---



class ProposedTitle(BaseModel):
    best_title: str = Field(description="A specific, plain language job title for this posting")
    fit_score: int = Field(description="How well your proposed title captures the role (1-5)", ge=1, le=5)
    reasoning: str = Field(description="Brief explanation of why this title fits (1-2 sentences)")


async def propose_new_title(
    client: AsyncOpenAI, position_title: str, text: str,
    model: str, semaphore: asyncio.Semaphore, propose_prompt: str, max_text: int,
) -> dict:
    clean_text = scrub_title(text, position_title)
    async with semaphore:
        try:
            response = await client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": propose_prompt},
                    {"role": "user", "content": f"Propose a plain language title for this posting:\n\n{clean_text[:max_text]}"},
                ],
                response_format=ProposedTitle,
                temperature=0.3,
            )
            result = response.choices[0].message.parsed
            return {
                "best_title": result.best_title,
                "fit_score": result.fit_score,
                "reasoning": result.reasoning,
            }
        except Exception as e:
            return {
                "best_title": "error",
                "fit_score": 0,
                "reasoning": f"ERROR: {e}",
            }


# --- Orchestration ---

async def run_classification(df: pd.DataFrame, titles: list[dict], model: str, concurrency: int) -> list[dict]:
    cfg = load_config()
    classify_prompt_template = cfg["prompts"]["classify"]
    propose_prompt = cfg["prompts"]["classify_propose"]
    max_text = cfg["max_text_length"]

    client = AsyncOpenAI()
    semaphore = asyncio.Semaphore(concurrency)
    system_prompt = build_system_prompt(titles, classify_prompt_template)
    title_names = [t["title"] for t in titles]

    # Pass 1: enum-constrained classification
    # Each posting gets its own enum that includes the original title as an option
    print(f"  Pass 1: Classifying {len(df)} postings (constrained enum, concurrency={concurrency})...")
    start = time.time()

    tasks = []
    for _, row in df.iterrows():
        tasks.append(classify_posting_enum(
            client, row["position_title"], row["combined_text"],
            system_prompt, model, semaphore, title_names, max_text,
        ))

    batch_size = 100
    all_results = []
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i + batch_size]
        results = await asyncio.gather(*batch)
        all_results.extend(results)
        elapsed = time.time() - start
        print(f"    {len(all_results)}/{len(tasks)} done ({elapsed:.0f}s)")

    elapsed = time.time() - start
    print(f"  Pass 1 done in {elapsed:.1f}s ({len(tasks) / elapsed:.1f} calls/sec)")

    # Pass 2: free-text proposals for "none" results
    none_indices = [i for i, r in enumerate(all_results) if r["best_title"] == "none"]

    if none_indices:
        print(f"\n  Pass 2: Proposing new titles for {len(none_indices)} unmatched postings...")
        start2 = time.time()

        pass2_tasks = []
        for idx in none_indices:
            row = df.iloc[idx]
            pass2_tasks.append(propose_new_title(
                client, row["position_title"], row["combined_text"],
                model, semaphore, propose_prompt, max_text,
            ))

        pass2_results = await asyncio.gather(*pass2_tasks)

        for idx, result in zip(none_indices, pass2_results):
            all_results[idx] = result

        elapsed2 = time.time() - start2
        print(f"  Pass 2 done in {elapsed2:.1f}s")
    else:
        print(f"\n  Pass 2: No unmatched postings — skipping.")

    return all_results


def main():
    parser = argparse.ArgumentParser(description="Classify postings against canonical titles")
    parser.add_argument("--series", default="0343", help="Occupational series code")
    parser.add_argument("--model", default="gpt-5.4-mini", help="OpenAI model")
    parser.add_argument("--concurrency", type=int, default=20, help="Max concurrent API calls")
    parser.add_argument("--sample-size", type=int, default=0,
                        help="Classify a random sample instead of all postings (0=all)")
    args = parser.parse_args()

    raw_path = os.path.join(DATA_DIR, f"{args.series}_raw.parquet")
    validated_path = os.path.join(DATA_DIR, f"{args.series}_titles_validated.json")
    titles_path = validated_path if os.path.exists(validated_path) else os.path.join(DATA_DIR, f"{args.series}_titles.json")

    if not os.path.exists(raw_path) or not os.path.exists(titles_path):
        print(f"Need both {raw_path} and {titles_path}. Run previous steps first.")
        return

    df = pd.read_parquet(raw_path)
    with open(titles_path) as f:
        titles_data = json.load(f)
    titles = titles_data["titles"]

    if args.sample_size > 0 and args.sample_size < len(df):
        df = df.sample(n=args.sample_size, random_state=42).reset_index(drop=True)
        print(f"Sampled {len(df)} postings")

    print(f"Classifying {len(df)} postings against {len(titles)} canonical titles...")

    results = asyncio.run(run_classification(df, titles, args.model, args.concurrency))

    # Merge results back into dataframe
    df["best_title"] = [r["best_title"] for r in results]
    df["fit_score"] = [r["fit_score"] for r in results]
    df["classification_reasoning"] = [r["reasoning"] for r in results]

    out_path = os.path.join(DATA_DIR, f"{args.series}_classified.parquet")
    df.to_parquet(out_path, index=False)

    # Quick summary
    title_names = {t["title"] for t in titles}
    errors = (df["best_title"] == "error").sum()
    from_list = df["best_title"].isin(title_names).sum()
    proposed_new = len(df) - errors - from_list

    print(f"\nSaved to {out_path}")
    print(f"  From title list: {from_list}, Proposed new: {proposed_new}, Errors: {errors}")
    print(f"  Average fit score: {df[df['fit_score'] > 0]['fit_score'].mean():.2f}")
    if proposed_new > 0:
        new_titles = df[~df["best_title"].isin(title_names | {"error"})]["best_title"].value_counts()
        print(f"\n  Newly proposed titles:")
        for title, count in new_titles.items():
            print(f"    {count:4d}  {title}")
    print(f"\n  Title distribution:")
    for title, group in df[df["best_title"] != "error"].groupby("best_title"):
        avg_fit = group["fit_score"].mean()
        marker = " (new)" if title not in title_names else ""
        print(f"    {len(group):4d}  (avg fit {avg_fit:.1f})  {title}{marker}")


if __name__ == "__main__":
    main()
