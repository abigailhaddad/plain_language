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
    client: AsyncOpenAI, position_title: str, text: str, duties: str, system_prompt: str,
    model: str, semaphore: asyncio.Semaphore, title_names: list[str], max_text: int,
    approved_extra: str = None,
) -> dict:
    if len(duties) < 100:
        return {"best_title": "missing_duties", "fit_score": 0, "reasoning": "Posting does not contain substantive duties text."}
    clean_text = scrub_title(text, position_title)
    enum_model = make_enum_model_with_extra(title_names, approved_extra) if approved_extra else make_enum_model(title_names)
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


# --- Pre-validate original titles ---

class TitleValidation(BaseModel):
    is_real_job_title: bool
    is_specific_enough: bool
    verdict: str  # "pass" or "fail"
    reason: str


async def _validate_one_title(client: AsyncOpenAI, title: str, model: str,
                              validate_prompt: str, semaphore: asyncio.Semaphore) -> tuple[str, bool, str]:
    async with semaphore:
        try:
            response = await client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": validate_prompt},
                    {"role": "user", "content": f'Title: "{title}"'},
                ],
                response_format=TitleValidation,
                temperature=0.1,
            )
            result = response.choices[0].message.parsed
            passed = result.verdict.lower() == "pass"
            return (title, passed, result.reason)
        except Exception as e:
            return (title, False, f"ERROR: {e}")


async def validate_original_titles(client: AsyncOpenAI, titles_to_check: list[str],
                                   model: str, validate_prompt: str, concurrency: int) -> set[str]:
    """Check which original posting titles are valid plain-language titles.
    Returns the set of titles that passed."""
    semaphore = asyncio.Semaphore(concurrency)
    print(f"  Validating {len(titles_to_check)} unique original titles (concurrency={concurrency})...")

    tasks = [_validate_one_title(client, t, model, validate_prompt, semaphore) for t in titles_to_check]
    results = await asyncio.gather(*tasks)

    approved = set()
    for title, passed, reason in results:
        status = "PASS" if passed else "FAIL"
        print(f"    {status}: {title}" + (f" — {reason}" if not passed else ""))
        if passed:
            approved.add(title)
    return approved


# --- Orchestration ---

async def run_classification(df: pd.DataFrame, titles: list[dict], model: str, concurrency: int) -> list[dict]:
    cfg = load_config()
    classify_prompt_template = cfg["prompts"]["classify"]
    propose_prompt = cfg["prompts"]["classify_propose"]
    validate_prompt = cfg["prompts"]["consolidate_validate"]
    max_text = cfg["max_text_length"]

    client = AsyncOpenAI()
    semaphore = asyncio.Semaphore(concurrency)
    system_prompt = build_system_prompt(titles, classify_prompt_template)
    title_names = [t["title"] for t in titles]

    # Pass 0: validate original titles — only approved ones get added to enum
    unique_originals = set(df["position_title"].unique()) - set(title_names)
    if unique_originals:
        approved_originals = await validate_original_titles(
            client, sorted(unique_originals), model, validate_prompt, concurrency)
    else:
        approved_originals = set()
    print(f"  {len(approved_originals)}/{len(unique_originals)} original titles approved as plain language")

    # Pass 1: enum-constrained classification
    print(f"\n  Pass 1: Classifying {len(df)} postings (constrained enum, concurrency={concurrency})...")
    start = time.time()

    tasks = []
    for _, row in df.iterrows():
        orig = row["position_title"]
        duties = row.get("major_duties", "") or ""
        extra = orig if orig in approved_originals else None
        tasks.append(classify_posting_enum(
            client, orig, row["combined_text"], duties,
            system_prompt, model, semaphore, title_names, max_text, extra,
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


def run_classification_batch(df: pd.DataFrame, titles: list[dict], model: str, concurrency: int, series_code: str = "unknown") -> list[dict]:
    """Batch API version — 50% cheaper, slower turnaround."""
    from batch import build_batch_request, pydantic_to_response_format, run_batch

    cfg = load_config()
    classify_prompt_template = cfg["prompts"]["classify"]
    propose_prompt = cfg["prompts"]["classify_propose"]
    validate_prompt = cfg["prompts"]["consolidate_validate"]
    max_text = cfg["max_text_length"]

    system_prompt = build_system_prompt(titles, classify_prompt_template)
    title_names = [t["title"] for t in titles]

    # Pass 0: validate original titles (still real-time, small number of calls)
    client = AsyncOpenAI()
    unique_originals = set(df["position_title"].unique()) - set(title_names)
    if unique_originals:
        approved_originals = asyncio.run(validate_original_titles(
            client, sorted(unique_originals), model, validate_prompt, concurrency))
    else:
        approved_originals = set()
    print(f"  {len(approved_originals)}/{len(unique_originals)} original titles approved as plain language")

    # For batch, we use ProposedTitle (free-text best_title) since we can't do
    # per-row enums in a single batch. The system prompt lists all valid titles
    # and we add approved originals to the prompt text.
    all_valid = list(title_names)
    for orig in approved_originals:
        if orig not in all_valid:
            all_valid.append(orig)

    batch_system_prompt = system_prompt
    if approved_originals - set(title_names):
        extra_titles = approved_originals - set(title_names)
        batch_system_prompt += "\n\nAdditional valid titles (from original postings):\n"
        batch_system_prompt += "\n".join(f"- {t}" for t in sorted(extra_titles))

    batch_system_prompt += ("\n\nYou MUST pick one of the titles listed above. "
                           "Use 'none' only if nothing fits at score 3 or above.")

    response_format = pydantic_to_response_format(ProposedTitle)

    # Build batch requests
    requests = []
    skip_indices = set()
    for i, (_, row) in enumerate(df.iterrows()):
        duties = row.get("major_duties", "") or ""
        if len(duties) < 100:
            skip_indices.add(i)
            continue
        clean_text = scrub_title(row["combined_text"], row["position_title"])
        requests.append(build_batch_request(
            custom_id=str(i),
            model=model,
            messages=[
                {"role": "system", "content": batch_system_prompt},
                {"role": "user", "content": f"Classify this job posting:\n\n{clean_text[:max_text]}"},
            ],
            response_format_schema=response_format,
            temperature=0.1,
        ))

    if skip_indices:
        print(f"  Skipping {len(skip_indices)} postings with insufficient duties")

    print(f"\n  Pass 1 (batch): Submitting {len(requests)} requests...")
    batch_results = run_batch(requests, tag=f"classify_{series_code}", description=f"classify {series_code}")

    # Build results array
    skip_result = {"best_title": "missing_duties", "fit_score": 0, "reasoning": "Posting does not contain substantive duties text."}
    error_result = {"best_title": "error", "fit_score": 0, "reasoning": "Batch request failed."}

    all_results = []
    for i in range(len(df)):
        if i in skip_indices:
            all_results.append(skip_result)
        elif str(i) in batch_results:
            r = batch_results[str(i)]
            if "error" in r:
                all_results.append(error_result)
            else:
                all_results.append({
                    "best_title": r.get("best_title", "error"),
                    "fit_score": r.get("fit_score", 0),
                    "reasoning": r.get("reasoning", ""),
                })
        else:
            all_results.append(error_result)

    # Pass 2: propose new titles for "none" results (also via batch)
    none_indices = [i for i, r in enumerate(all_results) if r["best_title"] == "none"]
    if none_indices:
        print(f"\n  Pass 2 (batch): Proposing new titles for {len(none_indices)} unmatched postings...")
        pass2_requests = []
        for idx in none_indices:
            row = df.iloc[idx]
            clean_text = scrub_title(row["combined_text"], row["position_title"])
            pass2_requests.append(build_batch_request(
                custom_id=f"p2_{idx}",
                model=model,
                messages=[
                    {"role": "system", "content": propose_prompt},
                    {"role": "user", "content": f"Propose a plain language title for this posting:\n\n{clean_text[:max_text]}"},
                ],
                response_format_schema=pydantic_to_response_format(ProposedTitle),
                temperature=0.3,
            ))

        pass2_results = run_batch(pass2_requests, tag=f"classify_{series_code}_pass2", description=f"classify {series_code} pass2")
        for idx in none_indices:
            key = f"p2_{idx}"
            if key in pass2_results and "error" not in pass2_results[key]:
                r = pass2_results[key]
                all_results[idx] = {
                    "best_title": r.get("best_title", "error"),
                    "fit_score": r.get("fit_score", 0),
                    "reasoning": r.get("reasoning", ""),
                }
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
    parser.add_argument("--batch", action="store_true",
                        help="Use OpenAI Batch API (50%% cheaper, slower)")
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

    print(f"Classifying {len(df)} postings against {len(titles)} canonical titles" +
          (" (batch mode)" if args.batch else "") + "...")

    if args.batch:
        results = run_classification_batch(df, titles, args.model, args.concurrency, args.series)
    else:
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
