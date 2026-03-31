#!/usr/bin/env python3
"""
Step 3b: Validate canonical titles against OPM plain language guidance.

Reads the consolidated titles and asks an LLM to reject any that fail the
plain language test. Rejected titles get replaced with more specific alternatives.

Per OPM guidance (Sept 2025 memo, EO 14170), titles must:
- Be written in plain language so people outside government can match their skills
- Accurately convey the nature of the position and skills sought
- Align with private-sector terminology to attract a broader talent pool
- NOT include jargon, abbreviations, or acronyms
- NOT be merely generic or non-descriptive

Usage:
    python validate_titles.py --series 0343
"""

import argparse
import json
import os

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel

load_dotenv()

from config import load_config

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


class TitleReview(BaseModel):
    title: str
    verdict: str  # "pass" or "fail"
    reason: str
    suggested_replacements: list[str]


class ValidationResult(BaseModel):
    reviews: list[TitleReview]


def main():
    parser = argparse.ArgumentParser(description="Validate titles against OPM plain language guidance")
    parser.add_argument("--series", default="0343", help="Occupational series code")
    parser.add_argument("--model", default="gpt-5.4-mini", help="OpenAI model")
    args = parser.parse_args()

    titles_path = os.path.join(DATA_DIR, f"{args.series}_titles.json")
    if not os.path.exists(titles_path):
        print(f"Run consolidate_titles.py first to create {titles_path}")
        return

    with open(titles_path) as f:
        titles_data = json.load(f)

    titles = titles_data["titles"]
    print(f"Validating {len(titles)} canonical titles against OPM plain language guidance...\n")

    # Format titles for the LLM
    titles_text = "\n".join(
        f"- **{t['title']}**: {t['description']}"
        for t in titles
    )

    cfg = load_config()
    system_prompt = cfg["prompts"]["validate_opm"]

    client = OpenAI()
    response = client.beta.chat.completions.parse(
        model=args.model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Review these proposed plain language titles:\n\n{titles_text}"},
        ],
        response_format=ValidationResult,
        temperature=0.2,
    )

    result = response.choices[0].message.parsed

    passed = []
    failed = []
    for review in result.reviews:
        if review.verdict.lower() == "pass":
            passed.append(review)
            print(f"  PASS  {review.title}")
            print(f"        {review.reason}")
        else:
            failed.append(review)
            print(f"  FAIL  {review.title}")
            print(f"        {review.reason}")
            print(f"        Suggested: {', '.join(review.suggested_replacements)}")
        print()

    print(f"\nResults: {len(passed)} passed, {len(failed)} failed")

    # Build updated title list: keep passing titles, replace failing ones with suggestions
    updated_titles = []
    for t in titles:
        review = next((r for r in result.reviews if r.title == t["title"]), None)
        if review and review.verdict.lower() == "fail":
            # Replace with suggested alternatives
            for replacement in review.suggested_replacements:
                updated_titles.append({
                    "title": replacement,
                    "description": f"Split from '{t['title']}': {t['description']}",
                    "absorbs": [],
                    "replaced_from": t["title"],
                })
        else:
            updated_titles.append(t)

    # Save validated titles
    out_path = os.path.join(DATA_DIR, f"{args.series}_titles_validated.json")
    output = {
        "series": args.series,
        "model": args.model,
        "num_titles_before": len(titles),
        "num_passed": len(passed),
        "num_failed": len(failed),
        "num_titles_after": len(updated_titles),
        "validation_details": [
            {
                "title": r.title,
                "verdict": r.verdict,
                "reason": r.reason,
                "suggested_replacements": r.suggested_replacements,
            }
            for r in result.reviews
        ],
        "titles": updated_titles,
    }
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nUpdated title list ({len(updated_titles)} titles) saved to {out_path}")
    print(f"\nNew title set:")
    for t in updated_titles:
        prefix = "[NEW] " if "replaced_from" in t else "      "
        print(f"  {prefix}{t['title']}")


if __name__ == "__main__":
    main()
