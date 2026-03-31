#!/usr/bin/env python3
"""
Step 3c: Generate proper descriptions for newly created titles from validation.

The validation step creates replacement titles with placeholder descriptions.
This script asks the LLM to write proper descriptions for each.

Usage:
    python enrich_new_titles.py --series 0343
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


class TitleDescription(BaseModel):
    title: str
    description: str


class EnrichedTitles(BaseModel):
    titles: list[TitleDescription]


def main():
    parser = argparse.ArgumentParser(description="Enrich new title descriptions")
    parser.add_argument("--series", default="0343", help="Occupational series code")
    parser.add_argument("--model", default="gpt-5.4-mini", help="OpenAI model")
    args = parser.parse_args()

    validated_path = os.path.join(DATA_DIR, f"{args.series}_titles_validated.json")
    if not os.path.exists(validated_path):
        print(f"Run validate_titles.py first")
        return

    with open(validated_path) as f:
        data = json.load(f)

    # Find titles that need descriptions (empty or placeholder)
    needs_description = [t for t in data["titles"] if not t.get("description") or "replaced_from" in t]

    if not needs_description:
        print("All titles already have descriptions.")
        return

    print(f"Generating descriptions for {len(needs_description)} new titles...")

    titles_text = "\n".join(f"- {t['title']}" for t in needs_description)

    cfg = load_config()
    enrich_prompt = cfg["prompts"]["enrich"]

    client = OpenAI()
    response = client.beta.chat.completions.parse(
        model=args.model,
        messages=[
            {"role": "system", "content": enrich_prompt},
            {"role": "user", "content": f"These are plain language job titles for federal positions in series 0343 "
             f"(Management and Program Analysis):\n\n{titles_text}"},
        ],
        response_format=EnrichedTitles,
        temperature=0.2,
    )

    result = response.choices[0].message.parsed
    desc_map = {t.title: t.description for t in result.titles}

    # Update descriptions
    for t in data["titles"]:
        if t["title"] in desc_map:
            t["description"] = desc_map[t["title"]]
            print(f"  {t['title']}: {t['description']}")

    with open(validated_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nUpdated {validated_path}")


if __name__ == "__main__":
    main()
