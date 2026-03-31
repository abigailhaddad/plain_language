# Plain Language Federal Job Titles

Federal job postings use titles like "Management and Program Analyst" and "IT Specialist (PLCYPLN)" that tell jobseekers nothing about what the role involves. This project uses LLMs to read the actual duties from USAJobs postings and propose plain language titles that match private-sector conventions.

**Live site:** https://plain-language-titles.netlify.app

## How it works

The pipeline reads job posting duties (without seeing the current title) and proposes titles you'd find on LinkedIn. A two-pass classification system uses constrained structured output first, then falls back to free-text proposals for edge cases. Original titles are scrubbed from the text to prevent anchoring bias — the existing title is only included as an option if it independently passes a plain-language validation check.

## Setup

```bash
pip install -r requirements.txt
```

You need an OpenAI API key in `.env`:
```
OPENAI_API_KEY=sk-...
```

## Running the pipeline

```bash
# Run all series listed in config.yaml (skips already-completed ones)
python pipeline/run_pipeline.py

# Run a specific series
python pipeline/run_pipeline.py --series 0343

# Force rerun even if already done
python pipeline/run_pipeline.py --series 0343 --force
```

### Pipeline diagram

```
USAJobs R2 Parquets
        |
        v
  +-----------+
  | 1. Fetch  |  Extract postings for a series,
  +-----------+  parse duties/qualifications/summary
        |
        v
  +-----------+
  | 2. Discover|  LLM reads duties (blind, no title)
  +-----------+  and proposes 1-3 plain language titles
        |
        v
  +-------------+
  | 3. Consolidate|  Programmatic dedup, then LLM
  +-------------+  validates each surviving title
        |
        v
  +-------------+
  | 4. Validate  |  Check against OPM plain-language
  +-------------+  guidance; replace generic titles
        |
        v
  +-----------+
  | 5. Enrich |  LLM writes descriptions
  +-----------+  for new/replacement titles
        |
        v
  +-------------------+
  | 6. Classify        |  Pass 1: constrained enum (title list
  | (two-pass)        |    + validated originals)
  +-------------------+  Pass 2: free-text for unmatched
        |
        v
  +-----------+
  | 7. Measure |  Coverage stats, fit scores,
  +-----------+  flag issues
        |
        v
  +-----------+
  | 8. Export  |  Build site JSON with full
  +-----------+  duties + USAJobs links
        |
        v
   Site / Netlify
```

### Pipeline steps

| Step | Script | What it does |
|------|--------|-------------|
| 0 | `download_data.py` | Downloads USAJobs parquet files from public R2 bucket |
| 1 | `fetch_series_jobs.py` | Extracts postings for a series, parses duties/qualifications/summary |
| 2 | `discover_titles.py` | LLM proposes 1-3 plain language titles per posting (blind, no current title) |
| 3 | `consolidate_titles.py` | Programmatic dedup + LLM validates each surviving title |
| 4 | `validate_titles.py` | Checks titles against OPM plain-language guidance, replaces generic ones |
| 5 | `enrich_new_titles.py` | LLM writes descriptions for new/replacement titles |
| 6 | `classify_postings.py` | Two-pass classify: constrained enum, then free-text for unmatched |
| 7 | `measure_coverage.py` | Coverage stats, fit scores, flags issues |
| 8 | `export_site_data.py` | Builds site-ready JSON with full duties and USAJobs links |

## config.yaml

Everything is controlled from `config.yaml`. No need to edit Python code to change behavior.

### Parameters

```yaml
model: gpt-5.4-mini          # LLM for all steps
concurrency: 20               # max parallel API calls
discover_sample_size: 400      # postings sampled for title discovery (0 = all)
classify_sample_size: 0        # postings sampled for classification (0 = all)
min_text_length: 100           # min chars to keep a posting
max_text_length: 5000          # max chars sent to the LLM per posting
min_title_count: 2             # title must appear N times to survive dedup
use_batch_api: false           # OpenAI Batch API for classify (50% cheaper, 24h turnaround)
```

### Adding a new occupational series

Add an entry to the `series` list and run the pipeline. It will skip series that are already done:

```yaml
series:
  - code: "0343"
    name: Management & Program Analysis
    fedTitle: Management and Program Analyst

  - code: "0610"
    name: Nursing
    fedTitle: Nurse
```

```bash
python pipeline/run_pipeline.py   # only runs the new series
```

### Prompts

All LLM prompts are in `config.yaml` under the `prompts` key. Each step reads its prompt at runtime, so you can iterate on prompt wording without touching code:

- **`discover`** — proposes titles from duties text (blind, doesn't see current title)
- **`consolidate_validate`** — validates each deduplicated title (is it a real LinkedIn title? is it specific?)
- **`validate_opm`** — checks titles against OPM September 2025 plain-language guidance
- **`enrich`** — writes 1-2 sentence descriptions for titles
- **`classify`** — pass 1, picks from the validated title list via constrained enum
- **`classify_propose`** — pass 2, proposes a new title for postings that didn't match anything

## Key design decisions

**The LLM never sees the original title.** The current federal title is scrubbed from the duties text before it's sent to the LLM. This prevents the model from anchoring on "IT Specialist" and just rubber-stamping it.

**Original titles can still win — but they have to earn it.** Before classification, each unique original title goes through the same plain-language validation check. If it passes (e.g., "Statistician" is a real, specific title), it's added to the enum as one option among many. If it fails (e.g., "IT SPECIALIST (CUSTSPT)"), it's excluded.

**Two-pass classification.** Pass 1 uses a constrained enum (structured output) for precision. If nothing fits, pass 2 proposes a new title via free text. This avoids both forcing bad fits and leaving postings unclassified.

**Postings without real duties get skipped.** Some postings are just redirect pages ("For additional information on direct hire opportunities..."). These are tagged `missing_duties` and excluded from the site.

## Deploying the site

```bash
netlify deploy --dir=site --prod
```

The site is static HTML/JS/CSS with JSON data files. No build step.

## Repo structure

```
plain-language-titles/
├── pipeline/           # Python pipeline scripts + shared config
├── site/               # Static web frontend
│   ├── js/             # ES modules (data, graph, detail, router, main)
│   └── data/           # Generated JSON for the site
├── data/               # Pipeline intermediate files (gitignored)
├── config.yaml         # All parameters and prompts
├── requirements.txt
└── netlify.toml
```
