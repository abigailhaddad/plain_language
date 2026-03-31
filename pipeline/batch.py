"""
Shared utility for OpenAI Batch API.

Submits requests as a JSONL file, polls for completion, and returns parsed results.
Half the cost of real-time API calls, with a 24-hour completion window.

State is saved to a JSON file so the process can be restarted without resubmitting.
"""

import json
import os
import tempfile
import time

from openai import OpenAI
from pydantic import BaseModel

PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PIPELINE_DIR)
BATCH_STATE_DIR = os.path.join(REPO_ROOT, "data")


def _state_path(tag: str) -> str:
    return os.path.join(BATCH_STATE_DIR, f"_batch_{tag}.json")


def _save_state(tag: str, state: dict):
    os.makedirs(BATCH_STATE_DIR, exist_ok=True)
    with open(_state_path(tag), "w") as f:
        json.dump(state, f, indent=2)


def _load_state(tag: str) -> dict | None:
    path = _state_path(tag)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def _clear_state(tag: str):
    path = _state_path(tag)
    if os.path.exists(path):
        os.unlink(path)


def build_batch_request(custom_id: str, model: str, messages: list[dict],
                        response_format_schema: dict, temperature: float = 0.1) -> dict:
    """Build a single batch request line."""
    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "response_format": response_format_schema,
        },
    }


def pydantic_to_response_format(model_class: type[BaseModel]) -> dict:
    """Convert a Pydantic model to the response_format dict for the batch API."""
    schema = model_class.model_json_schema()
    schema["additionalProperties"] = False
    return {
        "type": "json_schema",
        "json_schema": {
            "name": model_class.__name__,
            "strict": True,
            "schema": schema,
        },
    }


def submit_batch(requests: list[dict], description: str = "") -> str:
    """Write requests to JSONL, upload, and create a batch. Returns batch ID."""
    client = OpenAI()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        for req in requests:
            f.write(json.dumps(req) + "\n")
        jsonl_path = f.name

    print(f"  Uploading {len(requests)} requests ({os.path.getsize(jsonl_path) / 1024:.0f} KB)...")

    with open(jsonl_path, "rb") as f:
        file_obj = client.files.create(file=f, purpose="batch")

    os.unlink(jsonl_path)

    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": description[:512]} if description else None,
    )

    print(f"  Batch created: {batch.id}")
    return batch.id


def poll_batch(batch_id: str, poll_interval: int = 30) -> dict:
    """Poll until batch completes. Returns the batch object."""
    client = OpenAI()
    start = time.time()

    while True:
        batch = client.batches.retrieve(batch_id)
        elapsed = time.time() - start
        status = batch.status
        completed = batch.request_counts.completed if batch.request_counts else 0
        total = batch.request_counts.total if batch.request_counts else 0

        print(f"\r  [{elapsed:.0f}s] {status}: {completed}/{total} complete", end="", flush=True)

        if status in ("completed", "failed", "expired", "cancelled"):
            print()
            if status != "completed":
                print(f"  Batch {status}!")
                if batch.errors and batch.errors.data:
                    for err in batch.errors.data[:5]:
                        print(f"    {err.code}: {err.message}")
            return batch

        time.sleep(poll_interval)


def download_results(batch_id: str) -> dict[str, dict]:
    """Download batch results. Returns {custom_id: parsed_response_body}."""
    client = OpenAI()
    batch = client.batches.retrieve(batch_id)

    if not batch.output_file_id:
        print(f"  No output file for batch {batch_id}")
        return {}

    print(f"  Downloading results from batch {batch_id}...")
    content = client.files.content(batch.output_file_id).text
    results = {}
    for line in content.strip().split("\n"):
        obj = json.loads(line)
        custom_id = obj["custom_id"]
        if obj.get("error"):
            results[custom_id] = {"error": obj["error"]}
        else:
            body = obj["response"]["body"]
            message = body["choices"][0]["message"]
            try:
                parsed = json.loads(message["content"])
            except (json.JSONDecodeError, KeyError, TypeError):
                parsed = {"error": "Failed to parse response content"}
            results[custom_id] = parsed
    print(f"  Got {len(results)} results")
    return results


def run_batch(requests: list[dict], tag: str, description: str = "",
              poll_interval: int = 30) -> dict[str, dict]:
    """Submit, poll, and return results. Resumes from saved state if available.

    Args:
        requests: List of batch request dicts (only used if no saved state).
        tag: Unique tag for this batch (e.g., "classify_2210"). Used to save/resume state.
        description: Optional description for the batch.
        poll_interval: Seconds between status checks.
    """
    state = _load_state(tag)

    if state and state.get("batch_id"):
        batch_id = state["batch_id"]
        # Check if this batch is still valid
        client = OpenAI()
        try:
            existing = client.batches.retrieve(batch_id)
            status = existing.status
        except Exception:
            status = None

        if status == "completed":
            print(f"  Resuming: batch {batch_id} already completed, downloading results...")
            results = download_results(batch_id)
            _clear_state(tag)
            return results
        elif status in ("validating", "in_progress", "finalizing"):
            print(f"  Resuming: batch {batch_id} is {status}, polling...")
            batch = poll_batch(batch_id, poll_interval)
            if batch.status == "completed":
                results = download_results(batch_id)
                _clear_state(tag)
                return results
            _clear_state(tag)
            return {}
        elif status in ("failed", "expired", "cancelled"):
            print(f"  Previous batch {batch_id} {status}, resubmitting...")
            _clear_state(tag)
        else:
            print(f"  Previous batch {batch_id} not found, resubmitting...")
            _clear_state(tag)

    # Fresh submit
    batch_id = submit_batch(requests, description)
    _save_state(tag, {"batch_id": batch_id, "tag": tag, "num_requests": len(requests)})

    batch = poll_batch(batch_id, poll_interval)
    if batch.status == "completed":
        results = download_results(batch_id)
        _clear_state(tag)
        return results

    _clear_state(tag)
    return {}
