"""
Shared utility for OpenAI Batch API.

Submits requests as a JSONL file, polls for completion, and returns parsed results.
Half the cost of real-time API calls, with a 24-hour completion window.
"""

import json
import os
import tempfile
import time

from openai import OpenAI
from pydantic import BaseModel


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
    return {
        "type": "json_schema",
        "json_schema": {
            "name": model_class.__name__,
            "strict": True,
            "schema": model_class.model_json_schema(),
        },
    }


def submit_batch(requests: list[dict], description: str = "") -> str:
    """Write requests to JSONL, upload, and create a batch. Returns batch ID."""
    client = OpenAI()

    # Write JSONL to temp file
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


def poll_batch(batch_id: str, poll_interval: int = 15) -> dict:
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

    content = client.files.content(batch.output_file_id).text
    results = {}
    for line in content.strip().split("\n"):
        obj = json.loads(line)
        custom_id = obj["custom_id"]
        if obj.get("error"):
            results[custom_id] = {"error": obj["error"]}
        else:
            body = obj["response"]["body"]
            # Extract the parsed content from the chat completion response
            message = body["choices"][0]["message"]
            try:
                parsed = json.loads(message["content"])
            except (json.JSONDecodeError, KeyError, TypeError):
                parsed = {"error": "Failed to parse response content"}
            results[custom_id] = parsed
    return results


def run_batch(requests: list[dict], description: str = "", poll_interval: int = 15) -> dict[str, dict]:
    """Submit, poll, and return results. Convenience wrapper."""
    batch_id = submit_batch(requests, description)
    batch = poll_batch(batch_id, poll_interval)
    if batch.status != "completed":
        return {}
    return download_results(batch_id)
