"""Shared config loader for the pipeline."""

import os
import yaml

PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PIPELINE_DIR)
DEFAULT_CONFIG = os.path.join(REPO_ROOT, "config.yaml")


def load_config(path=None):
    with open(path or DEFAULT_CONFIG) as f:
        return yaml.safe_load(f)
