"""Validate managed event envelopes against the installed MCTS schema."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import jsonschema


def main():
    schema = json.loads((Path(__file__).resolve().parents[1] /
                         "resources" / "events-v1.schema.json").read_text())
    validator = jsonschema.Draft202012Validator(schema)
    validator.check_schema(schema)
    for line in sys.stdin:
        try:
            validator.validate(json.loads(line))
            response = {"valid": True}
        except (ValueError, jsonschema.ValidationError) as exc:
            response = {"valid": False, "error": str(exc)}
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
