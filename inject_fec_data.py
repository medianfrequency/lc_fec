#!/usr/bin/env python3
"""
inject_fec_data.py
Injects linecard_data.json and linecard_data_meta.json into linecard.html in place.

Usage:
    python3 inject_fec_data.py
    python3 inject_fec_data.py --html linecard.html --data linecard_data.json --meta linecard_data_meta.json
"""

import argparse
import json
import sys
from pathlib import Path


# Placeholder strings as they appear in the HTML template
DATA_PLACEHOLDER = '// REPLACE_WITH_JSON_DATA'
META_PLACEHOLDER = '// REPLACE_WITH_META_DATA'


def inject(html_path: Path, data_path: Path, meta_path: Path) -> None:
    html = html_path.read_text(encoding="utf-8")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    data_json = json.dumps(data, separators=(",", ":"))
    meta_json = json.dumps(meta, separators=(",", ":"))

    # ── Inject FEC_DATA ───────────────────────────────────────────────────────
    if DATA_PLACEHOLDER in html:
        # First run — replace placeholder comment
        html = html.replace(DATA_PLACEHOLDER, data_json)
    else:
        # Re-run — find the line with var FEC_DATA and replace the value
        lines = html.splitlines(keepends=True)
        replaced = False
        for i, line in enumerate(lines):
            if line.strip().startswith('var FEC_DATA'):
                lines[i] = f'var FEC_DATA = {data_json}\n'
                replaced = True
                break
        if not replaced:
            print("ERROR: Could not find FEC_DATA in HTML.", file=sys.stderr)
            sys.exit(1)
        html = ''.join(lines)

    # ── Inject FEC_META ───────────────────────────────────────────────────────
    if META_PLACEHOLDER in html:
        html = html.replace(META_PLACEHOLDER, meta_json)
    else:
        lines = html.splitlines(keepends=True)
        replaced = False
        for i, line in enumerate(lines):
            if line.strip().startswith('var FEC_META'):
                lines[i] = f'var FEC_META = {meta_json}\n'
                replaced = True
                break
        if not replaced:
            print("ERROR: Could not find FEC_META in HTML.", file=sys.stderr)
            sys.exit(1)
        html = ''.join(lines)

    html_path.write_text(html, encoding="utf-8")

    cands = len(data)
    generated = meta.get("generated", "unknown")[:19]
    print(f"✓ Injected {cands:,} candidates (generated: {generated}) → {html_path}")


def main():
    parser = argparse.ArgumentParser(description="Inject FEC data into linecard.html")
    parser.add_argument("--html", default="linecard.html")
    parser.add_argument("--data", default="linecard_data.json")
    parser.add_argument("--meta", default="linecard_data_meta.json")
    args = parser.parse_args()

    html_path = Path(args.html)
    data_path = Path(args.data)
    meta_path = Path(args.meta)

    for p in (html_path, data_path, meta_path):
        if not p.exists():
            print(f"ERROR: File not found: {p}", file=sys.stderr)
            sys.exit(1)

    inject(html_path, data_path, meta_path)


if __name__ == "__main__":
    main()