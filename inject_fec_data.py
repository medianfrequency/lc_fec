#!/usr/bin/env python3
"""
inject_fec_data.py
Injects linecard_data.json and linecard_data_meta.json into linecard.html in place.

Usage:
    python3 inject_fec_data.py
    python3 inject_fec_data.py --html path/to/linecard.html \
                                --data path/to/linecard_data.json \
                                --meta path/to/linecard_data_meta.json
"""

import argparse
import json
import re
import sys
from pathlib import Path


def inject(html_path: Path, data_path: Path, meta_path: Path) -> None:
    html = html_path.read_text(encoding="utf-8")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    data_json = json.dumps(data, separators=(",", ":"))
    meta_json = json.dumps(meta, separators=(",", ":"))

    # Replace FEC_DATA — handles placeholder comment, already-injected object, or null
    html, n1 = re.subn(
        r'var FEC_DATA\s*=\s*(?://\s*REPLACE_WITH_JSON_DATA|\{.*?\}|null)',
        f'var FEC_DATA = {data_json}',
        html,
        flags=re.DOTALL,
    )
    if n1 == 0:
        print("ERROR: Could not find FEC_DATA placeholder in HTML.", file=sys.stderr)
        sys.exit(1)

    # Replace FEC_META — handles placeholder comment, already-injected object, or null
    html, n2 = re.subn(
        r'var FEC_META\s*=\s*(?://\s*REPLACE_WITH_META_DATA|\{.*?\}|null)',
        f'var FEC_META = {meta_json}',
        html,
        flags=re.DOTALL,
    )
    if n2 == 0:
        print("ERROR: Could not find FEC_META placeholder in HTML.", file=sys.stderr)
        sys.exit(1)

    html_path.write_text(html, encoding="utf-8")

    cands = len(data)
    generated = meta.get("generated", "unknown")
    print(f"✓ Injected {cands:,} candidates (generated: {generated}) → {html_path}")


def main():
    parser = argparse.ArgumentParser(description="Inject FEC data into linecard.html")
    parser.add_argument("--html", default="linecard.html",  help="Path to linecard.html")
    parser.add_argument("--data", default="linecard_data.json", help="Path to linecard_data.json")
    parser.add_argument("--meta", default="linecard_data_meta.json", help="Path to linecard_data_meta.json")
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