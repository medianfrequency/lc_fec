#!/usr/bin/env python3
"""
inject_ie_data.py
Injects ie_data.json and ie_data_meta.json into ie_linecard.html in place.

Usage:
    python3 inject_ie_data.py
    python3 inject_ie_data.py --html ie_linecard.html \
                               --data ie_data.json \
                               --meta ie_data_meta.json
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

    # Replace placeholder or existing value for IE_DATA
    html, n1 = re.subn(
        r'(var IE_DATA\s*=\s*)//\s*REPLACE_WITH_JSON_DATA',
        rf'\g<1>{data_json}',
        html,
    )
    if n1 == 0:
        html, n1 = re.subn(
            r'(var IE_DATA\s*=\s*)(\{.*?\})(\s*\n)',
            rf'\g<1>{data_json}\3',
            html,
            flags=re.DOTALL,
        )
    if n1 == 0:
        print("ERROR: Could not find IE_DATA placeholder in HTML.", file=sys.stderr)
        sys.exit(1)

    # Replace placeholder or existing value for IE_META
    html, n2 = re.subn(
        r'(var IE_META\s*=\s*)//\s*REPLACE_WITH_META_DATA',
        rf'\g<1>{meta_json}',
        html,
    )
    if n2 == 0:
        html, n2 = re.subn(
            r'(var IE_META\s*=\s*)(\{.*?\})(\s*\n)',
            rf'\g<1>{meta_json}\3',
            html,
            flags=re.DOTALL,
        )
    if n2 == 0:
        print("ERROR: Could not find IE_META placeholder in HTML.", file=sys.stderr)
        sys.exit(1)

    html_path.write_text(html, encoding="utf-8")

    orgs = len(data)
    generated = meta.get("generated", "unknown")[:19]
    print(f"✓ Injected {orgs:,} organizations (generated: {generated}) → {html_path}")


def main():
    parser = argparse.ArgumentParser(description="Inject IE data into ie_linecard.html")
    parser.add_argument("--html", default="ie_linecard.html")
    parser.add_argument("--data", default="ie_data.json")
    parser.add_argument("--meta", default="ie_data_meta.json")
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