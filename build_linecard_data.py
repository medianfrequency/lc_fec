#!/usr/bin/env python3
"""
Line Card — FEC Bulk Data Processor
====================================
Downloads FEC bulk files for the 2025-2026 cycle and produces a compact
linecard_data.json ready to embed in the Line Card iOS app.

Run once per filing period (quarterly):
  python3 build_linecard_data.py

Outputs:
  linecard_data.json   — embed this in the HTML app
  linecard_data_meta.json — run metadata (counts, timestamp)

Requirements: Python 3.8+, no external packages needed.
"""

import csv
import json
import os
import urllib.request
import zipfile
import io
import datetime
import sys

# ── FEC bulk file URLs (2025-2026 cycle) ────────────────────────────────────
# Verified from https://www.fec.gov/data/browse-data/?tab=bulk-data
FEC_BASE = "https://www.fec.gov/files/bulk-downloads/2026"
FILES = {
    "candidates":    f"{FEC_BASE}/cn26.zip",       # Candidate master
    "committees":    f"{FEC_BASE}/cm26.zip",       # Committee master
    "ccl":           f"{FEC_BASE}/ccl26.zip",      # Committee-candidate linkage
    "schedule_b":    f"{FEC_BASE}/oppexp26.zip",   # Operating expenditures (Schedule B)
}

# ── Candidate master column indices (pipe-delimited) ────────────────────────
# CAND_ID|CAND_NAME|CAND_PTY_AFFILIATION|CAND_ELECTION_YR|CAND_OFFICE_ST|
# CAND_OFFICE|CAND_OFFICE_DISTRICT|CAND_ICI|CAND_STATUS|CAND_PCC|...
CN_ID       = 0
CN_NAME     = 1
CN_PARTY    = 2
CN_YR       = 3
CN_STATE    = 4
CN_OFFICE   = 5
CN_DISTRICT = 6
CN_STATUS   = 8
CN_PCC      = 9  # Principal campaign committee

# ── Committee master column indices ─────────────────────────────────────────
CM_ID    = 0
CM_NAME  = 1

# ── Committee-candidate linkage column indices ───────────────────────────────
CCL_CAND_ID  = 0
CCL_COMM_ID  = 3

# ── Operating expenditures (oppexp) column indices ──────────────────────────
# oppexp file format (pipe-delimited):
# CMTE_ID|AMNDT_IND|RPT_YR|RPT_TP|IMAGE_NUM|LINE_NUM|FORM_TP_CD|SCHED_TP_CD|
# NAME|CITY|STATE|ZIP_CODE|TRANSACTION_DT|TRANSACTION_AMT|PURPOSE|
# CATEGORY|CATEGORY_DESC|MEMO_CD|MEMO_TEXT|ENTITY_TP|SUB_ID|FILE_NUM|
# TRAN_ID|BACK_REF_TRAN_ID
SB_CMTE_ID   = 0
SB_NAME      = 8   # Recipient/vendor name
SB_DATE      = 12  # Transaction date
SB_AMT       = 13  # Amount
SB_PURPOSE   = 14  # Purpose of disbursement (primary description field)
SB_MEMO      = 18  # Memo text (supplemental)

# ── Media keyword classifier ─────────────────────────────────────────────────
MEDIA_TYPES = {
    "tv": [
        "television","broadcast","tv buy","tv production","media buy",
        "tv ad","local tv","network tv","nbc","cbs","abc","television advertising",
        "air time","airtime","tv placement","broadcast media",
    ],
    "ctv": [
        "ctv","connected tv","streaming","hulu","youtube tv","sling","fubo",
        "peacock","paramount+","tubi","programmatic video","ott","over-the-top",
        "video streaming","streaming advertising","connected television",
        "addressable tv","addressable television",
    ],
    "digital": [
        "digital","facebook","instagram","meta","google ads","display advertising",
        "online advertising","social media","twitter","tiktok","digital advertising",
        "banner","programmatic","search advertising","online media","web advertising",
        "digital media","email marketing","sms","text message advertising",
    ],
    "radio": [
        "radio","iheartmedia","cumulus","audacy","terrestrial radio",
        "am radio","fm radio","spotify","pandora","audio advertising","radio buy",
        "radio placement","radio production","radio advertising",
    ],
    "mail": [
        "direct mail","mail production","printing","postage","mailing list",
        "mail house","mailer","political mail","mail program",
        "direct mail production","print and mail",
    ],
    "cable": [
        "cable","cnn","msnbc","fox news","cable tv","cable advertising",
        "cable buy","cable placement","cable television","cable media",
    ],
}

def classify(text):
    if not text:
        return None
    t = text.lower()
    for media_type, keywords in MEDIA_TYPES.items():
        for kw in keywords:
            if kw in t:
                return media_type
    return None

def download_and_extract(url, filename_hint):
    print(f"  Downloading {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "LineCard/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    print(f"  Downloaded {len(data)/1024/1024:.1f} MB")
    zf = zipfile.ZipFile(io.BytesIO(data))
    # Find the data file inside the zip
    names = zf.namelist()
    target = names[0]
    print(f"  Extracting {target} ...")
    content = zf.read(target).decode("latin-1", errors="replace")
    return content

def parse_pipe(content):
    return csv.reader(io.StringIO(content), delimiter="|")

def fmt_name(s):
    # "LAST, FIRST MIDDLE" -> "First Middle Last"
    parts = s.split(",", 1)
    if len(parts) == 2:
        return (parts[1].strip() + " " + parts[0].strip()).title()
    return s.title()

# ── Step 1: Load candidate master — filter to active Dems (+ DFL + IND in NE) ──
print("\n[1/4] Loading candidate master...")
cn_raw = download_and_extract(FILES["candidates"], "cn")
dem_candidates = {}  # cand_id -> {name, party, state, office, district, pcc}

INCLUDE_PARTIES = {"DEM", "DFL"}  # IND handled separately for NE
NE_IND_IDS = {"S4NE00127"}  # Dan Osborn hardcoded

for row in parse_pipe(cn_raw):
    if len(row) < 10:
        continue
    party = row[CN_PARTY].strip().upper()
    cand_id = row[CN_ID].strip()
    status = row[CN_STATUS].strip().upper()
    yr = row[CN_YR].strip()

    # Include active Dems/DFL, plus known NE independents
    is_dem = party in INCLUDE_PARTIES
    is_ne_ind = cand_id in NE_IND_IDS
    if not (is_dem or is_ne_ind):
        continue
    if status not in ("C", "P", ""):  # C=current, P=prior (keep both)
        continue

    dem_candidates[cand_id] = {
        "name":     fmt_name(row[CN_NAME].strip()),
        "party":    party,
        "state":    row[CN_STATE].strip().upper(),
        "office":   row[CN_OFFICE].strip().upper(),
        "district": row[CN_DISTRICT].strip(),
        "pcc":      row[CN_PCC].strip(),
    }

print(f"  Found {len(dem_candidates)} Democratic/DFL candidates")

# ── Step 2: Load committee-candidate linkage to map committee -> candidate ───
print("\n[2/4] Loading committee-candidate linkage...")
ccl_raw = download_and_extract(FILES["ccl"], "ccl")
comm_to_cand = {}  # committee_id -> cand_id

for row in parse_pipe(ccl_raw):
    if len(row) < 4:
        continue
    cand_id = row[CCL_CAND_ID].strip()
    comm_id = row[CCL_COMM_ID].strip()
    if cand_id in dem_candidates and comm_id:
        comm_to_cand[comm_id] = cand_id

# Also add principal campaign committees from candidate master
for cand_id, cand in dem_candidates.items():
    if cand["pcc"]:
        comm_to_cand[cand["pcc"]] = cand_id

print(f"  Mapped {len(comm_to_cand)} committees to Democratic candidates")

# ── Step 3: Process Schedule B — stream through, keep only media matches ─────
print("\n[3/4] Processing Schedule B disbursements (this takes a minute)...")
sb_raw = download_and_extract(FILES["schedule_b"], "itpas2")

disbursements = {}  # cand_id -> list of {vendor, desc, amt, date, type}
total_rows = 0
matched_rows = 0

for row in parse_pipe(sb_raw):
    total_rows += 1
    if len(row) < 15:
        continue

    cmte_id = row[SB_CMTE_ID].strip()
    if cmte_id not in comm_to_cand:
        continue

    cand_id = comm_to_cand[cmte_id]

    # Purpose field is the primary description; memo is supplemental
    purpose = row[SB_PURPOSE].strip() if len(row) > SB_PURPOSE else ""
    memo    = row[SB_MEMO].strip()    if len(row) > SB_MEMO    else ""
    vendor  = row[SB_NAME].strip()

    # Classify on purpose first, then memo, then vendor name
    desc = purpose or memo
    media_type = classify(purpose) or classify(memo) or classify(vendor)
    if not media_type:
        continue

    try:
        amt = float(row[SB_AMT].strip() or "0")
    except ValueError:
        continue

    if amt <= 0:
        continue

    date_raw = row[SB_DATE].strip()
    # FEC format: MMDDYYYY
    if len(date_raw) == 8:
        date = date_raw[4:] + "-" + date_raw[:2] + "-" + date_raw[2:4]
    else:
        date = date_raw

    if cand_id not in disbursements:
        disbursements[cand_id] = []

    disbursements[cand_id].append({
        "v": vendor.title()[:60],       # vendor name (truncated)
        "d": desc[:80],                  # description (truncated)
        "a": round(amt, 2),             # amount
        "t": date,                      # date
        "m": media_type,                # media type
    })
    matched_rows += 1

    if total_rows % 500000 == 0:
        print(f"  ... processed {total_rows:,} rows, {matched_rows:,} media matches so far")

print(f"  Processed {total_rows:,} total rows, {matched_rows:,} media disbursements matched")

# ── Step 4: Assemble output JSON ─────────────────────────────────────────────
print("\n[4/4] Assembling output...")

output = {}
for cand_id, cand in dem_candidates.items():
    if cand_id not in disbursements:
        continue  # Skip candidates with no media spend
    disbs = disbursements[cand_id]
    total_media = sum(d["a"] for d in disbs)
    # Skip if trivially small
    if total_media < 200:
        continue
    output[cand_id] = {
        "name":     cand["name"],
        "party":    cand["party"],
        "state":    cand["state"],
        "office":   cand["office"],
        "district": cand["district"],
        "total":    round(total_media, 2),
        "disbs":    disbs,
    }

print(f"  {len(output)} candidates with media spend")

with open("linecard_data.json", "w") as f:
    json.dump(output, f, separators=(",", ":"))

size_kb = os.path.getsize("linecard_data.json") / 1024
print(f"  Written linecard_data.json ({size_kb:.0f} KB)")

# Meta file
meta = {
    "generated": datetime.datetime.utcnow().isoformat() + "Z",
    "cycle": "2026",
    "candidates_with_media": len(output),
    "total_disbursements_matched": matched_rows,
    "total_disbursements_processed": total_rows,
    "size_kb": round(size_kb, 1),
}
with open("linecard_data_meta.json", "w") as f:
    json.dump(meta, f, indent=2)

print("\n✓ Done!")
print(f"  linecard_data.json       — {size_kb:.0f} KB, embed in Line Card HTML")
print(f"  linecard_data_meta.json  — run metadata")
print(f"\nNext: copy linecard_data.json into your Line Card HTML file.")
print(f"Re-run after each quarterly filing deadline:")
print(f"  Q1: April 15 | Q2: July 15 | Q3: October 15 | Year-end: January 31")
