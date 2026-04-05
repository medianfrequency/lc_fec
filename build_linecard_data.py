#!/usr/bin/env python3
"""
Line Card — FEC Bulk Data Processor
====================================
Downloads FEC bulk files for the 2025-2026 cycle and produces a compact
linecard_data.json ready to embed in the Line Card HTML app.

Run once per filing period (quarterly):
  python3 build_linecard_data.py

Outputs:
  linecard_data.json      — embed in linecard.html via inject_fec_data.py
  linecard_data_meta.json — run metadata (counts, timestamp)

Requirements: Python 3.8+, no external packages needed.
"""

import csv
import datetime
import io
import json
import os
import urllib.request
import zipfile

# ── FEC bulk file URLs (2025-2026 cycle) ────────────────────────────────────
FEC_BASE = "https://www.fec.gov/files/bulk-downloads/2026"
FILES = {
    "candidates": f"{FEC_BASE}/cn26.zip",
    "committees": f"{FEC_BASE}/cm26.zip",
    "ccl":        f"{FEC_BASE}/ccl26.zip",
    "schedule_b": f"{FEC_BASE}/oppexp26.zip",
}

# ── Candidate master column indices ─────────────────────────────────────────
CN_ID       = 0
CN_NAME     = 1
CN_PARTY    = 2
CN_STATE    = 4
CN_OFFICE   = 5
CN_DISTRICT = 6
CN_STATUS   = 8
CN_PCC      = 9

# ── Committee master column indices ─────────────────────────────────────────
CM_ID       = 0
CM_NAME     = 1
CM_TYPE     = 8   # Y = party committee (state/local)
CM_STATE    = 6   # Committee state (2-letter abbreviation)
CM_PARTY    = 10  # Party affiliation (DEM, REP, etc.)

# ── Committee-candidate linkage column indices ───────────────────────────────
CCL_CAND_ID = 0
CCL_COMM_ID = 3

# ── Operating expenditures column indices ────────────────────────────────────
SB_CMTE_ID  = 0
SB_NAME     = 8   # Recipient / vendor name
SB_DATE     = 12  # Transaction date (MM/DD/YYYY)
SB_AMT      = 13  # Disbursement amount
SB_PURPOSE  = 14  # Free-text purpose (often blank in oppexp)
SB_CATEGORY = 15  # FEC category label — primary classification target
SB_DESC     = 16  # Category description (fallback)
SB_MEMO     = 18  # Memo text (last-resort fallback)
SB_TRAN_ID  = 21  # Transaction ID (used for deduplication)
SB_AMNDT    = 1   # Amendment indicator

# ── Hardcoded committee IDs ──────────────────────────────────────────────────
# National committees — looked up and pinned by ID for reliability
NATIONAL_COMMITTEE_IDS = {
    "C00000935": "DCCC",
    "C00042366": "DSCC",
    "C00010603": "DNC",
    "C00503789": "DGA Action",
    "C00638320": "DAGA PAC",
    "C00687137": "DAGA PLP",
    "C00756593": "DAGA Individual PAC",
    "C00608380": "DLCC PAC",
}

# State party committees — one primary federal account per state
STATE_PARTY_IDS = {
    "C00005173": "AL", "C00191247": "AK", "C00166710": "AZ", "C00024372": "AR",
    "C00105668": "CA", "C00161786": "CO", "C00167320": "CT", "C00211763": "DE",
    "C00005561": "FL", "C00041269": "GA", "C00212787": "HI", "C00010439": "ID",
    "C00167015": "IL", "C00108613": "IN", "C00035600": "IA", "C00019380": "KS",
    "C00011197": "KY", "C00071365": "LA", "C00179408": "ME", "C00141812": "MD",
    "C00089243": "MA", "C00031054": "MI", "C00025254": "MN", "C00149641": "MS",
    "C00135558": "MO", "C00010033": "MT", "C00003988": "NE", "C00208991": "NV",
    "C00178038": "NH", "C00104471": "NJ", "C00161810": "NM", "C00143230": "NY",
    "C00165688": "NC", "C00013748": "ND", "C00016899": "OH", "C00190934": "OK",
    "C00188367": "OR", "C00167130": "PA", "C00136200": "RI", "C00007658": "SC",
    "C00160937": "SD", "C00167346": "TN", "C00099267": "TX", "C00105973": "UT",
    "C00024679": "VT", "C00155952": "VA", "C00114439": "WA", "C00162578": "WV",
    "C00019331": "WI", "C00001917": "WY",
}

# ── Media taxonomy ───────────────────────────────────────────────────────────
# Five buckets: linear, digital, unclassified, radio, mail
#
# Classification is intentionally strict. Generic signals ("advertising",
# "media buy", "media placement") that could belong to more than one bucket
# are routed to "unclassified" rather than forced into a specific type.

MEDIA_KEYWORDS = {
    "linear": [
        # Broadcast TV
        "broadcast tv","broadcast television","local tv","network tv",
        "tv placement","tv buy","tv ad","tv production","television advertising",
        "television placement","air time","airtime","nbc","cbs","abc","pbs",
        "broadcast media","over the air","over-the-air",
        # Cable TV
        "cable tv","cable television","cable buy","cable placement",
        "cable advertising","cable media","cnn","msnbc","fox news",
    ],
    "digital": [
        # CTV / Streaming video
        "ctv","connected tv","connected television","addressable tv",
        "addressable television","ott","over-the-top","hulu","youtube tv",
        "sling","fubo","peacock","paramount+","tubi","streaming video",
        "programmatic video","video streaming","streaming advertising",
        # Social / display / search
        "facebook","instagram","meta ads","google ads","youtube ads",
        "tiktok","twitter ads","x ads","snapchat","display advertising",
        "programmatic display","search advertising","paid search",
        "banner advertising","online advertising","social media advertising",
        "digital advertising","digital media buy","digital placement",
        "email marketing","sms advertising","text message advertising",
    ],
    "radio": [
        "broadcast radio","terrestrial radio","am radio","fm radio",
        "radio buy","radio placement","radio advertising","radio production",
        "iheartmedia","iheart","cumulus","audacy","townsquare",
        "streaming audio","audio advertising","spotify","pandora",
        "audio buy","audio placement",
    ],
    "mail": [
        "direct mail","mail production","mail house","mail program",
        "direct mail production","print and mail","political mail",
        "mailer","mailing list","mail piece",
    ],
}

UNCLASSIFIED_KEYWORDS = [
    "advertising","advertisement","media buy","media placement","media services",
    "media production","media consulting","media strategy","ad buy","ad placement",
    "ad production","ad spend","ad services","digital","cable","television","tv",
    "streaming","programmatic","online media","web advertising","media",
]

EXCLUDE_KEYWORDS = [
    "credit card payment - see below",
    "compliance services and postage",
    "campaign internet service",
    "campaign telephone",
    "campaign radio subscription",
    "printing - yard signs","printing - signs","t-shirts","t shirts",
    "campaign materials","campaign supplies",
]

def classify(category, purpose, desc):
    fields = [category, purpose, desc]
    combined = " | ".join(f for f in fields if f).lower()
    if not combined:
        return None
    for excl in EXCLUDE_KEYWORDS:
        if excl in combined:
            return None
    for bucket, keywords in MEDIA_KEYWORDS.items():
        for kw in keywords:
            if kw in combined:
                return bucket
    for kw in UNCLASSIFIED_KEYWORDS:
        if kw in combined:
            return "unclassified"
    return None

def download_and_extract(url):
    print(f"  Downloading {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "LineCard/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    print(f"  Downloaded {len(data)/1024/1024:.1f} MB")
    zf = zipfile.ZipFile(io.BytesIO(data))
    target = zf.namelist()[0]
    print(f"  Extracting {target} ...")
    return zf.read(target).decode("latin-1", errors="replace")

def parse_pipe(content):
    return csv.reader(io.StringIO(content), delimiter="|")

def fmt_name(s):
    """LAST, FIRST MIDDLE -> First Middle Last"""
    parts = s.split(",", 1)
    if len(parts) == 2:
        return (parts[1].strip() + " " + parts[0].strip()).title()
    return s.title()

# Known acronyms to preserve after title-casing
_ACRONYMS = {
    'Dnc': 'DNC', 'Dccc': 'DCCC', 'Dscc': 'DSCC', 'Dga': 'DGA',
    'Daga': 'DAGA', 'Dlcc': 'DLCC', 'Pac': 'PAC', 'Llc': 'LLC',
    'Corp': 'Corp.', 'Plp': 'PLP',
}

def fmt_committee_name(s):
    """Title-case a committee name, preserving known acronyms."""
    words = s.strip().title().split()
    return ' '.join(_ACRONYMS.get(w, w) for w in words)

def parse_date(raw):
    """MM/DD/YYYY -> YYYY-MM-DD"""
    if len(raw) == 10 and raw[2] == "/" and raw[5] == "/":
        return f"{raw[6:]}-{raw[:2]}-{raw[3:5]}"
    return raw

# (No name-based lookup needed — IDs are hardcoded above)

# ── Step 1: Load candidate master ───────────────────────────────────────────
print("\n[1/4] Loading candidate master...")
cn_raw = download_and_extract(FILES["candidates"])

INCLUDE_PARTIES = {"DEM", "DFL"}
NE_IND_IDS = {"S4NE00127"}  # Dan Osborn

dem_candidates = {}
for row in parse_pipe(cn_raw):
    if len(row) < 10:
        continue
    cand_id = row[CN_ID].strip()
    party   = row[CN_PARTY].strip().upper()
    status  = row[CN_STATUS].strip().upper()

    if party not in INCLUDE_PARTIES and cand_id not in NE_IND_IDS:
        continue
    if status not in ("C", "P", ""):
        continue

    dem_candidates[cand_id] = {
        "name":     fmt_name(row[CN_NAME].strip()),
        "party":    party,
        "state":    row[CN_STATE].strip().upper(),
        "office":   row[CN_OFFICE].strip().upper(),
        "district": row[CN_DISTRICT].strip(),
        "pcc":      row[CN_PCC].strip(),
        "type":     "candidate",
    }

print(f"  {len(dem_candidates):,} Democratic/DFL candidates")

# ── Step 2: Load committee master — resolve names for hardcoded IDs ──────────
print("\n[2/4] Loading committee master...")
cm_raw = download_and_extract(FILES["committees"])

# Build name lookup for hardcoded IDs
all_ids = set(NATIONAL_COMMITTEE_IDS) | set(STATE_PARTY_IDS)
id_to_name = {}
for row in parse_pipe(cm_raw):
    if len(row) < 2:
        continue
    cmte_id = row[CM_ID].strip()
    if cmte_id in all_ids:
        id_to_name[cmte_id] = row[CM_NAME].strip()

# Build party_committees dict from hardcoded IDs
party_committees = {}

for cmte_id, label in NATIONAL_COMMITTEE_IDS.items():
    name = id_to_name.get(cmte_id, label)
    party_committees[cmte_id] = {
        "name":   fmt_committee_name(name),
        "state":  "US",
        "office": "FEDERAL",
        "type":   "national",
        "pcc":    cmte_id,
    }

for cmte_id, state in STATE_PARTY_IDS.items():
    name = id_to_name.get(cmte_id, f"{state} Democratic Party")
    party_committees[cmte_id] = {
        "name":   fmt_committee_name(name),
        "state":  state,
        "office": "STATE_PARTY",
        "type":   "state_party",
        "pcc":    cmte_id,
    }

nat_count   = sum(1 for c in party_committees.values() if c["type"] == "national")
state_count = sum(1 for c in party_committees.values() if c["type"] == "state_party")
print(f"  {nat_count} national committees, {state_count} state party committees")

# ── Step 3: Committee → candidate mapping ───────────────────────────────────
print("\n[3/4] Loading committee-candidate linkage...")
ccl_raw = download_and_extract(FILES["ccl"])

comm_to_cand = {}
for row in parse_pipe(ccl_raw):
    if len(row) < 4:
        continue
    cand_id = row[CCL_CAND_ID].strip()
    comm_id = row[CCL_COMM_ID].strip()
    if cand_id in dem_candidates and comm_id:
        comm_to_cand[comm_id] = cand_id

# Principal campaign committees from candidate master
for cand_id, cand in dem_candidates.items():
    if cand["pcc"]:
        comm_to_cand[cand["pcc"]] = cand_id

# Party committees map to themselves
for cmte_id in party_committees:
    comm_to_cand[cmte_id] = cmte_id

print(f"  {len(comm_to_cand):,} committees mapped")

# ── Step 4: Process Schedule B disbursements ─────────────────────────────────
print("\n[4/4] Processing disbursements (this takes a minute)...")
sb_raw = download_and_extract(FILES["schedule_b"])

raw_disbs = {}
total_rows = 0

for row in parse_pipe(sb_raw):
    total_rows += 1
    if len(row) <= SB_TRAN_ID:
        continue

    cmte_id = row[SB_CMTE_ID].strip()
    if cmte_id not in comm_to_cand:
        continue

    amndt    = row[SB_AMNDT].strip().upper()
    tran_id  = row[SB_TRAN_ID].strip()
    category = row[SB_CATEGORY].strip() if len(row) > SB_CATEGORY else ""
    purpose  = row[SB_PURPOSE].strip()  if len(row) > SB_PURPOSE  else ""
    desc     = row[SB_DESC].strip()     if len(row) > SB_DESC     else ""

    media_type = classify(category, purpose, desc)
    if not media_type:
        continue

    try:
        amt = float(row[SB_AMT].strip() or "0")
    except ValueError:
        continue
    if amt <= 0:
        continue

    key = (cmte_id, tran_id or f"__notran_{total_rows}")
    raw_disbs[key] = {
        "entity_id": comm_to_cand[cmte_id],
        "amndt":     amndt,
        "id":        tran_id or f"__notran_{total_rows}",
        "v":         row[SB_NAME].strip().title()[:60],
        "desc":      (category or purpose)[:80],
        "a":         round(amt, 2),
        "t":         parse_date(row[SB_DATE].strip()),
        "m":         media_type,
    }

    if total_rows % 500_000 == 0:
        print(f"  ... {total_rows:,} rows processed")

print(f"  {total_rows:,} total rows, {len(raw_disbs):,} media disbursements after dedup")

# Drop terminated, group by entity
disbursements = {}
dropped = 0
for key, d in raw_disbs.items():
    if d["amndt"] == "T":
        dropped += 1
        continue
    entity_id = d.pop("entity_id")
    d.pop("amndt")
    disbursements.setdefault(entity_id, []).append(d)

if dropped:
    print(f"  Dropped {dropped:,} terminated/voided transactions")

# ── Assemble output ──────────────────────────────────────────────────────────
print("\nAssembling output...")
output = {}

# Candidates
for cand_id, cand in dem_candidates.items():
    disbs = disbursements.get(cand_id)
    if not disbs:
        continue
    output[cand_id] = {
        "name":     cand["name"],
        "party":    cand["party"],
        "state":    cand["state"],
        "office":   cand["office"],
        "district": cand["district"],
        "pcc":      cand["pcc"],
        "type":     "candidate",
        "disbs":    sorted(disbs, key=lambda d: d["t"], reverse=True),
    }

# Party committees
for cmte_id, cmte in party_committees.items():
    disbs = disbursements.get(cmte_id)
    if not disbs:
        continue
    output[cmte_id] = {
        "name":     cmte["name"],
        "party":    "DEM",
        "state":    cmte["state"],
        "office":   cmte["office"],
        "district": "",
        "pcc":      cmte["pcc"],
        "type":     cmte["type"],   # "national" | "state_party"
        "disbs":    sorted(disbs, key=lambda d: d["t"], reverse=True),
    }

cand_count   = sum(1 for v in output.values() if v["type"] == "candidate")
nat_out      = sum(1 for v in output.values() if v["type"] == "national")
state_out    = sum(1 for v in output.values() if v["type"] == "state_party")
print(f"  {cand_count} candidates, {nat_out} national committees, {state_out} state party committees")

with open("linecard_data.json", "w") as f:
    json.dump(output, f, separators=(",", ":"))

size_kb = os.path.getsize("linecard_data.json") / 1024
print(f"  Written linecard_data.json ({size_kb:.0f} KB)")

meta = {
    "generated":                     datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "cycle":                         "2026",
    "candidates_with_media":         cand_count,
    "national_committees":           nat_out,
    "state_party_committees":        state_out,
    "total_disbursements_matched":   sum(len(v) for v in disbursements.values()),
    "total_disbursements_processed": total_rows,
    "size_kb":                       round(size_kb, 1),
}
with open("linecard_data_meta.json", "w") as f:
    json.dump(meta, f, indent=2)

print("\n✓ Done!")
print(f"  linecard_data.json       — {size_kb:.0f} KB")
print(f"  linecard_data_meta.json  — run metadata")
print(f"\nNext: python3 inject_fec_data.py")
print(f"\nRe-run after each quarterly filing deadline:")
print(f"  Q1: Apr 15 | Q2: Jul 15 | Q3: Oct 15 | Year-end: Jan 31")