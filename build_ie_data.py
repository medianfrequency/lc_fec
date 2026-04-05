#!/usr/bin/env python3
"""
Line Card — FEC Independent Expenditure Data Processor
=======================================================
Downloads the FEC IE bulk CSV for the 2025-2026 cycle and produces a compact
ie_data.json ready to embed in the IE Line Card HTML app.

Captures:
  - IEs supporting Democratic candidates
  - IEs opposing Republican candidates

Output is keyed by spending organization (Super PAC / 527), not candidate.

Run once per day (or whenever fresh data is needed):
  python3 build_ie_data.py

Outputs:
  ie_data.json      — embed in ie_linecard.html via inject_ie_data.py
  ie_data_meta.json — run metadata

Requirements: Python 3.8+, no external packages needed.
"""

import csv
import datetime
import io
import json
import os
import urllib.request

# ── FEC bulk file URL ────────────────────────────────────────────────────────
IE_URL = "https://www.fec.gov/files/bulk-downloads/2026/independent_expenditure_2026.csv"

# ── Column indices (from header row) ─────────────────────────────────────────
IE_CAND_ID    = 0   # Candidate ID
IE_CAND_NAME  = 1   # Candidate name
IE_SPE_ID     = 2   # Spending committee ID
IE_SPE_NAM    = 3   # Spending committee name
IE_ELE_TYPE   = 4   # Election type (P=primary, G=general, etc.)
IE_STATE      = 5   # Candidate office state
IE_DISTRICT   = 6   # Candidate district
IE_OFFICE     = 7   # Candidate office (H/S/P)
IE_PARTY      = 8   # Candidate party affiliation
IE_AMOUNT     = 9   # Expenditure amount
IE_EXP_DATE   = 10  # Expenditure date (DD-MON-YY)
IE_AGG_AMT    = 11  # Aggregate amount
IE_SUP_OPP    = 12  # S=support, O=oppose
IE_PURPOSE    = 13  # Purpose of expenditure
IE_PAYEE      = 14  # Payee name
IE_FILE_NUM   = 15  # Filing number
IE_AMNDT      = 16  # Amendment indicator
IE_TRAN_ID    = 17  # Transaction ID

# ── Republican org name keywords ─────────────────────────────────────────────
# Used only to screen out Republican-aligned orgs filing IEs opposing Republicans.
# These orgs are not approachable from a Democratic media buying perspective.
REP_ORG_KEYWORDS = [
    "conservative","republican","right pac","right fund","gop","freedom caucus",
    "patriot","liberty","maga","america first","trump","reagan","heritage",
    "right to rise","club for growth","crossroads","karl rove",
]

def is_rep_aligned_org(org_name):
    n = org_name.lower()
    return any(kw in n for kw in REP_ORG_KEYWORDS)

# ── Date parser ───────────────────────────────────────────────────────────────
_MONTHS = {
    'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06',
    'JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'
}

def parse_date(raw):
    """DD-MON-YY -> YYYY-MM-DD"""
    parts = raw.strip().upper().split('-')
    if len(parts) == 3:
        day, mon, yr = parts
        month = _MONTHS.get(mon, '00')
        year = ('20' + yr) if len(yr) == 2 else yr
        return f"{year}-{month}-{day.zfill(2)}"
    return raw

# ── Media taxonomy ───────────────────────────────────────────────────────────
MEDIA_KEYWORDS = {
    "linear": [
        "broadcast tv","broadcast television","local tv","network tv",
        "tv placement","tv buy","tv ad","tv production","television advertising",
        "television placement","air time","airtime","nbc","cbs","abc","pbs",
        "broadcast media","over the air","over-the-air",
        "cable tv","cable television","cable buy","cable placement",
        "cable advertising","cable media","cnn","msnbc","fox news",
        "placed media: tv","placed media: television","placed media: cable",
    ],
    "digital": [
        "ctv","connected tv","connected television","addressable tv",
        "addressable television","ott","over-the-top","hulu","youtube tv",
        "sling","fubo","peacock","paramount+","tubi","streaming video",
        "programmatic video","video streaming","streaming advertising",
        "facebook","instagram","meta ads","google ads","youtube ads",
        "tiktok","twitter ads","x ads","snapchat","display advertising",
        "programmatic display","search advertising","paid search",
        "banner advertising","online advertising","social media advertising",
        "digital advertising","digital media buy","digital placement",
        "email marketing","sms advertising","text message advertising",
        "placed media: digital","placed media: online","placed media: internet",
        "placed media: streaming","placed media: video",
    ],
    "radio": [
        "broadcast radio","terrestrial radio","am radio","fm radio",
        "radio buy","radio placement","radio advertising","radio production",
        "iheartmedia","iheart","cumulus","audacy","townsquare",
        "streaming audio","audio advertising","spotify","pandora",
        "audio buy","audio placement",
        "placed media: radio","placed media: audio",
    ],
    "mail": [
        "direct mail","mail production","mail house","mail program",
        "direct mail production","print and mail","political mail",
        "mailer","mailing list","mail piece",
        "placed media: mail","placed media: direct mail",
    ],
}

UNCLASSIFIED_KEYWORDS = [
    "advertising","advertisement","media buy","media placement","media services",
    "media production","media consulting","media strategy","ad buy","ad placement",
    "ad production","ad spend","ad services","digital","cable","television","tv",
    "streaming","programmatic","online media","web advertising","media",
    "placed media","production costs","production cost",
]

EXCLUDE_KEYWORDS = [
    "credit card payment - see below",
    "compliance","legal","accounting","fundraising","staffing",
    "travel","lodging","catering","food","beverage",
    "office","rent","utilities","telephone","internet service",
    "yard signs","t-shirts","apparel","printing - signs",
]

def classify(purpose, payee):
    combined = " | ".join(f for f in [purpose, payee] if f).lower()
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

def is_target(party, sup_opp, org_name):
    """
    Return True if this IE represents approachable outside spending.

    Rules:
    - Supporting any non-Republican candidate: always include (any org alignment)
    - Opposing a Republican candidate: include ONLY if org is not Republican-aligned
      (screens out intraparty Republican opposition we cannot approach)
    """
    party   = party.upper()
    sup_opp = sup_opp.upper()
    is_rep  = "REPUBLICAN" in party or party == "REP"

    if sup_opp == "S":
        # Supporting a non-Republican — include regardless of org name
        return not is_rep
    elif sup_opp == "O":
        # Opposing a Republican — only include if org is Democrat-approachable
        return is_rep and not is_rep_aligned_org(org_name)
    return False

def fmt_org_name(s):
    """
    Title-case org name with smart acronym preservation.
    - Short all-caps words that are NOT common English words stay all-caps (initialisms)
    - Known longer acronyms handled explicitly
    - Apostrophe-containing words title-cased correctly (e.g. Virginia's not Virginia'S)
    """
    # Common short English words that should NOT be treated as initialisms
    COMMON_WORDS = {
        'A','AN','AND','ARE','AS','AT','BE','BUT','BY','DO','FOR','FROM',
        'HAS','HE','IF','IN','IS','IT','ITS','MY','NO','NOT','NOW','OF',
        'ON','OR','OUR','OUT','SO','THE','TO','TOO','UP','US','WAS','WE',
        'WIN','BIG','NEW','OLD','ALL','CAN','GET','HOW','LET','PUT',
        'SAY','SEE','TRY','TWO','USE','WAY','WHO','WHY','YES','YET',
        # Common verbs and nouns that appear all-caps in org names
        'FIGHT','JOBS','THINK','VOTE','SAVE','STOP','FUND','KEEP','HELP',
        'MAKE','TAKE','GIVE','TURN','HOLD','GROW','LEAD','MOVE','RISE',
        'JOIN','BACK','REAL','TRUE','GOOD','BOLD','FAIR','FREE','SAFE',
        'STRONG','FIRST','GREAT','SMART','CLEAN','CLEAR','OPEN','AHEAD',
    }
    FORCE_UPPER = {'AFSCME', 'SEIU', 'NARAL', 'EMILY', 'DCCC', 'DSCC', 'DNC', 'DGA', 'DLCC', 'DAGA'}
    FORCE_TITLE = {
        'Pac': 'PAC', 'Llc': 'LLC', 'Inc': 'Inc.', 'Corp': 'Corp.', 'Usa': 'USA',
    }

    def fix_word(word):
        # Handle apostrophes correctly (e.g. "VIRGINIA'S" -> "Virginia's")
        if "'" in word:
            parts = word.split("'", 1)
            return parts[0].title() + "'" + parts[1].lower()
        # All-caps short word that isn't a common English word = initialism, keep upper
        if word.isupper() and len(word) <= 5 and word not in COMMON_WORDS:
            return word
        # All-caps longer word in known acronym list
        if word.upper() in FORCE_UPPER:
            return word.upper()
        titled = word.title()
        return FORCE_TITLE.get(titled, titled)

    return ' '.join(fix_word(w) for w in s.strip().split())

def capwords(s):
    return (s or '').strip().title()

# ── Download ──────────────────────────────────────────────────────────────────
print("\n[1/2] Downloading IE bulk data...")
req = urllib.request.Request(IE_URL, headers={"User-Agent": "LineCard/1.0"})
with urllib.request.urlopen(req, timeout=120) as resp:
    data = resp.read()
print(f"  Downloaded {len(data)/1024:.0f} KB")

content = data.decode("utf-8", errors="replace")
reader = csv.DictReader(io.StringIO(content))

# ── Process ───────────────────────────────────────────────────────────────────
print("\n[2/2] Processing IE records...")

# Dedup: last write wins per (spe_id, tran_id), then drop terminated
raw = {}
total = 0
matched = 0

for row in reader:
    total += 1

    party    = row.get('cand_pty_aff', '').strip()
    sup_opp  = row.get('sup_opp', '').strip()
    org_name = row.get('spe_nam', '').strip()
    if not is_target(party, sup_opp, org_name):
        continue

    purpose = row.get('pur', '').strip()
    payee   = row.get('pay', '').strip()
    media_type = classify(purpose, payee)
    if not media_type:
        continue

    try:
        amt = float(row.get('exp_amo', '0') or '0')
    except ValueError:
        continue
    if amt <= 0:
        continue

    spe_id  = row.get('spe_id', '').strip()
    tran_id = row.get('tran_id', '').strip()
    amndt   = row.get('amndt_ind', '').strip().upper()
    key     = (spe_id, tran_id or f"__notran_{total}")

    raw[key] = {
        "org_id":   spe_id,
        "org_name": row.get('spe_nam', '').strip(),
        "amndt":    amndt,
        "id":       tran_id or f"__notran_{total}",
        "cand_id":  row.get('cand_id', '').strip(),
        "cand":     row.get('cand_name', '').strip(),
        "state":    row.get('can_office_state', '').strip().upper(),
        "district": row.get('can_office_dis', '').strip(),
        "office":   row.get('can_office', '').strip().upper(),
        "party":    party,
        "sup_opp":  sup_opp.upper(),
        "desc":     purpose[:80],
        "payee":    capwords(payee)[:60],
        "a":        round(amt, 2),
        "t":        parse_date(row.get('exp_date', '')),
        "m":        media_type,
    }
    matched += 1

print(f"  {total:,} total rows, {len(raw):,} media IE records after dedup")

# Drop terminated, group by org
orgs = {}
dropped = 0
for key, d in raw.items():
    if d["amndt"] == "T":
        dropped += 1
        continue
    org_id   = d.pop("org_id")
    org_name = d.pop("org_name")
    d.pop("amndt")

    if org_id not in orgs:
        orgs[org_id] = {
            "name":  fmt_org_name(org_name),
            "disbs": [],
        }
    orgs[org_id]["disbs"].append(d)

if dropped:
    print(f"  Dropped {dropped:,} terminated/voided records")

# Sort disbursements by date desc, derive state coverage
output = {}
for org_id, org in orgs.items():
    disbs = sorted(org["disbs"], key=lambda d: d["t"], reverse=True)

    # States this org has spent in
    states = sorted(set(d["state"] for d in disbs if d["state"]))

    output[org_id] = {
        "name":   org["name"],
        "states": states,
        "disbs":  disbs,
    }

print(f"\n  {len(output):,} spending organizations with media IE spend")

with open("ie_data.json", "w") as f:
    json.dump(output, f, separators=(",", ":"))

size_kb = os.path.getsize("ie_data.json") / 1024
print(f"  Written ie_data.json ({size_kb:.0f} KB)")

meta = {
    "generated":                     datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "cycle":                         "2026",
    "source_url":                    IE_URL,
    "orgs_with_media_ie":            len(output),
    "total_ie_records_matched":      sum(len(v["disbs"]) for v in output.values()),
    "total_ie_records_processed":    total,
    "size_kb":                       round(size_kb, 1),
}
with open("ie_data_meta.json", "w") as f:
    json.dump(meta, f, indent=2)

print("\n✓ Done!")
print(f"  ie_data.json       — {size_kb:.0f} KB")
print(f"  ie_data_meta.json  — run metadata")
print(f"\nNext: python3 inject_ie_data.py")
print(f"\nUpdate daily — IE filings come in on a 24-hour cycle.")