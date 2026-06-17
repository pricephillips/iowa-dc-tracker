#!/usr/bin/env python3
"""
Iowa Opposition Events — Notion Sync (GitHub Actions)
=======================================================
Designed to run in CI. Fetches master_opposition.csv from the source repo,
filters to Iowa, and upserts into the Notion database:
  - New rows → created
  - Existing rows (matched by Incident title) → updated if any field changed
  - Rows removed from CSV → left in Notion (no deletes; managed manually)

Requires:
  - NOTION_TOKEN env var (set as a GitHub Actions secret)
  - requests package (pip install requests)
"""

import os
import sys
import csv
import json
import time
import hashlib

import requests

# ── Configuration ─────────────────────────────────────────────────────────────

NOTION_TOKEN    = os.environ.get("NOTION_TOKEN", "")
DATABASE_ID     = "ea2a4a922c6b4971995ac33fc2e8ddf6"
CSV_URL         = "https://raw.githubusercontent.com/pricephillips/data-center-map/main/master_opposition.csv"
NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION  = "2022-06-28"
IOWA_STATES     = {"Iowa", "IA"}

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type":   "application/json",
}

# ── Same lookup tables as import_iowa.py ─────────────────────────────────────

COUNTY_CD = {
    "Allamakee":"IA-01","Benton":"IA-01","Black Hawk":"IA-01","Bremer":"IA-01",
    "Buchanan":"IA-01","Butler":"IA-01","Cedar":"IA-01","Cerro Gordo":"IA-01",
    "Chickasaw":"IA-01","Clayton":"IA-01","Clinton":"IA-01","Delaware":"IA-01",
    "Dubuque":"IA-01","Fayette":"IA-01","Floyd":"IA-01","Franklin":"IA-01",
    "Howard":"IA-01","Jackson":"IA-01","Jones":"IA-01","Linn":"IA-01",
    "Mitchell":"IA-01","Muscatine":"IA-01","Scott":"IA-01","Winneshiek":"IA-01",
    "Appanoose":"IA-02","Davis":"IA-02","Decatur":"IA-02","Des Moines":"IA-02",
    "Grundy":"IA-02","Hamilton":"IA-02","Hardin":"IA-02","Henry":"IA-02",
    "Iowa":"IA-02","Jasper":"IA-02","Jefferson":"IA-02","Johnson":"IA-02",
    "Keokuk":"IA-02","Lee":"IA-02","Louisa":"IA-02","Lucas":"IA-02",
    "Mahaska":"IA-02","Marion":"IA-02","Marshall":"IA-02","Monroe":"IA-02",
    "Poweshiek":"IA-02","Ringgold":"IA-02","Tama":"IA-02","Van Buren":"IA-02",
    "Wapello":"IA-02","Warren":"IA-02","Washington":"IA-02","Wayne":"IA-02",
    "Adair":"IA-03","Adams":"IA-03","Audubon":"IA-03","Cass":"IA-03",
    "Clarke":"IA-03","Dallas":"IA-03","Fremont":"IA-03","Guthrie":"IA-03",
    "Harrison":"IA-03","Madison":"IA-03","Mills":"IA-03","Montgomery":"IA-03",
    "Page":"IA-03","Polk":"IA-03","Pottawattamie":"IA-03","Shelby":"IA-03",
    "Story":"IA-03","Taylor":"IA-03","Union":"IA-03","Boone":"IA-04",
    "Buena Vista":"IA-04","Calhoun":"IA-04","Carroll":"IA-04","Cherokee":"IA-04",
    "Clay":"IA-04","Crawford":"IA-04","Dickinson":"IA-04","Emmet":"IA-04",
    "Greene":"IA-04","Hancock":"IA-04","Humboldt":"IA-04","Ida":"IA-04",
    "Kossuth":"IA-04","Lyon":"IA-04","Monona":"IA-04","O'Brien":"IA-04",
    "Osceola":"IA-04","Palo Alto":"IA-04","Plymouth":"IA-04","Pocahontas":"IA-04",
    "Sac":"IA-04","Sioux":"IA-04","Webster":"IA-04","Winnebago":"IA-04",
    "Worth":"IA-04","Wright":"IA-04",
}

IOWA_COUNTY_FIPS = {
    "Adair":"19001","Adams":"19003","Allamakee":"19005","Appanoose":"19007",
    "Audubon":"19009","Benton":"19011","Black Hawk":"19013","Boone":"19015",
    "Bremer":"19017","Buchanan":"19019","Buena Vista":"19021","Butler":"19023",
    "Calhoun":"19025","Carroll":"19027","Cass":"19029","Cedar":"19031",
    "Cerro Gordo":"19033","Cherokee":"19035","Chickasaw":"19037","Clarke":"19039",
    "Clay":"19041","Clayton":"19043","Clinton":"19045","Crawford":"19047",
    "Dallas":"19049","Davis":"19051","Decatur":"19053","Delaware":"19055",
    "Des Moines":"19057","Dickinson":"19059","Dubuque":"19061","Emmet":"19063",
    "Fayette":"19065","Floyd":"19067","Franklin":"19069","Fremont":"19071",
    "Greene":"19073","Grundy":"19075","Guthrie":"19077","Hamilton":"19079",
    "Hancock":"19081","Hardin":"19083","Harrison":"19085","Henry":"19087",
    "Howard":"19089","Humboldt":"19091","Ida":"19093","Iowa":"19095",
    "Jackson":"19097","Jasper":"19099","Jefferson":"19101","Johnson":"19103",
    "Jones":"19105","Keokuk":"19107","Kossuth":"19109","Lee":"19111",
    "Linn":"19113","Louisa":"19115","Lucas":"19117","Lyon":"19119",
    "Madison":"19121","Mahaska":"19123","Marion":"19125","Marshall":"19127",
    "Mills":"19129","Mitchell":"19131","Monona":"19133","Monroe":"19135",
    "Montgomery":"19137","Muscatine":"19139","O'Brien":"19141","Osceola":"19143",
    "Page":"19145","Palo Alto":"19147","Plymouth":"19149","Pocahontas":"19151",
    "Polk":"19153","Pottawattamie":"19155","Poweshiek":"19157","Ringgold":"19159",
    "Sac":"19161","Scott":"19163","Shelby":"19165","Sioux":"19167",
    "Story":"19169","Tama":"19171","Taylor":"19173","Union":"19175",
    "Van Buren":"19177","Wapello":"19179","Warren":"19181","Washington":"19183",
    "Wayne":"19185","Webster":"19187","Winnebago":"19189","Winneshiek":"19191",
    "Woodbury":"19193","Worth":"19195","Wright":"19197",
}

SOURCE_MAP = {
    "bryce_tracker":"ongoing_monitoring","historical_archive":"historical_archive",
    "datacentertracker_org":"historical_archive","fractracker":"fractracker",
    "bryce_rejection_db":"bryce_rejection_db","datacenter_watch":"datacenter_watch",
    "dc_opposition_site":"dc_opposition_site",
}
VALID_SOURCES = {"fractracker","bryce_rejection_db","datacenter_watch","dc_opposition_site","manual"}

OPP_TYPE_MAP = {
    "zoning_restriction":"Zoning Rejection","zoning":"Zoning Rejection",
    "moratorium":"Local Ordinance","local_ordinance":"Local Ordinance","ordinance":"Local Ordinance",
    "legislation":"Legislative","legislative":"Legislative",
    "legal_challenge":"Legal Challenge","legal":"Legal Challenge","lawsuit":"Legal Challenge",
    "regulatory":"Regulatory","regulation":"Regulatory",
    "utility_dispute":"Utility Dispute","utility":"Utility Dispute",
    "environmental":"Environmental",
    "public_comment":"Community Opposition","community":"Community Opposition",
    "community_opposition":"Community Opposition","petition":"Community Opposition","protest":"Community Opposition",
}

OUTCOME_TO_STATUS = {
    "win":"Resolved","loss":"Resolved","mixed":"Resolved",
    "pending":"Pending","active":"Active",
}
OUTCOME_TO_NOTION_OUTCOME = {
    "win":"Rejected","loss":"Approved","mixed":"Appealed",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_county(raw):
    return (raw or "").replace(" County","").replace(" county","").strip()

def normalize_date(raw):
    import re
    if not raw: return None
    raw = raw.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw): return raw
    if re.match(r"^\d{4}-\d{1,2}$", raw):
        p = raw.split("-"); return f"{p[0]}-{int(p[1]):02d}-01"
    if re.match(r"^\d{4}$", raw): return f"{raw}-01-01"
    return None

def map_opposition_types(raw):
    if not raw: return []
    types = [t.strip().lower() for t in raw.split(";")]
    mapped = [OPP_TYPE_MAP.get(t) for t in types]
    result = list(dict.fromkeys(m for m in mapped if m))
    return result if result else ["Other"]

def normalize_source(raw):
    norm = SOURCE_MAP.get((raw or "").strip(), (raw or "").strip())
    return norm if norm in VALID_SOURCES else "manual"

def row_fingerprint(row):
    """MD5 of the key fields — used to detect changes without storing full payloads."""
    key_fields = ["Incident","County","Date","Opposition Type","Status","Community Outcome",
                  "Source URL","Summary","Objective","Company","Entity","Megawatts","lat","lon"]
    sig = "|".join(str(row.get(f,"")).strip() for f in key_fields)
    return hashlib.md5(sig.encode()).hexdigest()

def build_properties(row):
    county = normalize_county(row.get("County",""))
    cd = COUNTY_CD.get(county,"")
    fips = IOWA_COUNTY_FIPS.get(county,"")
    opp_types = map_opposition_types(row.get("Opposition Type",""))
    date_str = normalize_date(row.get("Date",""))
    source = normalize_source(row.get("data_source",""))
    outcome_raw = (row.get("Community Outcome","") or "").strip().lower()
    status = OUTCOME_TO_STATUS.get(outcome_raw,"Monitoring")
    notion_outcome = OUTCOME_TO_NOTION_OUTCOME.get(outcome_raw)
    company_parts = [row.get("Entity",""),row.get("Company",""),row.get("Hyperscaler","")]
    developer = next((p.strip() for p in company_parts if p.strip()),"")
    notes_parts = []
    if row.get("Summary","").strip(): notes_parts.append(row["Summary"].strip())
    if row.get("Objective","").strip(): notes_parts.append(f"Objective: {row['Objective'].strip()}")
    if row.get("Opposition Groups","").strip(): notes_parts.append(f"Opposition: {row['Opposition Groups'].strip()}")
    notes = "\n\n".join(notes_parts)[:2000]

    props = {
        "Name": {"title": [{"text": {"content": (row.get("Incident") or "Untitled").strip()[:255]}}]},
        "Status": {"select": {"name": status}},
        "Record Subtype": {"select": {"name": "Opposition Event"}},
    }
    if county:
        props["County"] = {"rich_text": [{"text": {"content": county}}]}
    if row.get("City","").strip():
        props["City"] = {"rich_text": [{"text": {"content": row["City"].strip()}}]}
    if cd:
        props["Congressional District"] = {"select": {"name": cd}}
    if fips:
        props["FIPS Code"] = {"rich_text": [{"text": {"content": fips}}]}
    if opp_types:
        props["Opposition Type"] = {"multi_select": [{"name": t} for t in opp_types]}
    if source:
        props["Data Source"] = {"select": {"name": source}}
    if (row.get("Source URL") or "").strip():
        props["Source URL"] = {"url": row["Source URL"].strip()}
    if developer:
        props["Developer / Company"] = {"rich_text": [{"text": {"content": developer}}]}
    if notes:
        props["Notes"] = {"rich_text": [{"text": {"content": notes}}]}
    if notion_outcome:
        props["Outcome"] = {"select": {"name": notion_outcome}}
    if date_str:
        props["Date"] = {"date": {"start": date_str}}
    try:
        props["Lat"] = {"number": float(row.get("lat","") or "")}
    except (ValueError, TypeError): pass
    try:
        props["Lon"] = {"number": float(row.get("lon","") or "")}
    except (ValueError, TypeError): pass
    try:
        props["Project Size (MW)"] = {"number": float(row.get("Megawatts","") or "")}
    except (ValueError, TypeError): pass
    return props

def notion_get(endpoint, params=None):
    resp = requests.get(f"{NOTION_API_BASE}/{endpoint}", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def notion_post(endpoint, body):
    resp = requests.post(f"{NOTION_API_BASE}/{endpoint}", headers=HEADERS, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()

def notion_patch(endpoint, body):
    resp = requests.patch(f"{NOTION_API_BASE}/{endpoint}", headers=HEADERS, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_existing_pages():
    """
    Returns dict of { lowercase_title: page_id } for all existing database pages.
    Also stores a fingerprint property in the page title metadata (via a naming
    convention in the page body) — for change detection we use a simpler approach:
    just re-upsert all rows every run. With 17-100 Iowa rows, this is fast.
    """
    pages = {}
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = notion_post(f"databases/{DATABASE_ID}/query", body)
        for page in resp.get("results", []):
            title_parts = page.get("properties", {}).get("Name", {}).get("title", [])
            if title_parts:
                title = title_parts[0].get("plain_text", "").strip().lower()
                pages[title] = page["id"]
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return pages

# ── Main sync ─────────────────────────────────────────────────────────────────

def main():
    if not NOTION_TOKEN:
        print("ERROR: NOTION_TOKEN not set.")
        sys.exit(1)

    # 1. Fetch CSV
    print(f"Fetching CSV from GitHub …")
    resp = requests.get(CSV_URL, timeout=30)
    resp.raise_for_status()
    lines = resp.content.decode("utf-8-sig").splitlines()
    all_rows = list(csv.DictReader(lines))
    iowa_rows = [r for r in all_rows if r.get("State","").strip() in IOWA_STATES]
    print(f"CSV loaded: {len(all_rows)} total rows, {len(iowa_rows)} Iowa rows.")

    if not iowa_rows:
        print("No Iowa rows found — nothing to sync.")
        sys.exit(0)

    # 2. Fetch existing Notion pages
    print("Fetching existing Notion pages …")
    existing = fetch_existing_pages()
    print(f"  {len(existing)} pages already in database.")

    # 3. Upsert
    created = updated = skipped = errors = 0

    for i, row in enumerate(iowa_rows):
        title = (row.get("Incident") or "Untitled").strip()
        title_key = title.lower()
        props = build_properties(row)

        try:
            if title_key in existing:
                # Update existing page
                page_id = existing[title_key]
                notion_patch(f"pages/{page_id}", {"properties": props})
                print(f"  [{i+1}/{len(iowa_rows)}] ↻ Updated:  {title}")
                updated += 1
            else:
                # Create new page
                notion_post("pages", {
                    "parent": {"database_id": DATABASE_ID},
                    "properties": props
                })
                print(f"  [{i+1}/{len(iowa_rows)}] ✓ Created:  {title}")
                created += 1
        except requests.HTTPError as e:
            print(f"  [{i+1}/{len(iowa_rows)}] ✗ Error:    {title} — {e.response.status_code}: {e.response.text[:200]}")
            errors += 1

        time.sleep(0.35)  # Respect Notion rate limit (~3 req/s)

    print()
    print(f"Sync complete — Created: {created} | Updated: {updated} | Skipped: {skipped} | Errors: {errors}")
    if errors:
        sys.exit(1)  # Signal failure to GitHub Actions

if __name__ == "__main__":
    main()
