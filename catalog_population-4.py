#!/usr/bin/env python3
"""
catalog_population.py
=====================
Enumerate the full population of clinical psychology journal articles
(2021–2025) from OpenAlex, filtered to the top 10 empirical journals
by Scimago/SJR ranking in the Clinical Psychology subject category.

Follows the OpenAlex two-step pattern (developers.openalex.org):
  Step 1: Resolve each journal ISSN → OpenAlex source ID
  Step 2: Filter works by primary_location.source.id

Usage:
    # With API key (required since Feb 2025; free = 100K credits/day):
    python catalog_population.py --api-key YOUR_KEY

    # Debug mode — prints URLs, fetches 1 page, shows response:
    python catalog_population.py --api-key YOUR_KEY --debug

    # Via environment variable:
    export OPENALEX_API_KEY=YOUR_KEY
    python catalog_population.py

Get a free API key:
    https://developers.openalex.org/guides/authentication

Output:
    articles.csv       — one row per article
    catalog_stats.txt  — summary counts
    resolved_sources.json — ISSN → OpenAlex source ID mapping (cached)

No full text or PDFs are accessed. Only metadata is collected.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime

import requests

# ---------------------------------------------------------------------------
# Journal list
# ---------------------------------------------------------------------------
# Top 10 *empirical* Clinical Psychology journals by SJR (2024 ranking).
# Review-only journals were excluded and backfilled from the SJR list.
#
# Each entry: (ISSN-L, journal name, SJR 2024, SJR rank in category)
# Source: Scimago category 3203 (Clinical Psychology), retrieved 2026-03-15

JOURNALS = [
    ("0033-3190", "Psychotherapy and Psychosomatics",              4.904,   4),
    ("2167-7026", "Clinical Psychological Science",                2.513,   7),
    ("2062-5871", "Journal of Behavioral Addictions",              2.260,  10),
    ("0144-6657", "British Journal of Clinical Psychology",        2.256,  11),
    ("0022-006X", "Journal of Consulting and Clinical Psychology", 2.231,  12),
    ("0887-6185", "Journal of Anxiety Disorders",                  2.171,  13),
    ("1073-1911", "Assessment",                                    2.158,  14),
    ("0165-0327", "Journal of Affective Disorders",                2.121,  16),
    ("0021-843X", "Journal of Abnormal Psychology",                2.044,  18),
    ("0005-7967", "Behaviour Research and Therapy",                2.009,  19),
]

# Journal of Abnormal Psychology was renamed in 2022 to
# "Journal of Psychopathology and Clinical Science" (ISSN 2769-755X).
# We resolve both ISSNs; OpenAlex may map them to the same source or not.
EXTRA_ISSNS = [
    ("2769-755X", "J. of Psychopathology and Clinical Science (renamed J. Abnormal Psych.)"),
]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = "https://api.openalex.org"
YEAR_START = 2021
YEAR_END = 2025
PER_PAGE = 100  # OpenAlex max per page
OUTPUT_CSV = "articles.csv"
STATS_FILE = "catalog_stats.txt"
SOURCES_CACHE = "resolved_sources.json"
PROGRESS_FILE = "catalog_progress.json"

FIELDNAMES = [
    "openalex_id",
    "doi",
    "title",
    "publication_year",
    "publication_date",
    "journal_name",
    "journal_issn",
    "publisher",
    "is_oa",
    "oa_status",
    "cited_by_count",
    "authors_short",
    "num_authors",
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Catalog clinical psychology articles from OpenAlex.",
        epilog="Get a free API key: https://developers.openalex.org/guides/authentication",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("OPENALEX_API_KEY", ""),
        help="OpenAlex API key (or set OPENALEX_API_KEY env var). "
             "Required since Feb 2025. Free key = 100K credits/day.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print request URLs, show response preview, stop after 1 page.",
    )
    parser.add_argument(
        "--output", "-o",
        default=OUTPUT_CSV,
        help=f"Output CSV path (default: {OUTPUT_CSV}).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume an interrupted run (reads progress file).",
    )
    parser.add_argument(
        "--refresh-sources",
        action="store_true",
        help="Force re-resolve ISSNs to source IDs (ignore cache).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api_get(session, url, params, debug=False):
    """
    GET with retries + backoff. Returns parsed JSON or exits on failure.
    """
    if debug:
        req = requests.Request("GET", url, params=params)
        prepared = session.prepare_request(req)
        print(f"  DEBUG URL: {prepared.url}")

    for attempt in range(5):
        try:
            resp = session.get(url, params=params, timeout=30)

            if debug and attempt == 0:
                print(f"  DEBUG HTTP {resp.status_code}, {len(resp.content)} bytes")

            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"  Rate limited (429) — waiting {wait}s …")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                print("ERROR: 403 Forbidden.")
                print("  Your API key may be invalid or credits exhausted.")
                print("  Get a free key: https://developers.openalex.org/guides/authentication")
                sys.exit(1)
            if resp.status_code == 409:
                print("ERROR: 409 Conflict — daily credit limit reached.")
                print("  Free tier = 100 credits/day without key.")
                print("  Get a free key for 100K credits/day.")
                sys.exit(1)
            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"  Request error: {e} — retrying in {wait}s …")
            time.sleep(wait)

    print("ERROR: Exceeded retry limit.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Step 1: Resolve ISSNs → OpenAlex source IDs
# ---------------------------------------------------------------------------

def resolve_issns(session, api_key, debug=False) -> dict:
    """
    For each journal ISSN, call /sources/issn:XXXX-XXXX to get the
    OpenAlex source ID (e.g. S12345678). Returns a dict mapping
    ISSN → {openalex_id, display_name, works_count}.

    This follows the OpenAlex "When You Have External IDs" pattern:
    https://developers.openalex.org/guides/key-concepts#when-you-have-external-ids
    """
    # Check cache first
    if not debug and os.path.exists(SOURCES_CACHE):
        with open(SOURCES_CACHE) as f:
            cached = json.load(f)
        print(f"Loaded {len(cached)} source IDs from cache ({SOURCES_CACHE})")
        return cached

    all_issns = [(issn, name) for issn, name, *_ in JOURNALS] + \
                [(issn, name) for issn, name in EXTRA_ISSNS]

    resolved = {}
    params = {}
    if api_key:
        params["api_key"] = api_key

    print(f"\nResolving {len(all_issns)} ISSNs to OpenAlex source IDs …")
    print(f"{'ISSN':<12s}  {'Source ID':<28s}  {'Works':>8s}  Name")
    print("-" * 90)

    for issn, expected_name in all_issns:
        url = f"{API_BASE}/sources/issn:{issn}"
        try:
            data = api_get(session, url, params, debug=debug)
        except SystemExit:
            raise
        except Exception as e:
            print(f"  WARNING: Could not resolve ISSN {issn}: {e}")
            continue

        if not data or "id" not in data:
            print(f"  WARNING: No source found for ISSN {issn} ({expected_name})")
            continue

        oa_id = data["id"]                # e.g. "https://openalex.org/S12345"
        short_id = oa_id.split("/")[-1]   # e.g. "S12345"
        display_name = data.get("display_name", "")
        works_count = data.get("works_count", 0)

        resolved[issn] = {
            "openalex_id": oa_id,
            "short_id": short_id,
            "display_name": display_name,
            "works_count": works_count,
        }

        print(f"{issn:<12s}  {short_id:<28s}  {works_count:>8,}  {display_name}")
        time.sleep(0.1)

    # Save cache
    with open(SOURCES_CACHE, "w") as f:
        json.dump(resolved, f, indent=2)
    print(f"\nSaved source ID mapping to {SOURCES_CACHE}")

    return resolved


# ---------------------------------------------------------------------------
# Step 2: Build filter and fetch works
# ---------------------------------------------------------------------------

def build_filter_string(source_ids: list[str]) -> str:
    """
    Build the works filter using OpenAlex source IDs (not ISSNs).
    Follows: primary_location.source.id:S123|S456|...

    Notes:
    - type:article (not "journal-article" — OpenAlex uses its own type taxonomy)
    - XPAC works are excluded by default; no filter needed (is_xpac filter
      only works when include_xpac=true is set as a query parameter)
    """
    id_filter = "|".join(source_ids)
    parts = [
        f"primary_location.source.id:{id_filter}",
        f"publication_year:{YEAR_START}-{YEAR_END}",
        "type:article",
        "is_retracted:false",
    ]
    return ",".join(parts)


def select_fields() -> str:
    """Fields to request (reduces payload)."""
    return ",".join([
        "id", "doi", "title", "publication_year", "publication_date",
        "primary_location", "open_access", "cited_by_count",
        "authorships", "biblio",
    ])


def extract_row(work: dict) -> dict:
    """Transform one OpenAlex work record into a flat CSV row."""
    primary = work.get("primary_location") or {}
    source = primary.get("source") or {}
    oa = work.get("open_access") or {}

    authorships = work.get("authorships") or []
    if authorships:
        first = (authorships[0].get("author") or {}).get("display_name", "Unknown")
        authors_short = f"{first} et al." if len(authorships) > 1 else first
    else:
        authors_short = ""

    doi_raw = work.get("doi") or ""
    doi = doi_raw.replace("https://doi.org/", "") if doi_raw else ""

    return {
        "openalex_id": work.get("id", ""),
        "doi": doi,
        "title": (work.get("title") or "").replace("\n", " "),
        "publication_year": work.get("publication_year", ""),
        "publication_date": work.get("publication_date", ""),
        "journal_name": source.get("display_name", ""),
        "journal_issn": source.get("issn_l", ""),
        "publisher": source.get("host_organization_name", ""),
        "is_oa": oa.get("is_oa", False),
        "oa_status": oa.get("oa_status", ""),
        "cited_by_count": work.get("cited_by_count", 0),
        "authors_short": authors_short,
        "num_authors": len(authorships),
    }


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------

def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"cursor": "*", "rows_written": 0}


def save_progress(cursor: str, rows_written: int):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"cursor": cursor, "rows_written": rows_written}, f)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    output_csv = args.output

    if not args.api_key:
        print("WARNING: No --api-key provided.")
        print("  Without a key you get 100 credits/day, then 409 errors.")
        print("  The mailto/polite-pool was retired Feb 2025.")
        print("  Get a free key: https://developers.openalex.org/guides/authentication\n")

    print("=" * 70)
    print("Clinical Psychology Article Catalog — OpenAlex")
    print(f"Date range:  {YEAR_START}–{YEAR_END}")
    print(f"Journals:    {len(JOURNALS)} (top empirical by SJR 2024)")
    print(f"Output:      {output_csv}")
    print(f"XPAC:        excluded by default (no filter needed)")
    if args.api_key:
        print(f"API key:     {args.api_key[:8]}…{'*' * (len(args.api_key) - 8)}")
    print("=" * 70)

    session = requests.Session()
    session.headers.update({"User-Agent": "ArticleCatalog/2.0"})

    # ----------------------------------------------------------------
    # Step 1: Resolve ISSNs → OpenAlex source IDs
    # ----------------------------------------------------------------
    if args.refresh_sources and os.path.exists(SOURCES_CACHE):
        os.remove(SOURCES_CACHE)

    resolved = resolve_issns(session, args.api_key, debug=args.debug)

    if not resolved:
        print("\nERROR: Could not resolve any ISSNs to source IDs.")
        print("  Check your API key and network connection.")
        sys.exit(1)

    # Deduplicate source IDs (renamed journals may share one)
    source_id_map = {}
    for issn, info in resolved.items():
        sid = info["short_id"]
        if sid not in source_id_map:
            source_id_map[sid] = info["display_name"]

    source_ids = list(source_id_map.keys())
    print(f"\n{len(source_ids)} unique source IDs to query.")

    # ----------------------------------------------------------------
    # Step 2: Build filter and paginate through works
    # ----------------------------------------------------------------
    filt = build_filter_string(source_ids)
    sel = select_fields()

    if args.resume:
        progress = load_progress()
    else:
        progress = {"cursor": "*", "rows_written": 0}

    cursor = progress["cursor"]
    rows_written = progress["rows_written"]
    resuming = rows_written > 0

    if resuming:
        print(f"\nResuming from row {rows_written}, cursor={cursor[:30]}…")
        mode = "a"
    else:
        mode = "w"

    params = {
        "filter": filt,
        "select": sel,
        "sort": "publication_date:asc",
        "per_page": PER_PAGE,
        "cursor": cursor,
    }
    if args.api_key:
        params["api_key"] = args.api_key

    total_count = None
    page_num = rows_written // PER_PAGE + 1

    print(f"\nFetching works …\n")

    with open(output_csv, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        if not resuming:
            writer.writeheader()

        while True:
            params["cursor"] = cursor

            data = api_get(session, f"{API_BASE}/works", params, debug=(args.debug and page_num == 1))
            meta = data.get("meta", {})

            if total_count is None:
                total_count = meta.get("count", 0)
                print(f"Total population: {total_count:,} articles\n")

                if total_count == 0:
                    print("WARNING: 0 articles returned. Possible causes:")
                    print("  1. API key missing or invalid (409/403)")
                    print("  2. Source IDs not matching any works")
                    print("  3. Network/proxy blocking api.openalex.org")
                    print("\nRerun with --debug to inspect the request and response.")

                    if args.debug:
                        print(f"\nDEBUG — Full response:\n{json.dumps(data, indent=2)[:2000]}")
                    sys.exit(1)

            results = data.get("results", [])
            if not results:
                break

            for work in results:
                row = extract_row(work)
                writer.writerow(row)
                rows_written += 1

            next_cursor = meta.get("next_cursor")

            pct = (rows_written / total_count * 100) if total_count else 0
            print(f"  Page {page_num}: +{len(results)} rows  "
                  f"(total: {rows_written:,} / {total_count:,}  {pct:.1f}%)")

            save_progress(cursor, rows_written)

            if args.debug:
                print("\nDEBUG — stopping after 1 page (--debug mode).")
                break

            if not next_cursor:
                break
            cursor = next_cursor
            page_num += 1

            time.sleep(0.1)

    # Clean up progress on success (unless debug)
    if not args.debug and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    # ----------------------------------------------------------------
    # Summary statistics
    # ----------------------------------------------------------------
    print(f"\nDone. {rows_written:,} articles written to {output_csv}")

    if rows_written == 0:
        print("No articles to summarize.")
        return

    import pandas as pd
    df = pd.read_csv(output_csv)

    stats_lines = [
        "=" * 60,
        "CATALOG SUMMARY",
        f"Clinical Psychology Articles (Top 10 Journals, {YEAR_START}–{YEAR_END})",
        f"Generated: {datetime.now().isoformat()}",
        "=" * 60,
        "",
        "Selection criteria:",
        f"  Journals:  Top 10 empirical Clinical Psychology journals",
        f"             by SJR (2024), Scimago category 3203",
        f"  Years:     {YEAR_START}–{YEAR_END}",
        f"  Type:      article (excludes editorials, errata, letters)",
        f"  Retracted: excluded",
        f"  XPAC:      excluded by default",
        "",
        "Source IDs used:",
    ]
    for sid, name in source_id_map.items():
        stats_lines.append(f"  {sid:<15s} {name}")

    stats_lines.extend([
        "",
        f"Total articles: {len(df):,}",
        "",
        "By year:",
        df["publication_year"].value_counts().sort_index().to_string(),
        "",
        "By journal:",
        df["journal_name"].value_counts().to_string(),
        "",
        "Open Access breakdown:",
        df["oa_status"].value_counts().to_string(),
        "",
        f"Articles with DOI: {df['doi'].notna().sum():,} "
        f"({df['doi'].notna().mean()*100:.1f}%)",
        "",
        "Top 10 publishers:",
        df["publisher"].value_counts().head(10).to_string(),
        "",
        "Citation statistics:",
        df["cited_by_count"].astype(float).describe().to_string(),
    ])

    stats_text = "\n".join(stats_lines)
    with open(STATS_FILE, "w") as f:
        f.write(stats_text)
    print(f"\nStats written to {STATS_FILE}")
    print(stats_text)


if __name__ == "__main__":
    main()
