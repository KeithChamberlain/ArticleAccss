#!/usr/bin/env python3
"""
sample_and_probe.py
===================
Draw a random sample of 1,000 articles from the population catalog,
then for each article query multiple sources to characterize availability.

Sources are controlled by the SOURCE_REGISTRY table near the top of this
file. Set "enabled" to True/False to include or exclude any source.

All probes collect metadata only — NO full text or PDF downloads.

Output: sample_results.csv — stacked table (one row per article × source)
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import random
from datetime import datetime
from urllib.parse import urlparse, quote

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INPUT_CSV = "articles.csv"
OUTPUT_CSV = "sample_results.csv"
PROGRESS_FILE = "probe_progress.json"
SAMPLE_SIZE = 1000
RANDOM_SEED = None  # Set via --seed CLI argument; None = no fixed seed
MAILTO = os.getenv("OPENALEX_MAILTO", "researcher@example.com")

# Rate-limit delays (seconds)
DELAY_CROSSREF = 0.05    # ~20 req/s with mailto (limit 50)
DELAY_UNPAYWALL = 0.1    # ~10 req/s (limit 100K/day)
DELAY_SEMANTIC = 1.0     # 100 req / 5 min = ~0.33/s; be conservative
DELAY_OPENALEX = 0.1     # polite pool
DELAY_PUBLISHER = 0.5    # be polite to publisher sites
DELAY_PUBMED = 0.34      # NCBI: max 3 req/s without API key
DELAY_EUROPEPMC = 0.2    # Europe PMC: no hard limit; be polite
DELAY_CORE = 0.5         # CORE free tier: ~5 req/s
DELAY_WAYBACK = 0.5      # Internet Archive: be polite
DELAY_SCIHUB = 1.0       # Sci-Hub: no published limits; be conservative

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": f"ArticleProbe/2.0 (mailto:{MAILTO})",
})

# ---------------------------------------------------------------------------
# SOURCE REGISTRY — enable / disable sources here
# ---------------------------------------------------------------------------
# Each entry maps a source key to its configuration.  Set "enabled" to
# True or False to control which sources are probed for every article.
#
# ┌─────────────────┬─────────┬────────────┬──────────────────────────────────┐
# │ key             │ enabled │ source_type│ what it checks                   │
# ├─────────────────┼─────────┼────────────┼──────────────────────────────────┤
# │ publisher_site  │  True   │ publisher  │ DOI → landing page redirect/HTTP │
# │ crossref        │  True   │ publisher  │ BibTeX, license, pub dates       │
# │ unpaywall       │  True   │ publisher  │ OA status, host type, best loc   │
# │ semantic_scholar│  True   │ other      │ Abstract, ext IDs, OA PDF flag   │
# │ openalex        │  True   │ varies     │ Locations, abstract, OA status   │
# │ pubmed          │  True   │ repository │ PMC deposit date, MeSH, PMID     │
# │ europe_pmc      │  True   │ repository │ Preprints, author MSS, full-text │
# │ core            │  False  │ repository │ Harvest/deposit date, full-text  │
# │ wayback_machine │  False  │ other      │ First web-archive snapshot date  │
# │ scihub          │  False  │ other      │ Shadow-library availability check│
# └─────────────────┴─────────┴────────────┴──────────────────────────────────┘
#
# To add a new source:
#   1. Add an entry to SOURCE_REGISTRY below
#   2. Write a probe_<key>() function that returns a row-dict or None
#   3. Register it in PROBE_FUNCTIONS further down

SOURCE_REGISTRY = {
    "publisher_site": {
        "enabled": True,
        "label": "Publisher Site",
        "source_type": "publisher",
        "description": "DOI → landing page redirect; HTTP status check",
        "api_endpoint": "https://doi.org/{DOI}",
        "delay": DELAY_PUBLISHER,
        "requires_doi": True,
        "requires_api_key": False,
    },
    "crossref": {
        "enabled": True,
        "label": "CrossRef",
        "source_type": "publisher",
        "description": "BibTeX via content-negotiation; license & date metadata",
        "api_endpoint": "https://api.crossref.org/works/{DOI}",
        "delay": DELAY_CROSSREF,
        "requires_doi": True,
        "requires_api_key": False,
    },
    "unpaywall": {
        "enabled": True,
        "label": "Unpaywall",
        "source_type": "publisher",
        "description": "OA status, host type, best OA location URL",
        "api_endpoint": "https://api.unpaywall.org/v2/{DOI}",
        "delay": DELAY_UNPAYWALL,
        "requires_doi": True,
        "requires_api_key": False,
    },
    "semantic_scholar": {
        "enabled": True,
        "label": "Semantic Scholar",
        "source_type": "other",
        "description": "Abstract, external IDs, open-access PDF flag",
        "api_endpoint": "https://api.semanticscholar.org/graph/v1/paper/DOI:{DOI}",
        "delay": DELAY_SEMANTIC,
        "requires_doi": False,   # can fall back to title search
        "requires_api_key": False,
    },
    "openalex": {
        "enabled": True,
        "label": "OpenAlex",
        "source_type": "varies",
        "description": "All hosting locations, inverted abstract, OA status, dates",
        "api_endpoint": "https://api.openalex.org/works/{ID}",
        "delay": DELAY_OPENALEX,
        "requires_doi": False,   # can use OpenAlex ID
        "requires_api_key": False,
    },
    "pubmed": {
        "enabled": True,
        "label": "PubMed / NCBI E-Utilities",
        "source_type": "repository",
        "description": "PMID lookup, PMC deposit date, MeSH terms, PubMed Central link",
        "api_endpoint": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
        "delay": DELAY_PUBMED,
        "requires_doi": True,
        "requires_api_key": False,  # optional NCBI API key speeds up
    },
    "europe_pmc": {
        "enabled": True,
        "label": "Europe PMC",
        "source_type": "repository",
        "description": "Parallel to PubMed; includes preprints and author manuscripts",
        "api_endpoint": "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
        "delay": DELAY_EUROPEPMC,
        "requires_doi": True,
        "requires_api_key": False,
    },
    "core": {
        "enabled": True,
        "label": "CORE",
        "source_type": "repository",
        "description": "Aggregated OA repository; harvest/deposit date",
        "api_endpoint": "https://api.core.ac.uk/v3/search/works",
        "delay": DELAY_CORE,
        "requires_doi": True,
        "requires_api_key": True,  # CORE v3 requires free API key
    },
    "wayback_machine": {
        "enabled": False,
        "label": "Wayback Machine (Internet Archive)",
        "source_type": "other",
        "description": "First web-archive snapshot date for the DOI landing page",
        "api_endpoint": "https://web.archive.org/cdx/search/cdx",
        "delay": DELAY_WAYBACK,
        "requires_doi": True,
        "requires_api_key": False,
    },
    "scihub": {
        "enabled": True,
        "label": "Sci-Hub",
        "source_type": "other",
        "description": "Shadow-library availability check (metadata only, no PDF download)",
        "api_endpoint": "{SCIHUB_MIRROR}/{DOI}",
        "delay": DELAY_SCIHUB,
        "requires_doi": True,
        "requires_api_key": False,
    },
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Sample articles and probe multiple sources for availability metadata.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible sampling. If omitted, sampling is non-deterministic.",
    )
    parser.add_argument(
        "--sample-size", "-n",
        type=int,
        default=SAMPLE_SIZE,
        help=f"Number of articles to sample (default: {SAMPLE_SIZE}).",
    )
    parser.add_argument(
        "--input", "-i",
        default=INPUT_CSV,
        help=f"Input CSV path (default: {INPUT_CSV}).",
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
        "--OPENALEX_API_KEY",
        default=os.getenv("OPENALEX_API_KEY", ""),
        help="OpenAlex API key (polite/premium pool). "
             "Falls back to OPENALEX_API_KEY env var if omitted.",
    )
    parser.add_argument(
        "--CORE_API_KEY",
        default=os.getenv("CORE_API_KEY", ""),
        help="CORE API v3 key (required to enable the CORE source). "
             "Falls back to CORE_API_KEY env var if omitted.",
    )
    parser.add_argument(
        "--SCIHUB_MIRROR",
        default=os.getenv("SCIHUB_MIRROR", ""),
        help="Sci-Hub mirror base URL (e.g. https://sci-hub.se). "
             "Required when the scihub source is enabled. "
             "Falls back to SCIHUB_MIRROR env var if omitted.",
    )
    return parser.parse_args()


def get_enabled_sources() -> list[str]:
    """Return list of enabled source keys in registry order."""
    return [k for k, v in SOURCE_REGISTRY.items() if v["enabled"]]


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------
FIELDNAMES = [
    "row_number",
    "article_id",
    "doi",
    "title",
    "num_sources",
    "source_name",
    "source_type",       # publisher / reprint / repository / preprint / other
    "access_type",       # open / free / pay
    "source_url",        # landing page or API URL (final destination)
    "publisher_domain",  # effective TLD+1 of final destination
    "http_status",       # final HTTP status code (200, 403, 404, …)
    "redirect_chain",    # pipe-separated domains traversed (doi.org|linkinghub.elsevier.com|sciencedirect.com)
    "bibtex_entry",
    "abstract",
    "date_first_available",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_get(url, headers=None, timeout=15, allow_redirects=True):
    """GET with retries + backoff. Returns response or None."""
    for attempt in range(4):
        try:
            resp = SESSION.get(
                url, headers=headers or {}, timeout=timeout,
                allow_redirects=allow_redirects,
            )
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"      429 on {urlparse(url).netloc} — waiting {wait}s")
                time.sleep(wait)
                continue
            return resp
        except requests.RequestException:
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# URL / domain classification infrastructure
# ---------------------------------------------------------------------------
#
# Real-world DOI redirect patterns this code handles:
#
#   Simple:         doi.org → wiley.com/doi/xxx                  (1 hop)
#   Multi-hop:      doi.org → linkinghub.elsevier.com → sciencedirect.com
#   Tracking:       doi.org → links.springernature.com → nature.com
#   Platform move:  doi.org → doi.apa.org → psycnet.apa.org
#   Small pub:      doi.org → journal.obscure-society.org        (unknown domain)
#   DOI→repo:       doi.org → repository.cam.ac.uk              (not a publisher)
#   DOI→preprint:   doi.org → psyarxiv.com/xxx
#   Paywall gate:   doi.org → publisher.com?needAccess=true      (403 = exists)
#   Meta-refresh:   doi.org → gateway (200 + <meta refresh>)     (hidden redirect)
#   Dead link:      doi.org → publisher.com/xxx                  (404/500)
#
# Key insight: the DOI resolver guarantees the chain ends at the content
# holder's canonical URL, so if a DOI resolves successfully the final
# destination IS the authoritative location — even if the domain is not
# in our pattern list.

# Domains known to be tracking/link-routing intermediaries that should
# be traversed, not treated as final destinations.
TRACKING_DOMAINS = {
    "links.springernature.com",
    "link.springer.com",
    "linkinghub.elsevier.com",
    "doi.apa.org",
    "dx.doi.org",
    "doi.org",
    "click.pstmrk.it",
    "redirect.cambridge.org",
    "gateway.webofknowledge.com",
    "click.email.taylorandfrancis.com",
}

# Domain → source_type classification.  Checked against the effective
# domain (netloc) — substring match, so "wiley.com" matches
# "onlinelibrary.wiley.com".  Order matters: first match wins.
# Organised from most specific to least specific.
DOMAIN_RULES: list[tuple[list[str], str]] = [
    # Repositories (curated OA archives)
    ([
        "ncbi.nlm.nih.gov", "europepmc.org", "pubmedcentral",
        "core.ac.uk", "hal.science", "hal.archives-ouvertes.fr",
        "zenodo.org", "dspace.", "eprints.", "ir.lib.",
        "repository.", "digitalcommons.", "opus.lib.",
    ], "repository"),

    # Preprint servers
    ([
        "psyarxiv.com", "medrxiv.org", "biorxiv.org", "arxiv.org",
        "ssrn.com", "osf.io", "preprints.org", "authorea.com",
        "techrxiv.org", "socarxiv.org", "edarxiv.org",
    ], "preprint"),

    # Self-archive / reprint hosts
    ([
        "researchgate.net", "academia.edu",
    ], "reprint"),

    # Known publishers (explicit match confirms publisher classification)
    ([
        "wiley.com", "onlinelibrary.wiley.com",
        "springer.com", "springerlink.com", "nature.com",
        "elsevier.com", "sciencedirect.com",
        "sagepub.com", "journals.sagepub.com",
        "tandfonline.com",
        "apa.org", "psycnet.apa.org",
        "oxfordacademic.com", "academic.oup.com",
        "cambridge.org",
        "frontiersin.org",
        "mdpi.com",
        "plos.org", "journals.plos.org",
        "bmj.com",
        "jamanetwork.com", "jama.jamanetwork.com",
        "thelancet.com",
        "cell.com",
        "journals.lww.com",
        "karger.com",
        "benthamscience.com",
        "degruyter.com",
        "liebertpub.com",
        "thieme-connect.com",
        "ieeexplore.ieee.org",
        "dl.acm.org",
        "taylorfrancis.com",
        "wolterskluwer.com",
        "hogrefe.com",
        "guilfordjournals.com",
    ], "publisher"),
]


def extract_domain(url: str) -> str:
    """Return the lower-cased netloc (hostname:port) from a URL."""
    if not url:
        return ""
    return urlparse(url).netloc.lower()


def normalize_url(url: str) -> str:
    """
    Normalize a URL for comparison:
      - force https
      - strip known tracking query params (utm_*, fbclid, etc.)
      - strip trailing slashes
    """
    if not url:
        return ""
    parsed = urlparse(url)

    # Force https
    scheme = "https"

    # Strip tracking params
    tracking_prefixes = ("utm_", "fbclid", "mc_cid", "mc_eid", "ref", "source")
    if parsed.query:
        from urllib.parse import parse_qs, urlencode
        qs = parse_qs(parsed.query, keep_blank_values=True)
        cleaned = {k: v for k, v in qs.items()
                   if not any(k.lower().startswith(p) for p in tracking_prefixes)}
        query = urlencode(cleaned, doseq=True)
    else:
        query = ""

    path = parsed.path.rstrip("/") or "/"
    netloc = parsed.netloc.lower()

    from urllib.parse import urlunparse
    return urlunparse((scheme, netloc, path, parsed.params, query, ""))


def classify_domain(domain: str) -> str | None:
    """
    Classify a single domain string against known patterns.
    Returns source_type or None if no rule matches.
    """
    if not domain:
        return None
    domain = domain.lower()
    for patterns, source_type in DOMAIN_RULES:
        for pattern in patterns:
            if pattern in domain:
                return source_type
    return None


def classify_host(url: str, redirect_chain: list[str] | None = None,
                  via_doi: bool = False) -> str:
    """
    Classify a URL (and optionally its redirect chain) into a source_type.

    Strategy:
      1. Check the FINAL url's domain against known patterns.
      2. If no match, scan the redirect chain for any recognized domain
         (sometimes the final domain is a CDN or vanity domain, but an
         intermediate hop reveals the real publisher).
      3. If the URL was reached by following a DOI and no pattern matched
         anywhere in the chain, it's still the content holder's chosen
         location — classify as "publisher" with high confidence because
         the DOI system is authoritative.
      4. If not via DOI and nothing matched, return "other" rather than
         guessing "publisher".
    """
    # 1. Classify the final URL
    final_domain = extract_domain(url)
    result = classify_domain(final_domain)
    if result:
        return result

    # 2. Scan redirect chain for clues
    if redirect_chain:
        for chain_domain in reversed(redirect_chain):
            # Skip tracking intermediaries
            if chain_domain in TRACKING_DOMAINS:
                continue
            result = classify_domain(chain_domain)
            if result:
                return result

    # 3. DOI-resolved URLs are authoritative: the DOI system guarantees
    #    this is where the content holder wants you to land.
    if via_doi:
        return "publisher"

    # 4. No evidence — be honest rather than guess
    return "other"


# ---------------------------------------------------------------------------
# Redirect chain follower
# ---------------------------------------------------------------------------

def follow_redirect_chain(start_url: str, max_hops: int = 10, timeout: int = 15):
    """
    Manually follow HTTP redirects from start_url, recording every hop.

    Returns a RedirectResult with:
      - final_url:       the last URL in the chain
      - final_status:    HTTP status of the final response
      - chain:           list of (url, status_code) tuples for every hop
      - chain_domains:   list of unique domains traversed (in order)
      - meta_refresh_url: URL from <meta http-equiv="refresh"> if detected
      - error:           error string if the chain broke, else None

    Handles:
      - 301, 302, 303, 307, 308 redirects
      - Meta-refresh redirects in HTML (gateway pages)
      - 403/451 as "article exists, paywalled" (not an error)
      - 429 with backoff
      - Timeouts and connection errors

    No full text or PDFs are downloaded.  For HTML responses that might
    contain a meta-refresh, only the first 4 KB is examined.
    """
    chain = []
    seen_urls = set()
    current_url = start_url

    for hop in range(max_hops):
        if current_url in seen_urls:
            # Redirect loop detected
            break
        seen_urls.add(current_url)

        resp = None
        for attempt in range(3):
            try:
                resp = SESSION.get(
                    current_url,
                    allow_redirects=False,
                    timeout=timeout,
                    stream=True,     # don't download body unless we need it
                )
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    time.sleep(wait)
                    resp.close()
                    resp = None
                    continue
                break
            except requests.RequestException:
                time.sleep(2 ** attempt)

        if resp is None:
            # All retries failed
            return _redirect_result(chain, error=f"Connection failed at hop {hop}")

        status = resp.status_code
        domain = extract_domain(current_url)
        chain.append((current_url, status, domain))

        # --- HTTP redirect (3xx with Location header) ---
        if status in (301, 302, 303, 307, 308):
            location = resp.headers.get("Location", "")
            resp.close()
            if not location:
                return _redirect_result(chain, error=f"Redirect {status} with no Location header")
            # Handle relative redirects
            if location.startswith("/"):
                parsed = urlparse(current_url)
                location = f"{parsed.scheme}://{parsed.netloc}{location}"
            elif not location.startswith("http"):
                # Relative path
                base = current_url.rsplit("/", 1)[0]
                location = f"{base}/{location}"
            current_url = location
            continue

        # --- Non-redirect status: check for meta-refresh ---
        # 200, 403, 404, etc. — we've "landed"
        meta_refresh_url = None
        if status == 200:
            # Read only the first 4 KB to check for meta-refresh
            # (avoids downloading full article pages)
            content_type = resp.headers.get("Content-Type", "")
            if "html" in content_type.lower():
                try:
                    head_bytes = resp.raw.read(4096)
                    head_text = head_bytes.decode("utf-8", errors="replace")
                    meta_refresh_url = _extract_meta_refresh(head_text, current_url)
                except Exception:
                    pass
        resp.close()

        # If meta-refresh found and it's a different URL, follow it
        if meta_refresh_url and meta_refresh_url != current_url and meta_refresh_url not in seen_urls:
            chain[-1] = (current_url, status, domain)  # update with meta note
            current_url = meta_refresh_url
            continue

        # We've reached the final destination
        return _redirect_result(chain, meta_refresh_url=meta_refresh_url)

    # Exceeded max hops
    return _redirect_result(chain, error=f"Exceeded {max_hops} redirect hops")


def _extract_meta_refresh(html_head: str, base_url: str) -> str | None:
    """
    Extract the target URL from a <meta http-equiv="refresh"> tag.

    Example:
      <meta http-equiv="refresh" content="0; url=https://publisher.com/article/123">

    Returns the absolute URL or None.
    """
    # Pattern: content="N; url=..." (case-insensitive)
    match = re.search(
        r'<meta[^>]+http-equiv\s*=\s*["\']?refresh["\']?[^>]+content\s*=\s*["\']?\d+\s*;\s*url\s*=\s*([^"\'\s>]+)',
        html_head,
        re.IGNORECASE,
    )
    if not match:
        # Try alternate attribute order: content before http-equiv
        match = re.search(
            r'<meta[^>]+content\s*=\s*["\']?\d+\s*;\s*url\s*=\s*([^"\'\s>]+)[^>]+http-equiv\s*=\s*["\']?refresh',
            html_head,
            re.IGNORECASE,
        )
    if not match:
        return None

    target = match.group(1).rstrip("\"'>;")
    if not target:
        return None

    # Make absolute
    if target.startswith("/"):
        parsed = urlparse(base_url)
        target = f"{parsed.scheme}://{parsed.netloc}{target}"
    elif not target.startswith("http"):
        base = base_url.rsplit("/", 1)[0]
        target = f"{base}/{target}"

    return target


def _redirect_result(chain: list[tuple], error: str | None = None,
                     meta_refresh_url: str | None = None) -> dict:
    """Package redirect chain into a result dict."""
    if chain:
        final_url, final_status, final_domain = chain[-1]
    else:
        final_url, final_status, final_domain = ("", 0, "")

    # Unique domains in traversal order (deduplicated, preserving order)
    seen = set()
    chain_domains = []
    for url, status, domain in chain:
        if domain and domain not in seen:
            seen.add(domain)
            chain_domains.append(domain)

    return {
        "final_url": final_url,
        "final_status": final_status,
        "final_domain": final_domain,
        "chain": chain,
        "chain_domains": chain_domains,
        "meta_refresh_url": meta_refresh_url,
        "error": error,
    }


# ---------------------------------------------------------------------------
# Access classification and other helpers
# ---------------------------------------------------------------------------

def classify_access(oa_status: str, license_str: str = "", is_oa: bool = False) -> str:
    """Map OA metadata into open / free / pay."""
    oa_status = (oa_status or "").lower()
    if oa_status in ("gold", "green", "diamond"):
        return "open"
    if oa_status in ("hybrid",) and is_oa:
        return "open"
    if oa_status == "bronze":
        return "free"
    if is_oa:
        return "open"
    if license_str and "cc" in license_str.lower():
        return "open"
    return "pay"


def decode_inverted_abstract(inv: dict) -> str:
    """OpenAlex stores abstracts as inverted indexes — reconstruct text."""
    if not inv:
        return ""
    word_positions = []
    for word, positions in inv.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort()
    return " ".join(w for _, w in word_positions)


def sanitize(text: str) -> str:
    """Remove newlines / excess whitespace for CSV safety."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


# ===========================================================================
# PROBE FUNCTIONS — one per source
# ===========================================================================
# Each probe function signature:
#   probe_<key>(doi, title, openalex_id, **kwargs) -> dict | None
#
# All must accept **kwargs for forward-compatibility.  Return a dict with
# keys matching the non-article FIELDNAMES, or None if the source had
# nothing for this article.
# ===========================================================================

def probe_publisher_site(doi: str, title: str = "", openalex_id: str = "", **kw) -> dict | None:
    """
    Resolve DOI to publisher landing page by walking the full redirect
    chain.  Records final URL, all intermediate domains, HTTP status,
    and uses chain context for accurate source_type classification.

    Handles:
      - Multi-hop redirects (linkinghub → sciencedirect, etc.)
      - Tracking/routing intermediaries (links.springernature.com, etc.)
      - Meta-refresh redirects (gateway pages with <meta refresh>)
      - 403 = paywall (article exists), 404 = dead link
      - DOIs that resolve to repositories or preprint servers

    NO full text is downloaded.  HTML is read only up to 4 KB for
    meta-refresh detection.
    """
    if not doi:
        return None

    start_url = f"https://doi.org/{doi}"

    result = follow_redirect_chain(start_url, max_hops=10, timeout=15)
    time.sleep(DELAY_PUBLISHER)

    if result["error"] and not result["chain"]:
        # Total failure — no hops at all
        return None

    final_url = normalize_url(result["final_url"])
    final_status = result["final_status"]
    chain_domains = result["chain_domains"]
    final_domain = result["final_domain"]

    # --- Classify source_type using the full chain context ---
    # The DOI system is authoritative: if a DOI resolves, the final
    # destination IS where the content holder wants you.  But it might
    # be a repository or preprint server, not a traditional publisher.
    source_type = classify_host(
        final_url,
        redirect_chain=chain_domains,
        via_doi=True,
    )

    # --- Determine access_type from HTTP status ---
    # 200        → page loaded (access unknown without Unpaywall; default "pay")
    # 403 / 451  → paywall or legal block (article exists, access denied)
    # 404 / 410  → content removed or broken DOI
    # 5xx        → server error (transient)
    if final_status in (403, 451):
        access_type = "pay"
    elif final_status in (404, 410):
        access_type = "pay"  # can't confirm; default
    else:
        access_type = "pay"  # default; updated by merge step with Unpaywall/CrossRef

    # --- Build pipe-separated domain chain for the CSV ---
    # e.g. "doi.org|linkinghub.elsevier.com|sciencedirect.com"
    chain_str = "|".join(chain_domains)

    # Log redirect details for transparency
    if len(chain_domains) > 2:
        print(f"      chain: {' → '.join(chain_domains)} ({final_status})")
    elif result["error"]:
        print(f"      chain error: {result['error']}")

    return {
        "source_name": "publisher_site",
        "source_type": source_type,
        "access_type": access_type,
        "source_url": final_url,
        "publisher_domain": final_domain,
        "http_status": str(final_status),
        "redirect_chain": chain_str,
        "bibtex_entry": "",
        "abstract": "",
        "date_first_available": "",
    }


def probe_crossref(doi: str, title: str = "", openalex_id: str = "", **kw) -> dict | None:
    """CrossRef metadata + BibTeX via content-negotiation."""
    if not doi:
        return None

    # --- Metadata ---
    meta_url = f"https://api.crossref.org/works/{doi}"
    resp = safe_get(meta_url, headers={"Accept": "application/json"})
    time.sleep(DELAY_CROSSREF)

    meta = {}
    access = "pay"
    date_available = ""
    if resp and resp.status_code == 200:
        try:
            data = resp.json().get("message", {})
            meta = data
            licenses = data.get("license", [])
            for lic in licenses:
                url_l = (lic.get("URL") or "").lower()
                if "creativecommons" in url_l or "/cc" in url_l:
                    access = "open"
                    break

            online = data.get("published-online", {}).get("date-parts", [[]])[0]
            created = data.get("created", {}).get("date-parts", [[]])[0]
            if online and len(online) >= 3:
                date_available = f"{online[0]:04d}-{online[1]:02d}-{online[2]:02d}"
            elif created and len(created) >= 3:
                date_available = f"{created[0]:04d}-{created[1]:02d}-{created[2]:02d}"
        except (json.JSONDecodeError, KeyError, IndexError):
            pass

    # --- BibTeX via content negotiation ---
    bibtex = ""
    bibtex_url = f"https://doi.org/{doi}"
    bib_resp = safe_get(bibtex_url, headers={"Accept": "application/x-bibtex"})
    time.sleep(DELAY_CROSSREF)
    if bib_resp and bib_resp.status_code == 200:
        ct = bib_resp.headers.get("Content-Type", "")
        if "bibtex" in ct or bib_resp.text.strip().startswith("@"):
            bibtex = sanitize(bib_resp.text)

    return {
        "source_name": "crossref",
        "source_type": "publisher",
        "access_type": access,
        "source_url": f"https://api.crossref.org/works/{doi}",
        "publisher_domain": "",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": bibtex,
        "abstract": sanitize(meta.get("abstract", "")),
        "date_first_available": date_available,
    }


def probe_unpaywall(doi: str, title: str = "", openalex_id: str = "", **kw) -> dict | None:
    """Unpaywall OA status and locations."""
    if not doi:
        return None

    url = f"https://api.unpaywall.org/v2/{doi}?email={MAILTO}"
    resp = safe_get(url)
    time.sleep(DELAY_UNPAYWALL)

    if resp is None or resp.status_code != 200:
        return None

    try:
        data = resp.json()
    except json.JSONDecodeError:
        return None

    oa_status = data.get("oa_status", "closed")
    is_oa = data.get("is_oa", False)
    best_loc = data.get("best_oa_location") or {}

    host_type = best_loc.get("host_type", "")
    if host_type == "publisher":
        stype = "publisher"
    elif host_type == "repository":
        stype = "repository"
    else:
        stype = "other"

    source_url = best_loc.get("url_for_landing_page", "") or best_loc.get("url", "")
    license_str = best_loc.get("license", "") or ""
    access = classify_access(oa_status, license_str, is_oa)
    date_avail = data.get("published_date", "") or ""

    return {
        "source_name": "unpaywall",
        "source_type": stype,
        "access_type": access,
        "source_url": source_url,
        "publisher_domain": urlparse(source_url).netloc if source_url else "",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": "",
        "date_first_available": date_avail,
    }


def probe_semantic_scholar(doi: str, title: str = "", openalex_id: str = "", **kw) -> dict | None:
    """Semantic Scholar abstract, external IDs, and OA flag."""
    if doi:
        url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}"
    elif title:
        url = "https://api.semanticscholar.org/graph/v1/paper/search"
        url += f"?query={quote(title[:200])}&limit=1"
    else:
        return None

    fields = "title,abstract,externalIds,isOpenAccess,openAccessPdf,publicationDate,url"
    if "search" not in url:
        url += f"?fields={fields}"
    else:
        url += f"&fields={fields}"

    resp = safe_get(url)
    time.sleep(DELAY_SEMANTIC)

    if resp is None or resp.status_code != 200:
        return None

    try:
        data = resp.json()
        if "data" in data and isinstance(data["data"], list) and data["data"]:
            data = data["data"][0]
    except (json.JSONDecodeError, KeyError, IndexError):
        return None

    is_oa = data.get("isOpenAccess", False)
    oa_pdf = data.get("openAccessPdf") or {}
    s2_url = data.get("url", "")

    if is_oa and oa_pdf:
        access = "open"
        stype = classify_host(oa_pdf.get("url", ""))
    elif is_oa:
        access = "open"
        stype = "other"
    else:
        access = "pay"
        stype = "other"

    return {
        "source_name": "semantic_scholar",
        "source_type": stype,
        "access_type": access,
        "source_url": s2_url,
        "publisher_domain": "",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": sanitize(data.get("abstract", "")),
        "date_first_available": data.get("publicationDate", ""),
    }


def probe_openalex(doi: str = "", title: str = "", openalex_id: str = "",
                   openalex_api_key: str = "", **kw) -> dict | None:
    """OpenAlex hosting locations, OA details, and inverted abstract."""
    if openalex_id:
        oa_id = openalex_id.split("/")[-1] if "/" in openalex_id else openalex_id
        url = f"https://api.openalex.org/works/{oa_id}?mailto={MAILTO}"
    elif doi:
        url = f"https://api.openalex.org/works/doi:{doi}?mailto={MAILTO}"
    else:
        return None

    # Append API key for premium/polite pool access when provided
    if openalex_api_key:
        url += f"&api_key={openalex_api_key}"

    resp = safe_get(url)
    time.sleep(DELAY_OPENALEX)

    if resp is None or resp.status_code != 200:
        return None

    try:
        data = resp.json()
    except json.JSONDecodeError:
        return None

    oa = data.get("open_access") or {}
    is_oa = oa.get("is_oa", False)
    oa_status = oa.get("oa_status", "closed")
    oa_url = oa.get("oa_url", "")

    access = classify_access(oa_status, "", is_oa)
    source_url = oa_url or (data.get("primary_location") or {}).get("landing_page_url", "")
    stype = classify_host(source_url)

    abstract = decode_inverted_abstract(data.get("abstract_inverted_index"))

    pub_date = data.get("publication_date", "")
    created = data.get("created_date", "")
    date_avail = pub_date or created

    return {
        "source_name": "openalex",
        "source_type": stype,
        "access_type": access,
        "source_url": source_url,
        "publisher_domain": urlparse(source_url).netloc if source_url else "",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": sanitize(abstract),
        "date_first_available": date_avail,
    }


# ---------------------------------------------------------------------------
# NEW PROBES — added from research plan
# ---------------------------------------------------------------------------

def probe_pubmed(doi: str, title: str = "", openalex_id: str = "", **kw) -> dict | None:
    """
    Query NCBI E-Utilities for PubMed record and PMC deposit date.
    Uses esearch (DOI → PMID) then esummary for metadata.
    No full text downloaded.
    """
    if not doi:
        return None

    ncbi_key = os.getenv("NCBI_API_KEY", "")

    # Step 1: esearch — find PMID by DOI
    esearch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "pubmed",
        "term": f"{doi}[DOI]",
        "retmode": "json",
        "retmax": 1,
    }
    if ncbi_key:
        params["api_key"] = ncbi_key

    resp = safe_get(f"{esearch_url}?{'&'.join(f'{k}={v}' for k,v in params.items())}")
    time.sleep(DELAY_PUBMED)

    if resp is None or resp.status_code != 200:
        return None

    try:
        esearch = resp.json()
        id_list = esearch.get("esearchresult", {}).get("idlist", [])
    except (json.JSONDecodeError, KeyError):
        return None

    if not id_list:
        return None

    pmid = id_list[0]

    # Step 2: esummary — get metadata
    esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params2 = {
        "db": "pubmed",
        "id": pmid,
        "retmode": "json",
    }
    if ncbi_key:
        params2["api_key"] = ncbi_key

    resp2 = safe_get(f"{esummary_url}?{'&'.join(f'{k}={v}' for k,v in params2.items())}")
    time.sleep(DELAY_PUBMED)

    if resp2 is None or resp2.status_code != 200:
        return None

    try:
        summary = resp2.json()
        record = summary.get("result", {}).get(pmid, {})
    except (json.JSONDecodeError, KeyError):
        return None

    # Check for PMC ID (indicates deposit in PubMed Central)
    pmc_id = ""
    article_ids = record.get("articleids", [])
    for aid in article_ids:
        if aid.get("idtype") == "pmc":
            pmc_id = aid.get("value", "")
            break

    # Determine access type
    if pmc_id:
        access = "open"
        stype = "repository"
        source_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/"
    else:
        access = "pay"
        stype = "repository"
        source_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

    # Dates — PubMed "sortpubdate" or "epubdate"
    epub_date = record.get("epubdate", "")
    sort_date = record.get("sortpubdate", "")
    # sortpubdate format: "2021/03/15 00:00"
    date_avail = ""
    for d in [epub_date, sort_date]:
        if d:
            # Normalize to ISO 8601
            date_avail = d.replace("/", "-").split(" ")[0]
            break

    return {
        "source_name": "pubmed",
        "source_type": stype,
        "access_type": access,
        "source_url": source_url,
        "publisher_domain": "ncbi.nlm.nih.gov",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": "",  # esummary doesn't include abstract; could use efetch
        "date_first_available": date_avail,
    }


def probe_europe_pmc(doi: str, title: str = "", openalex_id: str = "", **kw) -> dict | None:
    """
    Query Europe PMC REST API for article metadata.
    Europe PMC includes PubMed, PMC, preprints, and author manuscripts.
    No full text downloaded.
    """
    if not doi:
        return None

    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    params = {
        "query": f'DOI:"{doi}"',
        "format": "json",
        "resultType": "core",  # includes abstract
        "pageSize": 1,
    }

    resp = safe_get(f"{url}?{'&'.join(f'{k}={quote(str(v))}' for k,v in params.items())}")
    time.sleep(DELAY_EUROPEPMC)

    if resp is None or resp.status_code != 200:
        return None

    try:
        data = resp.json()
        results = data.get("resultList", {}).get("result", [])
    except (json.JSONDecodeError, KeyError):
        return None

    if not results:
        return None

    rec = results[0]

    # Determine source type and access
    is_oa = rec.get("isOpenAccess") == "Y"
    in_pmc = bool(rec.get("pmcid"))
    source_type_raw = rec.get("source", "")  # "MED", "PMC", "PPR" (preprint)

    if source_type_raw == "PPR":
        stype = "preprint"
        access = "open"
    elif in_pmc:
        stype = "repository"
        access = "open"
    elif is_oa:
        stype = "publisher"
        access = "open"
    else:
        stype = "publisher"
        access = "pay"

    # Build URL
    pmcid = rec.get("pmcid", "")
    pmid = rec.get("pmid", "")
    if pmcid:
        source_url = f"https://europepmc.org/article/PMC/{pmcid}"
    elif pmid:
        source_url = f"https://europepmc.org/article/MED/{pmid}"
    else:
        source_url = f"https://europepmc.org/search?query=DOI:{doi}"

    # Dates
    date_avail = ""
    first_pub = rec.get("firstPublicationDate", "")
    first_index = rec.get("firstIndexDate", "")
    date_avail = first_pub or first_index

    return {
        "source_name": "europe_pmc",
        "source_type": stype,
        "access_type": access,
        "source_url": source_url,
        "publisher_domain": "europepmc.org",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": sanitize(rec.get("abstractText", "")),
        "date_first_available": date_avail,
    }


def probe_core(doi: str, title: str = "", openalex_id: str = "",
               core_api_key: str = "", **kw) -> dict | None:
    """
    Query CORE API v3 for OA repository records and deposit dates.
    Requires a free API key (pass via --CORE_API_KEY or set CORE_API_KEY env var).
    No full text downloaded.
    """
    # CLI key takes precedence; fall back to env var
    core_key = core_api_key or os.getenv("CORE_API_KEY", "")
    if not core_key:
        return None  # CORE v3 requires an API key

    if not doi:
        return None

    url = "https://api.core.ac.uk/v3/search/works"
    headers = {"Authorization": f"Bearer {core_key}"}
    params = {
        "q": f'doi:"{doi}"',
        "limit": 1,
    }

    resp = safe_get(
        f"{url}?{'&'.join(f'{k}={quote(str(v))}' for k,v in params.items())}",
        headers=headers,
    )
    time.sleep(DELAY_CORE)

    if resp is None or resp.status_code != 200:
        return None

    try:
        data = resp.json()
        results = data.get("results", [])
    except (json.JSONDecodeError, KeyError):
        return None

    if not results:
        return None

    rec = results[0]

    # depositedDate is when CORE first harvested the record
    deposited = rec.get("depositedDate", "")
    # publishedDate from the source
    published = rec.get("publishedDate", "")
    date_avail = deposited or published

    # Access: CORE only indexes OA content, so if it's here it's open
    access = "open"
    source_url = rec.get("downloadUrl", "") or rec.get("sourceFulltextUrls", [""])[0] if rec.get("sourceFulltextUrls") else ""
    if not source_url:
        core_id = rec.get("id", "")
        source_url = f"https://core.ac.uk/works/{core_id}" if core_id else ""

    return {
        "source_name": "core",
        "source_type": "repository",
        "access_type": access,
        "source_url": source_url,
        "publisher_domain": "core.ac.uk",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": sanitize(rec.get("abstract", "")),
        "date_first_available": date_avail,
    }


def probe_wayback_machine(doi: str, title: str = "", openalex_id: str = "", **kw) -> dict | None:
    """
    Query the Wayback Machine CDX API for the first archived snapshot
    of the article's DOI landing page.  Returns the earliest capture
    date — useful as a lower bound for "when was this URL publicly
    accessible?"  No full text downloaded.
    """
    if not doi:
        return None

    target_url = f"https://doi.org/{doi}"
    cdx_url = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url": target_url,
        "output": "json",
        "limit": 1,
        "fl": "timestamp,statuscode,original",
        "sort": "asc",        # earliest snapshot first
        "filter": "statuscode:200",
    }

    resp = safe_get(f"{cdx_url}?{'&'.join(f'{k}={quote(str(v))}' for k,v in params.items())}")
    time.sleep(DELAY_WAYBACK)

    if resp is None or resp.status_code != 200:
        return None

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return None

    # CDX returns [ [header_row], [data_row], ... ]
    if not data or len(data) < 2:
        return None

    row = data[1]  # first data row
    timestamp = str(row[0])  # format: "20210315120000"

    # Parse Wayback timestamp → ISO date
    date_avail = ""
    if len(timestamp) >= 8:
        date_avail = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"

    wayback_url = f"https://web.archive.org/web/{timestamp}/{target_url}"

    return {
        "source_name": "wayback_machine",
        "source_type": "other",
        "access_type": "free",  # Wayback snapshots are freely accessible
        "source_url": wayback_url,
        "publisher_domain": "web.archive.org",
        "http_status": "",
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": "",
        "date_first_available": date_avail,
    }


def probe_scihub(doi: str, title: str = "", openalex_id: str = "",
                 scihub_mirror: str = "", **kw) -> dict | None:
    """
    Check whether a DOI is available on a Sci-Hub mirror.

    This is a metadata-only availability probe for bibliometric research
    (see e.g. Himmelstein et al. 2018, eLife; Bohannon 2016, Science).
    NO full text or PDFs are downloaded.  Only the landing-page HTML is
    inspected (first 4 KB) to detect whether an embedded PDF viewer is
    present, which indicates the article is in Sci-Hub's corpus.

    The mirror URL can be supplied via --SCIHUB_MIRROR on the command line
    or the SCIHUB_MIRROR environment variable.  If neither is set, the
    probe is skipped with a warning.

    Returns availability status, HTTP status code, and the resolved URL.
    """
    if not doi:
        return None

    # CLI value takes precedence; fall back to env var
    mirror = (scihub_mirror or os.getenv("SCIHUB_MIRROR", "")).rstrip("/")
    if not mirror:
        print("      WARNING: SCIHUB_MIRROR not set — skipping.")
        print("      Pass --SCIHUB_MIRROR=https://sci-hub.se or set the env var.")
        return None

    url = f"{mirror}/{doi}"
    available = False
    final_url = url
    http_status = 0

    try:
        resp = SESSION.get(
            url,
            allow_redirects=True,
            timeout=20,
            stream=True,
        )
        http_status = resp.status_code
        final_url = resp.url  # after any redirects

        if resp.status_code == 200:
            # Read only the first 4 KB to check for the PDF embed marker.
            # Sci-Hub pages embed the PDF in an <iframe> or <embed> tag
            # when the article is in their corpus.  An error page or
            # captcha page will lack these markers.
            content_type = resp.headers.get("Content-Type", "")
            if "html" in content_type.lower():
                try:
                    head_bytes = resp.raw.read(4096)
                    head_text = head_bytes.decode("utf-8", errors="replace").lower()
                    # Markers that indicate a PDF is being served:
                    #   <iframe>  — main delivery method
                    #   <embed>   — alternate embed
                    #   #pdf      — anchor link to PDF viewer
                    #   .pdf      — direct PDF URL in page
                    if any(marker in head_text for marker in
                           ("<iframe", "<embed", "#pdf", ".pdf", "sci-hub")):
                        available = True
                except Exception:
                    pass
            elif "pdf" in content_type.lower():
                # Some mirrors redirect directly to the PDF stream;
                # the Content-Type header alone confirms availability.
                available = True

        resp.close()

    except requests.RequestException as e:
        print(f"      Sci-Hub request error: {e}")
        return None

    time.sleep(DELAY_SCIHUB)

    if not available:
        # Article not in corpus — still a valid data point
        return {
            "source_name": "scihub",
            "source_type": "other",
            "access_type": "pay",  # not available here
            "source_url": "",
            "publisher_domain": extract_domain(mirror),
            "http_status": str(http_status),
            "redirect_chain": "",
            "bibtex_entry": "",
            "abstract": "",
            "date_first_available": "",
        }

    return {
        "source_name": "scihub",
        "source_type": "other",
        "access_type": "free",   # accessible without payment, but no OA license
        "source_url": final_url,
        "publisher_domain": extract_domain(final_url),
        "http_status": str(http_status),
        "redirect_chain": "",
        "bibtex_entry": "",
        "abstract": "",
        "date_first_available": "",  # Sci-Hub doesn't expose upload dates
    }


# ---------------------------------------------------------------------------
# Probe function registry — maps source key → callable
# ---------------------------------------------------------------------------
# To add a new source, write probe_<key>() above, then add it here.

PROBE_FUNCTIONS = {
    "publisher_site":   probe_publisher_site,
    "crossref":         probe_crossref,
    "unpaywall":        probe_unpaywall,
    "semantic_scholar":  probe_semantic_scholar,
    "openalex":         probe_openalex,
    "pubmed":           probe_pubmed,
    "europe_pmc":       probe_europe_pmc,
    "core":             probe_core,
    "wayback_machine":  probe_wayback_machine,
    "scihub":           probe_scihub,
}


# ---------------------------------------------------------------------------
# Merge BibTeX + access across sources
# ---------------------------------------------------------------------------

def merge_access_into_publisher(rows: list[dict]):
    """
    If we got a more specific access classification from Unpaywall or CrossRef,
    propagate it to the publisher_site row (which defaults to 'pay').
    Also propagate the BibTeX from CrossRef to all rows that lack it.
    """
    bibtex = ""
    best_access = "pay"
    abstract = ""

    for r in rows:
        if r["bibtex_entry"]:
            bibtex = r["bibtex_entry"]
        if r["access_type"] == "open":
            best_access = "open"
        elif r["access_type"] == "free" and best_access == "pay":
            best_access = "free"
        if r["abstract"] and len(r["abstract"]) > len(abstract):
            abstract = r["abstract"]

    for r in rows:
        if not r["bibtex_entry"] and bibtex:
            r["bibtex_entry"] = bibtex
        if not r["abstract"] and abstract:
            r["abstract"] = abstract
        if r["source_name"] == "publisher_site" and best_access != "pay":
            r["access_type"] = best_access


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_progress_probe() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"last_completed_index": -1}


def save_progress_probe(idx: int):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_completed_index": idx}, f)


def print_source_table():
    """Print the source registry as a readable table at startup."""
    enabled = get_enabled_sources()
    print(f"\n{'#':<3s} {'Source':<22s} {'Enabled':<9s} {'Type':<12s} Description")
    print("-" * 90)
    for i, (key, cfg) in enumerate(SOURCE_REGISTRY.items(), 1):
        check = "  ✓" if cfg["enabled"] else "  ✗"
        print(f"{i:<3d} {cfg['label']:<22s} {check:<9s} {cfg['source_type']:<12s} "
              f"{cfg['description']}")
    print(f"\n{len(enabled)} of {len(SOURCE_REGISTRY)} sources enabled.\n")


def main():
    args = parse_args()

    print("=" * 70)
    print("Article Source Probe — Clinical Psychology Sample")
    print("=" * 70)

    # ---- Show source table ----
    print_source_table()
    enabled_keys = get_enabled_sources()

    # ---- Validate probe functions exist for all enabled sources ----
    for key in enabled_keys:
        if key not in PROBE_FUNCTIONS:
            print(f"ERROR: No probe function registered for enabled source '{key}'")
            sys.exit(1)

    # ---- Load catalog ----
    input_csv = args.input
    output_csv = args.output
    sample_size = args.sample_size
    seed = args.seed
    openalex_api_key = args.OPENALEX_API_KEY
    core_api_key = args.CORE_API_KEY
    scihub_mirror = args.SCIHUB_MIRROR

    # ---- Report API key status ----
    if openalex_api_key:
        print(f"OpenAlex API key: provided ({len(openalex_api_key)} chars)")
    else:
        print("OpenAlex API key: not set (using polite pool with mailto)")
    if core_api_key:
        print(f"CORE API key:     provided ({len(core_api_key)} chars)")
    else:
        print("CORE API key:     not set (CORE source will be skipped if enabled)")
    if scihub_mirror:
        print(f"Sci-Hub mirror:   {scihub_mirror}")
    else:
        print("Sci-Hub mirror:   not set (scihub source will be skipped if enabled)")

    if not os.path.exists(input_csv):
        print(f"ERROR: {input_csv} not found. Run catalog_population.py first.")
        sys.exit(1)

    df = pd.read_csv(input_csv, dtype=str).fillna("")
    print(f"Loaded {len(df):,} articles from {input_csv}")

    # ---- Draw sample ----
    n = min(sample_size, len(df))
    sample = df.sample(n=n, random_state=seed).reset_index(drop=True)
    seed_label = str(seed) if seed is not None else "none (non-deterministic)"
    print(f"Random sample: {n} articles (seed={seed_label})\n")

    # ---- Resume support ----
    progress = load_progress_probe()
    start_idx = progress["last_completed_index"] + 1

    mode = "a" if start_idx > 0 else "w"
    if start_idx > 0:
        print(f"Resuming from article index {start_idx}")

    row_counter = start_idx * len(enabled_keys)  # approximate

    with open(output_csv, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        if mode == "w":
            writer.writeheader()

        for idx in range(start_idx, n):
            article = sample.iloc[idx]
            doi = article.get("doi", "")
            openalex_id = article.get("openalex_id", "")
            title = article.get("title", "")
            article_id = doi if doi else openalex_id

            print(f"[{idx+1}/{n}] {article_id[:60]}…")

            source_rows = []

            # --- Run each enabled probe ---
            for key in enabled_keys:
                cfg = SOURCE_REGISTRY[key]
                label = cfg["label"]

                # Skip DOI-required sources when no DOI
                if cfg["requires_doi"] and not doi:
                    print(f"    → {label} … skipped (no DOI)")
                    continue

                print(f"    → {label} …")
                probe_fn = PROBE_FUNCTIONS[key]
                r = probe_fn(
                    doi=doi, title=title, openalex_id=openalex_id,
                    openalex_api_key=openalex_api_key,
                    core_api_key=core_api_key,
                    scihub_mirror=scihub_mirror,
                )
                if r:
                    source_rows.append(r)

            # --- Merge BibTeX + access info across sources ---
            merge_access_into_publisher(source_rows)

            num_sources = len(source_rows)

            # --- Write stacked rows ---
            for sr in source_rows:
                row_counter += 1
                out = {
                    "row_number": row_counter,
                    "article_id": article_id,
                    "doi": doi,
                    "title": sanitize(title),
                    "num_sources": num_sources,
                    **sr,
                }
                writer.writerow(out)

            save_progress_probe(idx)
            print(f"    ✓ {num_sources} sources found\n")

    # Clean up progress
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    # ---- Summary ----
    print("=" * 70)
    result = pd.read_csv(output_csv)
    n_articles = result["article_id"].nunique()
    print(f"Done. {len(result):,} rows for {n_articles} articles → {output_csv}")
    print(f"\nSource breakdown:")
    print(result["source_name"].value_counts().to_string())
    print(f"\nSource type breakdown:")
    print(result["source_type"].value_counts().to_string())
    print(f"\nAccess type breakdown:")
    print(result["access_type"].value_counts().to_string())
    print(f"\nArticles with BibTeX: "
          f"{result[result['bibtex_entry'] != '']['article_id'].nunique()}")
    print(f"Articles with abstract: "
          f"{result[result['abstract'] != '']['article_id'].nunique()}")


if __name__ == "__main__":
    main()
