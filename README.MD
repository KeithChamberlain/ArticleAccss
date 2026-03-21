# Scientific Article Access & Availability Research Toolkit

*Empirical analysis of how and when clinical psychology articles (2021–2025) become accessible online.*

---

## Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Journal Selection](#journal-selection)
- [Source Registry](#source-registry)
- [Output Schema](#output-schema)
- [Classification Rubrics](#classification-rubrics)
- [Rate Limiting & Ethics](#rate-limiting--ethics)
- [CLI Arguments & Environment Variables](#cli-arguments--environment-variables)
- [Limitations](#limitations)
- [References](#references)
- [License](#license)

---

## Overview

This toolkit answers a set of questions that matter to researchers, librarians, and policymakers:

- What fraction of recent clinical psychology articles are freely available?
- Through which channels (gold/green OA, preprint servers, repositories)?
- How long after publication does free access typically appear?
- Does availability vary by journal tier, funding source, or sub-topic?

It does this in two stages: first cataloging the full population of articles from top journals, then probing a random sample against multiple metadata sources to characterize access.

**No full text or PDFs are downloaded at any stage.** All probes collect metadata only — BibTeX records, abstracts, OA status flags, landing-page URLs, and publication dates.

## Project Structure

```
.
├── README.md                ← You are here
├── article_access_research_plan.md  ← Full research plan & methodology
├── catalog_population.py    ← Script 1: enumerate article population
├── sample_and_probe.py      ← Script 2: sample & probe multiple sources
├── requirements.txt         ← Python dependencies

Generated outputs (after running the scripts)
├── articles.csv             ← Full population catalog (Script 1)
├── catalog_stats.txt        ← Summary statistics (Script 1)
├── resolved_sources.json    ← ISSN → OpenAlex source ID cache (Script 1)
├── sample_results.csv       ← Stacked results table (Script 2)
└── *.json                   ← Progress files (auto-cleaned on success)
```

## Requirements

- Python 3.10+
- An OpenAlex API key (free — [get one here](https://developers.openalex.org/guides/authentication)). Required since Feb 2025; without a key you're limited to 100 requests/day.

Install dependencies:

```bash
pip install -r requirements.txt
```

The only runtime dependencies are `requests` and `pandas`.

## Quick Start

### 1. Catalog the population

Enumerate all clinical psychology articles (2021–2025) from the top 10 empirical journals by SJR ranking:

```bash
python catalog_population.py --api-key YOUR_OPENALEX_KEY
```

This produces `articles.csv` (one row per article) and `catalog_stats.txt` (summary counts by year, journal, OA status, and publisher).

Debug mode (fetches 1 page, prints URLs and response preview):

```bash
python catalog_population.py --api-key YOUR_KEY --debug
```

Resume an interrupted run:

```bash
python catalog_population.py --api-key YOUR_KEY --resume
```

### 2. Sample and probe sources

Draw a random sample of 1,000 articles from the catalog, then query up to 7 metadata sources for each:

```bash
python sample_and_probe.py
```

This produces `sample_results.csv` — a stacked table with one row per article × source, including BibTeX entries, abstracts, OA status, access type, publisher domains, redirect chains, and availability dates.

The probe is resumable; if interrupted it picks up where it left off.

Pass API keys and the Sci-Hub mirror URL on the command line or via environment variables:

```bash
# All keys from the command line
python sample_and_probe.py \
  --OPENALEX_API_KEY YOUR_KEY \
  --CORE_API_KEY YOUR_CORE_KEY \
  --SCIHUB_MIRROR https://sci-hub.se

# Or via environment variables (same precedence)
export OPENALEX_API_KEY=YOUR_KEY
export CORE_API_KEY=YOUR_CORE_KEY
export SCIHUB_MIRROR=https://sci-hub.se
python sample_and_probe.py

# Mix: some from CLI, some from env
export CORE_API_KEY=YOUR_CORE_KEY
python sample_and_probe.py --OPENALEX_API_KEY YOUR_KEY
```

> Command-line values take precedence over environment variables when both are set.

## Journal Selection

The catalog targets the top 10 empirical clinical psychology journals by Scimago/SJR (2024), category 3203. Review-only journals are excluded and backfilled from the SJR list:

| ISSN | Journal | SJR | Rank |
|------|---------|-----|------|
| 0033-3190 | Psychotherapy and Psychosomatics | 4.904 | 4 |
| 2167-7026 | Clinical Psychological Science | 2.513 | 7 |
| 2062-5871 | Journal of Behavioral Addictions | 2.260 | 10 |
| 0144-6657 | British Journal of Clinical Psychology | 2.256 | 11 |
| 0022-006X | Journal of Consulting and Clinical Psychology | 2.231 | 12 |
| 0887-6185 | Journal of Anxiety Disorders | 2.171 | 13 |
| 1073-1911 | Assessment | 2.158 | 14 |
| 0165-0327 | Journal of Affective Disorders | 2.121 | 16 |
| 0021-843X | Journal of Abnormal Psychology | 2.044 | 18 |
| 0005-7967 | Behaviour Research and Therapy | 2.009 | 19 |

The renamed Journal of Psychopathology and Clinical Science (ISSN 2769-755X, formerly J. Abnormal Psychology) is also resolved.

## Source Registry

Script 2 probes each sampled article against multiple metadata sources. Sources can be enabled or disabled in the `SOURCE_REGISTRY` table at the top of `sample_and_probe.py`:

| Source | Enabled | Type | What it checks |
|--------|---------|------|----------------|
| Publisher Site | ✓ | publisher | DOI → landing page redirect, HTTP status |
| CrossRef | ✓ | publisher | BibTeX via content negotiation, license, dates |
| Unpaywall | ✓ | publisher | OA status, host type, best OA location |
| Semantic Scholar | ✓ | other | Abstract, external IDs, OA PDF flag |
| OpenAlex | ✓ | varies | All hosting locations, inverted abstract, dates |
| PubMed / NCBI | ✓ | repository | PMID, PMC deposit date, MeSH terms |
| Europe PMC | ✓ | repository | Preprints, author manuscripts, full-text flags |
| CORE | ✗ | repository | Harvest/deposit date (requires free API key) |
| Wayback Machine | ✗ | other | First web-archive snapshot date |
| Sci-Hub | ✗ | other | Shadow-library availability check |

To enable CORE, pass `--CORE_API_KEY` on the command line (or set `CORE_API_KEY` in your environment) and flip `"enabled": True` in the registry. Same pattern for Sci-Hub: pass `--SCIHUB_MIRROR` with the mirror base URL and enable the source in the registry. Wayback Machine needs no key — just enable it.

## Output Schema

### articles.csv (Script 1)

One row per article:

| Column | Description |
|--------|-------------|
| `openalex_id` | OpenAlex work ID |
| `doi` | DOI (without `https://doi.org/` prefix) |
| `title` | Article title |
| `publication_year` | Year of publication |
| `publication_date` | Full ISO date |
| `journal_name` | Journal display name |
| `journal_issn` | ISSN-L |
| `publisher` | Host organization name |
| `is_oa` | Boolean OA flag |
| `oa_status` | gold / green / hybrid / bronze / closed |
| `cited_by_count` | Citation count at time of query |
| `authors_short` | "First Author et al." or single author |
| `num_authors` | Author count |

### sample_results.csv (Script 2)

Stacked table — one row per article × source:

| Column | Description |
|--------|-------------|
| `row_number` | Sequential integer |
| `article_id` | DOI or OpenAlex ID |
| `doi` | DOI |
| `title` | Article title |
| `num_sources` | Total sources found for this article |
| `source_name` | crossref, unpaywall, semantic_scholar, etc. |
| `source_type` | publisher / reprint / repository / preprint / other |
| `access_type` | open / free / pay |
| `source_url` | Landing page or API URL |
| `publisher_domain` | Effective domain of final destination |
| `http_status` | HTTP status code (publisher site probe) |
| `redirect_chain` | Pipe-separated domains traversed |
| `bibtex_entry` | Full BibTeX string |
| `abstract` | Abstract text |
| `date_first_available` | Earliest date this source shows the article |

## Classification Rubrics

### Source Type

| Value | Definition |
|-------|------------|
| `publisher` | Hosted on the publisher's own platform |
| `reprint` | Self-archived copy (ResearchGate, institutional IR) |
| `repository` | Curated OA repository (PMC, Europe PMC, CORE) |
| `preprint` | Pre-peer-review version (PsyArXiv, medRxiv, etc.) |
| `other` | Anything else |

### Access Type

| Value | Definition |
|-------|------------|
| `open` | Gold or green OA under a recognized license (CC-BY, CC-BY-NC…) |
| `free` | Readable without payment but no OA license (bronze, delayed OA) |
| `pay` | Requires subscription or per-article payment |

## Rate Limiting & Ethics

All APIs are queried within their stated limits. The scripts implement exponential backoff on HTTP 429 responses and include configurable delays per source. No full text or PDFs are downloaded — only metadata, abstracts, and BibTeX citation records.

| Source | Rate Limit | Script Delay |
|--------|------------|--------------|
| OpenAlex | 100K credits/day (with key) | 0.1s |
| CrossRef | 50 req/s (with mailto) | 0.05s |
| Unpaywall | 100K req/day | 0.1s |
| Semantic Scholar | 100 req / 5 min | 1.0s |
| PubMed / NCBI | 3 req/s (10 with API key) | 0.34s |
| Europe PMC | No hard limit | 0.2s |
| CORE | ~5 req/s (free tier) | 0.5s |
| Wayback Machine | No hard limit | 0.5s |
| Publisher sites | Varies | 0.5s |

## CLI Arguments & Environment Variables

Script 2 (`sample_and_probe.py`) accepts the following command-line arguments. All API keys and the Sci-Hub mirror URL can be supplied either via CLI flags or environment variables; **CLI values take precedence** when both are set.

### sample_and_probe.py arguments

| Flag | Env Var | Required | Description |
|------|---------|----------|-------------|
| `--seed SEED` | — | No | Random seed for reproducible sampling |
| `-n, --sample-size N` | — | No | Number of articles to sample (default: 1000) |
| `-i, --input PATH` | — | No | Input CSV path (default: articles.csv) |
| `-o, --output PATH` | — | No | Output CSV path (default: sample_results.csv) |
| `--resume` | — | No | Resume an interrupted run |
| `--OPENALEX_API_KEY KEY` | `OPENALEX_API_KEY` | No\* | OpenAlex API key for polite/premium pool |
| `--CORE_API_KEY KEY` | `CORE_API_KEY` | No\*\* | CORE API v3 key |
| `--SCIHUB_MIRROR URL` | `SCIHUB_MIRROR` | No\*\* | Sci-Hub mirror base URL (e.g. https://sci-hub.se) |

\* Recommended. Without an OpenAlex API key you are limited to the polite pool with mailto headers.

\*\* Required only when the corresponding source is enabled in SOURCE_REGISTRY.

### catalog_population.py arguments

| Flag | Env Var | Description |
|------|---------|-------------|
| `--api-key KEY` | `OPENALEX_API_KEY` | OpenAlex API key (required since Feb 2025) |
| `--debug` | — | Print URLs, fetch 1 page, show response preview |
| `-o, --output PATH` | — | Output CSV path (default: articles.csv) |
| `--resume` | — | Resume an interrupted run |
| `--refresh-sources` | — | Force re-resolve ISSNs (ignore cache) |

### Additional environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENALEX_MAILTO` | No | Email for polite-pool headers |
| `NCBI_API_KEY` | No | Speeds up PubMed queries (3→10 req/s) |

## Limitations

- **Temporal bias:** OpenAlex indexes articles after DOI registration; very recent articles may be under-represented.
- **Concept tagging:** OpenAlex concept assignment is automated and probabilistic; some articles may be miscategorized.
- **Shadow libraries:** The research plan documents how one could check Sci-Hub/LibGen availability, but the scripts do not query those services by default.
- **Wayback Machine / CORE:** Disabled by default; enable them in the source registry if desired.
- **Longitudinal tracking:** Running the probe quarterly would reveal how access status changes over time (e.g., embargo lifting).

## References

Bibliometric precedent:

1. Himmelstein, D. S., Romero, A. R., Levernier, J. G., Munro, T. A., McLaughlin, S. R., Greshake Tzovaras, B., & Greene, C. S. (2018). Sci-Hub provides access to nearly all scholarly literature. *eLife*, 7, e32822. <https://doi.org/10.7554/eLife.32822>

2. Bohannon, J. (2016). Who's downloading pirated papers? Everyone. *Science*, 352(6285), 508–512. <https://doi.org/10.1126/science.352.6285.508>

3. Greshake, B. (2017). Looking into Pandora's Box: The content of Sci-Hub and its usage. *F1000Research*, 6, 541. <https://doi.org/10.12688/f1000research.11366.1>

## License

Research use. See `article_access_research_plan.md` for full methodology.

---

*Generated 2026-03-20*
