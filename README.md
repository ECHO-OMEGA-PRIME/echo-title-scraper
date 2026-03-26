# echo-title-scraper

> Chain-of-title training data generator that scrapes Texas county deed records via ShadowGlass and produces ChatML JSONL for TitleHound AI model training.

## Overview

Echo Title Scraper builds chain-of-title training data for the TitleHound AI model by querying deed records from ShadowGlass (259K+ records across 80 Texas counties). For each tract, it assembles grantor-to-grantee document chains, scores completeness using a 5-gate framework (Continuity, Outconveyance, Probate/Heirship, Severance, Liens), identifies gaps, and generates ChatML-formatted JSONL training examples. The target is 2,000-5,000 high-quality training examples across 13 Permian Basin priority counties.

All authenticated endpoints require the `X-Echo-API-Key` header.

## Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | No | Health check with tract/chain/example counts, progress percentage, and priority counties |
| `GET` | `/init` | No | Initialize database schema (create all tables and indexes) |
| `POST` | `/scrape/tract` | Yes | Scrape a single tract. Body: `{county, section, block?, survey?}` |
| `POST` | `/scrape/batch` | Yes | Scrape multiple tracts (max 200). Body: `{tracts: [{county, section, block?, survey?}]}` |
| `POST` | `/scrape/county` | Yes | Auto-discover and scrape tracts for a county. Body: `{county, limit?}` (max 100) |
| `GET` | `/chains` | Yes | Query assembled chains. Filters: `county`, `min_docs`, `chain_type`, `limit` |
| `GET` | `/chains/:id` | Yes | Full chain detail with parsed documents, gaps, and linked training examples |
| `POST` | `/training/generate` | Yes | Generate training examples from existing chains. Body: `{county?, limit?, min_quality?}` |
| `GET` | `/training/export` | Yes | Export all training examples as ChatML JSONL (downloadable file) |
| `GET` | `/training/stats` | Yes | Training dataset statistics: total examples, quality distribution, county coverage |
| `GET` | `/status` | Yes | Operational status: recent runs, tracts by status, county coverage, training stats |
| `GET` | `/dashboard` | Yes | Full dashboard: totals, complete/interesting chains, quality distribution, gap types, county breakdown |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_VERSION` | `1.0.0` | Version identifier |
| `ENVIRONMENT` | `production` | Runtime environment label |

### Secrets

| Secret | Description |
|--------|-------------|
| `ECHO_API_KEY` | API key required for authenticated endpoints (`X-Echo-API-Key` header) |

### Bindings

| Binding | Type | Service/Resource |
|---------|------|------------------|
| `DB` | D1 Database | `echo-title-scraper` — tracts, documents, chains, training examples, scrape runs |
| `TITLE_CACHE` | KV Namespace | Cache for tract lookups and chain assembly results |
| `RECORDS` | R2 Bucket | `echo-prime-records` — persistent storage for exported training data |
| `SHADOWGLASS` | Service Binding | `shadowglass-v8-warpspeed` — deed record query source (259K+ records) |
| `BRAIN` | Service Binding | `echo-shared-brain` — broadcasts scrape progress and training milestones |
| `AI_ORCHESTRATOR` | Service Binding | `echo-ai-orchestrator` — multi-LLM inference for chain analysis |
| `AI` | Workers AI | Fallback AI for chain gap analysis |

### Cron Triggers

| Schedule | Description |
|----------|-------------|
| `0 4 * * *` | Daily automated scrape at 04:00 UTC (11pm CST) |

## Deployment

```bash
cd O:\ECHO_OMEGA_PRIME\WORKERS\echo-title-scraper
npx wrangler deploy
echo "your-api-key" | npx wrangler secret put ECHO_API_KEY
```

## Architecture

Built on Hono with CORS and smart placement enabled. The scraping pipeline works in three stages: (1) query ShadowGlass for deed records matching a tract's legal description, (2) assemble documents into chronological chains tracking grantor-to-grantee transfers, and (3) apply the TitleHound 5-gate framework to score chain completeness and identify gaps. Chains are classified by type (full, partial, gap-heavy) and scored 0.0-1.0 for quality. Training example generation converts scored chains into ChatML-formatted prompt/response pairs using the TitleHound system prompt. The JSONL export endpoint produces files ready for QLoRA fine-tuning. Priority counties are the 13 Permian Basin counties: Reeves, Loving, Ward, Winkler, Pecos, Crane, Upton, Midland, Ector, Martin, Howard, Andrews, and Lea.
