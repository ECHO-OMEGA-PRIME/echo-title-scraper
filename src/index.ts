/**
 * ECHO TITLE SCRAPER v1.0.0
 * Chain of Title Training Data Generator — Cloudflare Worker
 *
 * Scrapes completed chains of title from Texas county public records
 * via ShadowGlass (259K+ deed records, 80 counties). Assembles grantor→grantee
 * chains, scores completeness, identifies gaps, and outputs ChatML JSONL
 * for TitleHound AI model training.
 *
 * Target: 2,000–5,000 training examples across 13 Permian Basin counties.
 */

import { Hono } from 'hono';
import { cors } from 'hono/cors';

// ─── Structured Logging ─────────────────────────────────────────────────────

function log(level: 'info' | 'warn' | 'error', message: string, data?: Record<string, unknown>) {
  const entry = { ts: new Date().toISOString(), level, worker: 'echo-title-scraper', message, ...data };
  if (level === 'error') console.error(JSON.stringify(entry));
  else console.log(JSON.stringify(entry));
}

// ─── Types ──────────────────────────────────────────────────────────────────

interface Env {
  DB: D1Database;
  TITLE_CACHE: KVNamespace;
  RECORDS: R2Bucket;
  SHADOWGLASS: Fetcher;
  BRAIN: Fetcher;
  AI_ORCHESTRATOR: Fetcher;
  AI: Ai;
  WORKER_VERSION: string;
  ENVIRONMENT: string;
  ECHO_API_KEY: string;
}

interface ChainDocument {
  doc_type: string;
  instrument_number: string;
  recording_date: string;
  grantor: string;
  grantee: string;
  legal_description: string;
  consideration: string;
  acreage: number | null;
  volume: string;
  page: string;
  notes: string;
}

interface ChainGap {
  gate: string;
  description: string;
  severity: 'high' | 'medium' | 'low';
}

interface ChainResult {
  tract: { county: string; section: string; block: string; survey: string };
  documents: ChainDocument[];
  chain_complete: boolean;
  gaps: ChainGap[];
  quality_score: number;
  gap_types: string[];
  is_interesting: boolean;
}

interface TractInput {
  county: string;
  section: string;
  block?: string;
  survey?: string;
}

// ─── Constants ──────────────────────────────────────────────────────────────

const PERMIAN_COUNTIES = [
  'Reeves', 'Loving', 'Ward', 'Winkler', 'Pecos', 'Crane', 'Upton',
  'Midland', 'Ector', 'Martin', 'Howard', 'Andrews', 'Lea',
];

const CHAIN_DOC_TYPES = new Set([
  'warranty deed', 'general warranty deed', 'special warranty deed',
  'deed', 'quit claim deed', 'mineral deed', 'royalty deed',
  'assignment', 'oil gas lease', 'release of lien', 'deed of trust',
  'correction deed', 'probate', 'court order', 'partition deed',
  'right of way', 'easement', 'surface lease', 'affidavit of heirship',
  'heirship affidavit', 'judgment', 'lis pendens', 'mechanics lien',
  'abstract of judgment', 'power of attorney', 'ratification',
]);

const TITLEHOUND_SYSTEM_PROMPT = `You are TitleHound, an expert AI title examiner specializing in Texas oil and gas title chains. You apply a rigorous 5-gate framework to every chain of title analysis:

GATE 1 — CONTINUITY: Verify unbroken chain from sovereign (State of Texas) to current owner. Every conveyance must connect grantor-to-grantee without gaps. Flag missing links, name variations, and breaks.

GATE 2 — OUTCONVEYANCE: Track all outconveyances (partial interest transfers, mineral reservations, overriding royalty interests). Calculate remaining net mineral interest. Flag any interest that exceeds 100% (mathematical impossibility).

GATE 3 — PROBATE/HEIRSHIP: When an owner dies, verify proper probate proceedings or heirship affidavits. Flag estates without recorded probate. Track all heirs and their fractional interests.

GATE 4 — SEVERANCE: Track separation of surface and mineral estates. Identify when minerals were severed, by whom, and what fraction. Maintain parallel chains for surface and mineral interests.

GATE 5 — LIENS & ENCUMBRANCES: Identify all liens, deeds of trust, judgments, and lis pendens. Verify releases. Flag unreleased liens that cloud title.

For each analysis, output a structured JSON response with:
- run_sheet: chronological list of all instruments with grantor, grantee, type, date, and interest conveyed
- gaps: identified gaps or issues in the chain, classified by gate
- next_search_actions: recommended next documents to search for to resolve gaps
- gate_status: pass/fail/needs_investigation for each of the 5 gates
- net_mineral_interest: calculated NMI for current apparent owner(s)`;

// ─── D1 Schema ──────────────────────────────────────────────────────────────

const SCHEMA_STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS tracts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    county TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'TX',
    section TEXT,
    block TEXT,
    survey TEXT,
    abstract_number TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    document_count INTEGER DEFAULT 0,
    chain_complete INTEGER DEFAULT 0,
    last_scraped TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(county, section, block, survey)
  )`,
  `CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tract_id INTEGER NOT NULL,
    doc_type TEXT NOT NULL,
    volume TEXT,
    page TEXT,
    date_filed TEXT,
    date_executed TEXT,
    grantor TEXT,
    grantee TEXT,
    legal_description TEXT,
    instrument_number TEXT,
    consideration TEXT,
    acreage REAL,
    notes TEXT,
    raw_data TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (tract_id) REFERENCES tracts(id)
  )`,
  `CREATE TABLE IF NOT EXISTS chains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tract_id INTEGER NOT NULL,
    chain_type TEXT NOT NULL DEFAULT 'full',
    documents TEXT DEFAULT '[]',
    gaps TEXT DEFAULT '[]',
    score REAL DEFAULT 0,
    notes TEXT,
    validated INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (tract_id) REFERENCES tracts(id)
  )`,
  `CREATE TABLE IF NOT EXISTS training_examples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id INTEGER NOT NULL,
    tract_id INTEGER NOT NULL,
    system_prompt TEXT NOT NULL,
    user_prompt TEXT NOT NULL,
    assistant_response TEXT NOT NULL,
    format TEXT NOT NULL DEFAULT 'chatml',
    quality_score REAL DEFAULT 0,
    gap_types TEXT DEFAULT '[]',
    exported INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (chain_id) REFERENCES chains(id),
    FOREIGN KEY (tract_id) REFERENCES tracts(id)
  )`,
  `CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    county TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    tracts_processed INTEGER DEFAULT 0,
    docs_found INTEGER DEFAULT 0,
    chains_built INTEGER DEFAULT 0,
    examples_generated INTEGER DEFAULT 0,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_tracts_county ON tracts(county)`,
  `CREATE INDEX IF NOT EXISTS idx_tracts_status ON tracts(status)`,
  `CREATE INDEX IF NOT EXISTS idx_docs_tract ON documents(tract_id)`,
  `CREATE INDEX IF NOT EXISTS idx_docs_grantor ON documents(grantor)`,
  `CREATE INDEX IF NOT EXISTS idx_docs_grantee ON documents(grantee)`,
  `CREATE INDEX IF NOT EXISTS idx_docs_instrument ON documents(instrument_number)`,
  `CREATE INDEX IF NOT EXISTS idx_chains_tract ON chains(tract_id)`,
  `CREATE INDEX IF NOT EXISTS idx_chains_score ON chains(score)`,
  `CREATE INDEX IF NOT EXISTS idx_training_chain ON training_examples(chain_id)`,
  `CREATE INDEX IF NOT EXISTS idx_training_exported ON training_examples(exported)`,
  `CREATE INDEX IF NOT EXISTS idx_runs_status ON scrape_runs(status)`,
];

// ─── Name Normalization ─────────────────────────────────────────────────────

function normalizePartyName(name: string): string {
  return name
    .toLowerCase()
    .replace(/,\s*(jr|sr|ii|iii|iv|v)\.?$/i, '')
    .replace(/\b(mr|mrs|ms|dr|rev|hon)\.?\s*/gi, '')
    .replace(/\b(et\s*(al|ux|vir))\b/gi, '')
    .replace(/\s*\([^)]*\)\s*/g, ' ')
    .replace(/[.,;]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function namesMatch(nameA: string, nameB: string): boolean {
  const a = normalizePartyName(nameA);
  const b = normalizePartyName(nameB);
  if (a === b) return true;
  if (a.length === 0 || b.length === 0) return false;

  const partsA = a.split(' ').filter(Boolean);
  const partsB = b.split(' ').filter(Boolean);

  // Match on first and last name
  if (partsA.length >= 2 && partsB.length >= 2) {
    const firstMatch = partsA[0] === partsB[0];
    const lastMatch = partsA[partsA.length - 1] === partsB[partsB.length - 1];
    if (firstMatch && lastMatch) return true;
  }

  // Check if one contains the other (corporation names)
  if (a.includes(b) || b.includes(a)) return true;

  return false;
}

// ─── ShadowGlass Integration ────────────────────────────────────────────────

async function queryShadowGlassRecords(
  env: Env, county: string, section?: string, block?: string, survey?: string
): Promise<any[]> {
  const cacheKey = `sg:${county}:${section || ''}:${block || ''}:${survey || ''}`;
  const cached = await env.TITLE_CACHE.get(cacheKey, 'json');
  if (cached) return cached as any[];

  try {
    const params = new URLSearchParams({ county, limit: '500' });
    if (section) params.set('section', section);
    if (block) params.set('block', block);
    if (survey) params.set('survey', survey);

    const resp = await env.SHADOWGLASS.fetch(
      `https://sg/search?${params.toString()}`,
      { headers: { 'Content-Type': 'application/json' } }
    );
    if (!resp.ok) return [];

    const data = await resp.json() as any;
    const records = data.data || data.records || data.results || [];

    // Cache for 6 hours
    if (records.length > 0) {
      await env.TITLE_CACHE.put(cacheKey, JSON.stringify(records), { expirationTtl: 21600 });
    }
    return records;
  } catch (e) {
    log("warn", "ShadowGlass record query failed", { error: (e as Error)?.message || String(e) });
    return [];
  }
}

async function queryShadowGlassSearch(env: Env, county: string, query: string): Promise<any[]> {
  try {
    const params = new URLSearchParams({ county, q: query, limit: '200' });
    const resp = await env.SHADOWGLASS.fetch(
      `https://sg/search?${params.toString()}`,
      { headers: { 'Content-Type': 'application/json' } }
    );
    if (!resp.ok) return [];
    const data = await resp.json() as any;
    return data.data || data.records || data.results || [];
  } catch (e) {
    log("warn", "ShadowGlass search query failed", { error: (e as Error)?.message || String(e) });
    return [];
  }
}

async function getCountyTractDistribution(env: Env, county: string): Promise<Map<string, any[]>> {
  const records = await queryShadowGlassRecords(env, county);
  const tractMap = new Map<string, any[]>();

  for (const r of records) {
    // Extract section — REQUIRED for tract identification
    let rawSection = (r.section || r.sec || '').toString().trim();
    if (!rawSection) continue; // Skip records without section data

    // Handle multi-section values like "13; 12" — use first section only
    const section = rawSection.split(/[;,]/)[0].trim();
    if (!section) continue;

    // Extract block (optional but useful)
    let rawBlock = (r.block || r.blk || '').toString().trim();
    const block = rawBlock.split(/[;,]/)[0].trim();

    const key = `${section}|${block}`;
    const existing = tractMap.get(key) || [];
    existing.push(r);
    tractMap.set(key, existing);
  }

  return tractMap;
}

// ─── Chain Assembly ─────────────────────────────────────────────────────────

function parseRecord(r: any): ChainDocument {
  return {
    doc_type: r.doc_type || r.instrument_type || r.type || 'Unknown',
    instrument_number: r.instrument_number || r.doc_number || r.inst_no || '',
    recording_date: r.recording_date || r.file_date || r.date || r.date_filed || '',
    grantor: r.grantor || r.seller || r.from || '',
    grantee: r.grantee || r.buyer || r.to || '',
    legal_description: r.legal_description || r.legal || r.description || '',
    consideration: r.consideration || r.amount || '',
    acreage: r.acreage || r.acres || null,
    volume: r.volume || r.vol || '',
    page: r.page || r.pg || '',
    notes: r.notes || r.remarks || '',
  };
}

function assembleChain(records: any[], tract: TractInput): ChainResult {
  const docs = records.map(parseRecord).filter(d => d.recording_date || d.grantor || d.grantee);

  // Sort by recording date ascending
  docs.sort((a, b) => {
    const dateA = a.recording_date || '0000-00-00';
    const dateB = b.recording_date || '0000-00-00';
    return dateA.localeCompare(dateB);
  });

  const gaps: ChainGap[] = [];
  const gapTypes: string[] = [];

  // ── Gate 1: Continuity ──
  for (let i = 1; i < docs.length; i++) {
    const prev = docs[i - 1];
    const curr = docs[i];
    if (!prev.grantee || !curr.grantor) continue;

    // Check if previous grantee matches current grantor
    if (!namesMatch(prev.grantee, curr.grantor)) {
      // Allow for multi-party conveyances (check if any part matches)
      const prevGrantees = prev.grantee.split(/[;&]/).map(s => s.trim()).filter(Boolean);
      const currGrantors = curr.grantor.split(/[;&]/).map(s => s.trim()).filter(Boolean);
      const anyMatch = prevGrantees.some(pg =>
        currGrantors.some(cg => namesMatch(pg, cg))
      );

      if (!anyMatch) {
        gaps.push({
          gate: 'continuity',
          description: `Chain break between doc ${i} and ${i + 1}: "${prev.grantee}" → gap → "${curr.grantor}"`,
          severity: 'high',
        });
        if (!gapTypes.includes('continuity')) gapTypes.push('continuity');
      }
    }
  }

  // Check for empty chain or single-document chain
  if (docs.length < 2) {
    gaps.push({
      gate: 'continuity',
      description: `Insufficient documents (${docs.length}) to verify continuity`,
      severity: 'medium',
    });
    if (!gapTypes.includes('continuity')) gapTypes.push('continuity');
  }

  // ── Gate 2: Outconveyance ──
  const mineralDocs = docs.filter(d => {
    const dt = d.doc_type.toLowerCase();
    return dt.includes('mineral') || dt.includes('royalty') || dt.includes('assignment');
  });
  if (mineralDocs.length > 0) {
    if (!gapTypes.includes('outconveyance')) gapTypes.push('outconveyance');
    // Check for potential over-conveyance (multiple mineral deeds from same grantor)
    const mineralGrantors = new Map<string, number>();
    for (const md of mineralDocs) {
      const norm = normalizePartyName(md.grantor);
      mineralGrantors.set(norm, (mineralGrantors.get(norm) || 0) + 1);
    }
    for (const [grantor, count] of mineralGrantors) {
      if (count > 2) {
        gaps.push({
          gate: 'outconveyance',
          description: `Multiple mineral conveyances (${count}) from "${grantor}" — potential over-conveyance risk`,
          severity: 'medium',
        });
      }
    }
  }

  // ── Gate 3: Probate/Heirship ──
  const deathIndicators = docs.filter(d => {
    const dt = d.doc_type.toLowerCase();
    const gr = d.grantor.toLowerCase();
    return dt.includes('probate') || dt.includes('heirship') || dt.includes('estate') ||
      gr.includes('estate of') || gr.includes('heirs of') || gr.includes('deceased');
  });
  if (deathIndicators.length > 0) {
    if (!gapTypes.includes('probate')) gapTypes.push('probate');
    const hasProbateDoc = docs.some(d => {
      const dt = d.doc_type.toLowerCase();
      return dt.includes('probate') || dt.includes('heirship') || dt.includes('letters testamentary');
    });
    if (!hasProbateDoc) {
      gaps.push({
        gate: 'probate',
        description: 'Estate/heir references found but no recorded probate or heirship affidavit located',
        severity: 'high',
      });
    }
  }

  // ── Gate 4: Severance ──
  const severanceDocs = docs.filter(d => {
    const dt = d.doc_type.toLowerCase();
    return dt.includes('mineral deed') || dt.includes('surface') ||
      (d.notes && d.notes.toLowerCase().includes('mineral'));
  });
  if (severanceDocs.length > 0) {
    if (!gapTypes.includes('severance')) gapTypes.push('severance');
    gaps.push({
      gate: 'severance',
      description: `Surface/mineral severance detected in ${severanceDocs.length} document(s) — parallel chains required`,
      severity: 'medium',
    });
  }

  // ── Gate 5: Liens & Encumbrances ──
  const lienDocs = docs.filter(d => {
    const dt = d.doc_type.toLowerCase();
    return dt.includes('lien') || dt.includes('deed of trust') || dt.includes('judgment') ||
      dt.includes('lis pendens') || dt.includes('mechanic');
  });
  const releaseDocs = docs.filter(d => {
    const dt = d.doc_type.toLowerCase();
    return dt.includes('release') || dt.includes('satisfaction') || dt.includes('reconveyance');
  });
  if (lienDocs.length > releaseDocs.length) {
    if (!gapTypes.includes('liens')) gapTypes.push('liens');
    gaps.push({
      gate: 'liens',
      description: `${lienDocs.length} lien(s)/encumbrance(s) found with only ${releaseDocs.length} release(s) — unreleased liens may cloud title`,
      severity: lienDocs.length - releaseDocs.length > 2 ? 'high' : 'medium',
    });
  }

  // ── Quality Score ──
  let quality = 1.0;
  const highGaps = gaps.filter(g => g.severity === 'high').length;
  const medGaps = gaps.filter(g => g.severity === 'medium').length;
  quality -= highGaps * 0.15;
  quality -= medGaps * 0.05;
  if (docs.length < 3) quality -= 0.3;
  if (docs.length >= 5 && docs.length <= 15) quality += 0.05;
  if (docs.length > 15) quality += 0.1;
  // Bonus for having dates spanning many years
  if (docs.length >= 2) {
    const firstYear = parseInt(docs[0].recording_date?.slice(0, 4) || '0');
    const lastYear = parseInt(docs[docs.length - 1].recording_date?.slice(0, 4) || '0');
    if (lastYear - firstYear > 30) quality += 0.05;
  }
  quality = Math.max(0, Math.min(1, quality));

  const isInteresting =
    gapTypes.length >= 2 ||
    docs.length >= 6 ||
    deathIndicators.length > 0 ||
    severanceDocs.length > 0 ||
    (mineralDocs.length >= 2 && lienDocs.length >= 1);

  const chainComplete = highGaps === 0 && docs.length >= 3;

  return {
    tract: {
      county: tract.county,
      section: tract.section || '',
      block: tract.block || '',
      survey: tract.survey || '',
    },
    documents: docs,
    chain_complete: chainComplete,
    gaps,
    quality_score: Math.round(quality * 100) / 100,
    gap_types: gapTypes,
    is_interesting: isInteresting,
  };
}

// ─── Training Data Generation ───────────────────────────────────────────────

function gapToAction(gap: ChainGap): string {
  const actions: Record<string, string> = {
    continuity: 'Search grantor-grantee index for missing link party. Check name variations, married names, trust transfers, and corporate successor entities.',
    probate: 'Search probate records in county clerk office. Look for heirship affidavits, letters testamentary, and death certificates. Check district court probate filings.',
    outconveyance: 'Pull all mineral deeds, royalty conveyances, and assignments. Calculate fractional interests for each party. Verify no over-conveyance exceeding 100%.',
    severance: 'Trace mineral estate separately from surface estate. Identify original severance instrument and all subsequent mineral transfers. Build parallel run sheets.',
    liens: 'Search for lien releases, deed of trust satisfactions, and judgment releases. Verify each lien has a corresponding release or is still outstanding.',
  };
  return actions[gap.gate] || 'Conduct further investigation to resolve this issue.';
}

function buildTrainingExample(chain: ChainResult): {
  messages: { role: string; content: string }[];
  metadata: Record<string, any>;
} {
  const t = chain.tract;
  const tractDescription = [
    `Section ${t.section || 'N/A'}`,
    `Block ${t.block || 'N/A'}`,
    t.survey ? `${t.survey} Survey` : null,
    `${t.county} County, Texas`,
  ].filter(Boolean).join(', ');

  // Build grantor-grantee index (what a real title examiner would receive)
  const grantorIndex = chain.documents.map((d, i) =>
    `${i + 1}. ${d.recording_date || 'undated'} | ${d.doc_type} | Grantor: ${d.grantor || 'N/A'} | Grantee: ${d.grantee || 'N/A'} | Vol ${d.volume || '—'} Pg ${d.page || '—'} | Inst# ${d.instrument_number || 'N/A'}${d.consideration ? ` | $${d.consideration}` : ''}`
  ).join('\n');

  // Build expected assistant response (the training target)
  const runSheet = chain.documents.map((d, i) => ({
    sequence: i + 1,
    instrument_type: d.doc_type,
    instrument_number: d.instrument_number || null,
    recording_date: d.recording_date || null,
    grantor: d.grantor || null,
    grantee: d.grantee || null,
    consideration: d.consideration || null,
    interest_conveyed: d.notes?.includes('mineral')
      ? 'Mineral interest (fraction TBD)'
      : 'Fee simple (surface + minerals)',
    volume_page: d.volume || d.page ? `${d.volume || ''}/${d.page || ''}` : null,
  }));

  const gapAnalysis = chain.gaps.map(g => ({
    gate: g.gate,
    issue: g.description,
    severity: g.severity,
    recommended_action: gapToAction(g),
  }));

  const nextActions: string[] = chain.gaps.map(g => gapToAction(g));
  if (nextActions.length === 0) {
    nextActions.push('Run forward search from last grantee to verify no subsequent conveyances');
    nextActions.push('Verify current tax rolls match last grantee of record');
    nextActions.push('Check for any unreleased mineral leases or overriding royalty interests');
  }

  const gateStatus = {
    continuity: chain.gaps.some(g => g.gate === 'continuity' && g.severity === 'high')
      ? 'fail' : chain.gap_types.includes('continuity') ? 'needs_investigation' : 'pass',
    outconveyance: chain.gap_types.includes('outconveyance') ? 'needs_investigation' : 'pass',
    probate: chain.gaps.some(g => g.gate === 'probate' && g.severity === 'high')
      ? 'fail' : chain.gap_types.includes('probate') ? 'needs_investigation' : 'pass',
    severance: chain.gap_types.includes('severance') ? 'needs_investigation' : 'pass',
    liens: chain.gaps.some(g => g.gate === 'liens' && g.severity === 'high')
      ? 'fail' : chain.gap_types.includes('liens') ? 'needs_investigation' : 'pass',
  };

  const lastDoc = chain.documents[chain.documents.length - 1];
  const firstDoc = chain.documents[0];
  const nmi = chain.gap_types.includes('outconveyance')
    ? 'Requires fractional calculation — mineral/royalty conveyances detected'
    : '100% NMI (no mineral severance or outconveyance detected)';

  const assistantPayload = {
    run_sheet: runSheet,
    gaps: gapAnalysis,
    next_search_actions: nextActions,
    gate_status: gateStatus,
    net_mineral_interest: nmi,
    current_apparent_owner: lastDoc?.grantee || 'Unknown — chain incomplete',
    chain_summary: `${chain.documents.length} instrument(s) examined spanning ${firstDoc?.recording_date || 'unknown'} to ${lastDoc?.recording_date || 'unknown'}. ${chain.gaps.length} issue(s) identified across ${new Set(chain.gaps.map(g => g.gate)).size} gate(s). Chain quality: ${Math.round(chain.quality_score * 100)}%.`,
  };

  const userPrompt = `TRACT: ${tractDescription}

GRANTOR-GRANTEE INDEX:
${grantorIndex}

Analyze this chain of title. Apply the 5-gate framework. Identify all gaps, calculate net mineral interest, and recommend next search actions.`;

  const difficulty = chain.gap_types.length >= 3 ? 'hard'
    : chain.gap_types.length >= 1 ? 'moderate' : 'easy';

  return {
    messages: [
      { role: 'system', content: TITLEHOUND_SYSTEM_PROMPT },
      { role: 'user', content: userPrompt },
      { role: 'assistant', content: JSON.stringify(assistantPayload, null, 2) },
    ],
    metadata: {
      county: chain.tract.county,
      section: chain.tract.section,
      block: chain.tract.block,
      document_count: chain.documents.length,
      gap_types: chain.gap_types,
      quality_score: chain.quality_score,
      difficulty,
      is_interesting: chain.is_interesting,
    },
  };
}

// ─── Database Helpers ───────────────────────────────────────────────────────

let dbInitialized = false;

async function ensureDB(db: D1Database): Promise<void> {
  if (dbInitialized) return;
  for (const sql of SCHEMA_STATEMENTS) {
    try {
      await db.prepare(sql).run();
    } catch (e) {
      log("warn", "DB schema statement skipped (table/index may already exist)", { error: (e as Error)?.message || String(e) });
    }
  }
  dbInitialized = true;
}

async function createScrapeRun(db: D1Database, type: string, county: string | null): Promise<number> {
  const result = await db.prepare(
    'INSERT INTO scrape_runs (type, county) VALUES (?, ?) RETURNING id'
  ).bind(type, county).first('id');
  return result as number;
}

async function finishScrapeRun(
  db: D1Database, runId: number,
  tractsProcessed: number, docsFound: number, chainsBuilt: number, examplesGenerated: number,
): Promise<void> {
  await db.prepare(
    `UPDATE scrape_runs SET status = 'completed', tracts_processed = ?, docs_found = ?,
     chains_built = ?, examples_generated = ?, completed_at = datetime('now') WHERE id = ?`
  ).bind(tractsProcessed, docsFound, chainsBuilt, examplesGenerated, runId).run();
}

async function failScrapeRun(db: D1Database, runId: number): Promise<void> {
  await db.prepare(
    `UPDATE scrape_runs SET status = 'failed', completed_at = datetime('now') WHERE id = ?`
  ).bind(runId).run();
}

// ─── Scrape Orchestration ───────────────────────────────────────────────────

async function scrapeTract(env: Env, input: TractInput): Promise<any> {
  await ensureDB(env.DB);

  const { county, section, block, survey } = input;
  if (!county || !section) {
    return { error: 'county and section are required' };
  }

  // Query ShadowGlass for existing records matching this tract
  const records = await queryShadowGlassRecords(env, county, section, block, survey);
  if (records.length === 0) {
    return { success: false, error: 'No records found in ShadowGlass for this tract', county, section, block };
  }

  // Upsert tract
  const existing = await env.DB.prepare(
    'SELECT id, status FROM tracts WHERE county = ? AND section = ? AND block = ? AND survey = ?'
  ).bind(county, section || '', block || '', survey || '').first();

  let tractId: number;
  if (existing) {
    tractId = existing.id as number;
    await env.DB.prepare(
      `UPDATE tracts SET document_count = ?, last_scraped = datetime('now'), status = 'scraped' WHERE id = ?`
    ).bind(records.length, tractId).run();
  } else {
    const inserted = await env.DB.prepare(
      `INSERT INTO tracts (county, state, section, block, survey, abstract_number, document_count, status, last_scraped)
       VALUES (?, 'TX', ?, ?, ?, ?, ?, 'scraped', datetime('now')) RETURNING id`
    ).bind(county, section || '', block || '', survey || '', '', records.length).first('id');
    tractId = inserted as number;
  }

  // Store documents (skip duplicates by instrument_number)
  let docsInserted = 0;
  for (const r of records) {
    const doc = parseRecord(r);
    try {
      await env.DB.prepare(
        `INSERT OR IGNORE INTO documents
         (tract_id, doc_type, volume, page, date_filed, date_executed, grantor, grantee,
          legal_description, instrument_number, consideration, acreage, notes, raw_data)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).bind(
        tractId, doc.doc_type, doc.volume, doc.page,
        doc.recording_date, doc.recording_date,
        doc.grantor, doc.grantee, doc.legal_description,
        doc.instrument_number, doc.consideration, doc.acreage,
        doc.notes, JSON.stringify(r).slice(0, 8000)
      ).run();
      docsInserted++;
    } catch (e) {
      log("warn", "Document insert skipped (duplicate or constraint violation)", { error: (e as Error)?.message || String(e) });
    }
  }

  // Assemble chain from records
  const chain = assembleChain(records, input);

  // Determine chain type
  const chainType = chain.chain_complete ? 'full' : chain.gaps.length > 0 ? 'partial' : 'gap';

  // Store chain
  const chainRow = await env.DB.prepare(
    `INSERT INTO chains (tract_id, chain_type, documents, gaps, score, notes, validated)
     VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id`
  ).bind(
    tractId,
    chainType,
    JSON.stringify(chain.documents.slice(0, 100)),
    JSON.stringify(chain.gaps),
    chain.quality_score,
    `${chain.documents.length} docs, ${chain.gaps.length} gaps, ${chain.gap_types.join(',')}`,
    chain.chain_complete ? 1 : 0
  ).first('id');

  // Update tract status
  const newStatus = chain.chain_complete ? 'complete' : 'scraped';
  await env.DB.prepare(
    'UPDATE tracts SET status = ?, chain_complete = ? WHERE id = ?'
  ).bind(newStatus, chain.chain_complete ? 1 : 0, tractId).run();

  return {
    success: true,
    tract_id: tractId,
    chain_id: chainRow,
    county,
    section,
    block: block || '',
    documents_found: records.length,
    documents_inserted: docsInserted,
    chain_complete: chain.chain_complete,
    chain_type: chainType,
    quality_score: chain.quality_score,
    gaps: chain.gaps,
    gap_types: chain.gap_types,
    is_interesting: chain.is_interesting,
  };
}

async function scrapeBatch(env: Env, tracts: TractInput[]): Promise<any> {
  await ensureDB(env.DB);
  const runId = await createScrapeRun(env.DB, 'batch', tracts[0]?.county || null);
  const results: any[] = [];
  let totalDocs = 0;
  let chainsBuilt = 0;

  for (const tract of tracts) {
    try {
      const result = await scrapeTract(env, tract);
      results.push(result);
      if (result.success) {
        totalDocs += result.documents_found || 0;
        chainsBuilt++;
      }
      // Rate limit between tracts
      await new Promise(r => setTimeout(r, 1000));
    } catch (err: any) {
      results.push({ success: false, error: err.message, ...tract });
    }
  }

  const successful = results.filter(r => r.success).length;
  await finishScrapeRun(env.DB, runId, results.length, totalDocs, chainsBuilt, 0);

  return {
    run_id: runId,
    total: results.length,
    successful,
    failed: results.length - successful,
    total_documents: totalDocs,
    chains_built: chainsBuilt,
    results,
  };
}

async function scrapeCounty(env: Env, county: string, limit: number = 50): Promise<any> {
  await ensureDB(env.DB);

  // Get tract distribution from ShadowGlass
  const tractMap = await getCountyTractDistribution(env, county);
  if (tractMap.size === 0) {
    return { error: `No records found in ShadowGlass for ${county} County` };
  }

  // Sort tracts by document count (most documents first — most likely complete chains)
  const sortedTracts = [...tractMap.entries()]
    .sort((a, b) => b[1].length - a[1].length)
    .slice(0, limit);

  // Filter out tracts we already scraped
  const tractsToScrape: TractInput[] = [];
  for (const [key, recs] of sortedTracts) {
    const [section, block] = key.split('|');
    const existing = await env.DB.prepare(
      `SELECT id FROM tracts WHERE county = ? AND section = ? AND block = ? AND status IN ('complete', 'scraped')`
    ).bind(county, section, block).first();
    if (!existing) {
      tractsToScrape.push({ county, section, block, survey: recs[0]?.survey || '' });
    }
  }

  if (tractsToScrape.length === 0) {
    return { message: `All top-${limit} tracts in ${county} County already scraped`, total_tracts_available: sortedTracts.length };
  }

  return scrapeBatch(env, tractsToScrape);
}

// ─── Training Data Generation & Export ──────────────────────────────────────

async function generateTrainingFromChains(
  env: Env, options: { county?: string; limit?: number; min_quality?: number }
): Promise<any> {
  await ensureDB(env.DB);

  const minQuality = options.min_quality ?? 0.3;
  const limit = options.limit ?? 100;
  const params: any[] = [minQuality];
  let whereClause = 'WHERE c.score >= ? AND c.validated = 0';

  if (options.county) {
    whereClause += ' AND t.county = ?';
    params.push(options.county);
  }

  params.push(limit);

  const chains = await env.DB.prepare(
    `SELECT c.id as chain_id, c.tract_id, c.chain_type, c.score, c.gaps as chain_gaps,
            t.county, t.section, t.block, t.survey
     FROM chains c
     JOIN tracts t ON c.tract_id = t.id
     ${whereClause}
     ORDER BY c.score DESC
     LIMIT ?`
  ).bind(...params).all();

  const generated: any[] = [];
  let skipped = 0;

  for (const row of chains.results) {
    // Get documents for this tract
    const docs = await env.DB.prepare(
      'SELECT * FROM documents WHERE tract_id = ? ORDER BY date_filed ASC, id ASC'
    ).bind(row.tract_id).all();

    if (docs.results.length < 3) {
      skipped++;
      continue;
    }

    // Re-assemble chain from stored documents
    const chain = assembleChain(docs.results, {
      county: row.county as string,
      section: row.section as string,
      block: row.block as string,
      survey: row.survey as string,
    });

    const example = buildTrainingExample(chain);

    // Estimate token count (~4 chars per token)
    const totalChars = example.messages.reduce((s, m) => s + m.content.length, 0);
    const tokenCount = Math.ceil(totalChars / 4);

    // Skip if too short to be useful
    if (tokenCount < 200) {
      skipped++;
      continue;
    }

    try {
      await env.DB.prepare(
        `INSERT INTO training_examples
         (chain_id, tract_id, system_prompt, user_prompt, assistant_response, format, quality_score, gap_types, exported)
         VALUES (?, ?, ?, ?, ?, 'chatml', ?, ?, 0)`
      ).bind(
        row.chain_id, row.tract_id,
        example.messages[0].content,
        example.messages[1].content,
        example.messages[2].content,
        chain.quality_score,
        JSON.stringify(chain.gap_types)
      ).run();

      // Mark chain as validated (used for training)
      await env.DB.prepare('UPDATE chains SET validated = 1 WHERE id = ?')
        .bind(row.chain_id).run();

      generated.push({
        chain_id: row.chain_id,
        county: row.county,
        section: row.section,
        block: row.block,
        documents: docs.results.length,
        quality: chain.quality_score,
        difficulty: example.metadata.difficulty,
        gap_types: chain.gap_types,
        tokens: tokenCount,
      });
    } catch (e) {
      log("warn", "Training example insert skipped (duplicate or constraint error)", { error: (e as Error)?.message || String(e) });
    }
  }

  return {
    generated: generated.length,
    skipped,
    chains_evaluated: chains.results.length,
    examples: generated,
  };
}

async function exportTrainingJSONL(env: Env): Promise<Response> {
  await ensureDB(env.DB);

  const rows = await env.DB.prepare(
    `SELECT system_prompt, user_prompt, assistant_response, quality_score
     FROM training_examples
     ORDER BY quality_score DESC`
  ).all();

  if (rows.results.length === 0) {
    return new Response(JSON.stringify({ error: 'No training examples generated yet' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }

  const lines = rows.results.map(row => {
    const entry = {
      messages: [
        { role: 'system', content: row.system_prompt as string },
        { role: 'user', content: row.user_prompt as string },
        { role: 'assistant', content: row.assistant_response as string },
      ],
    };
    return JSON.stringify(entry);
  });

  const jsonl = lines.join('\n');

  // Store to R2
  const dateStamp = new Date().toISOString().split('T')[0];
  const r2Key = `training/titlehound_scraped_${dateStamp}.jsonl`;
  await env.RECORDS.put(r2Key, jsonl);

  // Mark all as exported
  await env.DB.prepare('UPDATE training_examples SET exported = 1 WHERE exported = 0').run();

  return new Response(jsonl, {
    headers: {
      'Content-Type': 'application/x-ndjson',
      'Content-Disposition': `attachment; filename="titlehound_scraped_${dateStamp}.jsonl"`,
      'Access-Control-Allow-Origin': '*',
      'X-R2-Path': r2Key,
      'X-Total-Examples': String(rows.results.length),
    },
  });
}

async function getTrainingStats(env: Env): Promise<any> {
  await ensureDB(env.DB);

  const [total, byCounty, byGapType, byQuality, exportStats] = await Promise.all([
    env.DB.prepare('SELECT COUNT(*) as c FROM training_examples').first('c'),
    env.DB.prepare(
      `SELECT t.county, COUNT(*) as count
       FROM training_examples te
       JOIN tracts t ON te.tract_id = t.id
       GROUP BY t.county ORDER BY count DESC`
    ).all(),
    env.DB.prepare(
      'SELECT gap_types, COUNT(*) as count FROM training_examples GROUP BY gap_types ORDER BY count DESC LIMIT 20'
    ).all(),
    env.DB.prepare(
      `SELECT
         SUM(CASE WHEN quality_score >= 0.8 THEN 1 ELSE 0 END) as high_quality,
         SUM(CASE WHEN quality_score >= 0.5 AND quality_score < 0.8 THEN 1 ELSE 0 END) as medium_quality,
         SUM(CASE WHEN quality_score < 0.5 THEN 1 ELSE 0 END) as low_quality,
         AVG(quality_score) as avg_quality
       FROM training_examples`
    ).first(),
    env.DB.prepare(
      'SELECT COUNT(*) as exported FROM training_examples WHERE exported = 1'
    ).first('exported'),
  ]);

  const totalCount = (total as number) || 0;

  return {
    total_examples: totalCount,
    target: 5000,
    progress_pct: Math.round((totalCount / 5000) * 1000) / 10,
    by_county: byCounty.results,
    by_gap_type: byGapType.results,
    quality_distribution: {
      high: (exportStats as any)?.high_quality || 0,
      medium: (exportStats as any)?.medium_quality || 0,
      low: (exportStats as any)?.low_quality || 0,
      average: Math.round(((byQuality as any)?.avg_quality || 0) * 100) / 100,
    },
    exported: exportStats || 0,
    counties_covered: byCounty.results.length,
    gap_types_represented: byGapType.results.length,
  };
}

// ─── Router ─────────────────────────────────────────────────────────────────

type HonoEnv = { Bindings: Env };
const app = new Hono<HonoEnv>();

// ── Global Error Handler ──
app.onError((err, c) => {
  log('error', 'Unhandled error', { error: err.message, path: c.req.path, method: c.req.method });
  if (err instanceof SyntaxError && err.message.includes('JSON')) {
    return c.json({ error: true, message: 'Invalid JSON in request body' }, 400);
  }
  return c.json({ error: true, message: 'Internal server error' }, 500);
});

// CORS
app.use('*', cors({
  origin: '*',
  allowMethods: ['GET', 'POST', 'OPTIONS'],
  allowHeaders: ['Content-Type', 'X-Echo-API-Key'],
}));

// ── Health (no auth) ──
app.get("/", (c) => c.json({ service: 'echo-title-scraper', status: 'operational' }));

app.get('/health', async (c) => {
  const env = c.env;
  await ensureDB(env.DB);

  const [tractCount, chainCount, exampleCount, lastRun] = await Promise.all([
    env.DB.prepare('SELECT COUNT(*) as c FROM tracts').first('c'),
    env.DB.prepare('SELECT COUNT(*) as c FROM chains').first('c'),
    env.DB.prepare('SELECT COUNT(*) as c FROM training_examples').first('c'),
    env.DB.prepare('SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 1').first(),
  ]);

  return c.json({
    status: 'healthy',
    version: env.WORKER_VERSION,
    environment: env.ENVIRONMENT,
    stats: {
      tracts: tractCount || 0,
      chains: chainCount || 0,
      training_examples: exampleCount || 0,
      target: 5000,
      progress_pct: Math.round((((exampleCount as number) || 0) / 5000) * 1000) / 10,
    },
    last_run: lastRun || null,
    priority_counties: PERMIAN_COUNTIES,
    timestamp: new Date().toISOString(),
  });
});

// ── Init (create tables) ──
app.get('/init', async (c) => {
  const env = c.env;
  const results: string[] = [];

  for (const sql of SCHEMA_STATEMENTS) {
    try {
      await env.DB.prepare(sql).run();
      results.push(`OK: ${sql.slice(0, 60)}...`);
    } catch (err: any) {
      results.push(`SKIP: ${sql.slice(0, 60)}... (${err.message})`);
    }
  }

  dbInitialized = true;
  return c.json({ initialized: true, statements: results.length, details: results });
});

// ── Auth middleware for all other routes ──
app.use('/*', async (c, next) => {
  const path = c.req.path;
  if (path === '/health' || path === '/init') return next();

  const key = c.req.header('X-Echo-API-Key');
  if (!key || key !== c.env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized — X-Echo-API-Key required' }, 401);
  }
  await next();
});

// ── POST /scrape/tract ──
app.post('/scrape/tract', async (c) => {
  try {
    const body = await c.req.json<{ county: string; section: string; block?: string; survey?: string }>();
    if (!body.county || !body.section) {
      return c.json({ error: 'county and section are required' }, 400);
    }
    const result = await scrapeTract(c.env, body);
    return c.json(result, result.error ? 400 : 200);
  } catch (err: any) {
    return c.json({ error: err.message, stack: err.stack?.split('\n').slice(0, 5) }, 500);
  }
});

// ── POST /scrape/batch ──
app.post('/scrape/batch', async (c) => {
  try {
    const body = await c.req.json<{ tracts: TractInput[] }>();
    if (!body.tracts || body.tracts.length === 0) {
      return c.json({ error: 'tracts array is required and must not be empty' }, 400);
    }
    if (body.tracts.length > 200) {
      return c.json({ error: 'Maximum 200 tracts per batch' }, 400);
    }
    const result = await scrapeBatch(c.env, body.tracts);
    return c.json(result);
  } catch (err: any) {
    return c.json({ error: err.message, stack: err.stack?.split('\n').slice(0, 5) }, 500);
  }
});

// ── POST /scrape/county ──
app.post('/scrape/county', async (c) => {
  try {
    const body = await c.req.json<{ county: string; limit?: number }>();
    if (!body.county) {
      return c.json({ error: 'county is required' }, 400);
    }
    const limit = Math.min(body.limit || 50, 100);
    const result = await scrapeCounty(c.env, body.county, limit);
    return c.json(result);
  } catch (err: any) {
    return c.json({ error: err.message, stack: err.stack?.split('\n').slice(0, 5) }, 500);
  }
});

// ── GET /chains ──
app.get('/chains', async (c) => {
  const env = c.env;
  await ensureDB(env.DB);

  const county = c.req.query('county');
  const minDocs = parseInt(c.req.query('min_docs') || '0');
  const chainType = c.req.query('chain_type');
  const limit = Math.min(parseInt(c.req.query('limit') || '50'), 500);

  let sql = `SELECT c.id, c.tract_id, c.chain_type, c.score, c.notes, c.validated, c.created_at,
             t.county, t.section, t.block, t.survey, t.document_count
             FROM chains c JOIN tracts t ON c.tract_id = t.id WHERE 1=1`;
  const params: any[] = [];

  if (county) {
    sql += ' AND t.county = ?';
    params.push(county);
  }
  if (minDocs > 0) {
    sql += ' AND t.document_count >= ?';
    params.push(minDocs);
  }
  if (chainType) {
    sql += ' AND c.chain_type = ?';
    params.push(chainType);
  }

  sql += ' ORDER BY c.score DESC LIMIT ?';
  params.push(limit);

  const results = await env.DB.prepare(sql).bind(...params).all();
  return c.json({
    chains: results.results,
    count: results.results.length,
    filters: { county, min_docs: minDocs, chain_type: chainType },
  });
});

// ── GET /chains/:id ──
app.get('/chains/:id', async (c) => {
  const env = c.env;
  await ensureDB(env.DB);
  const chainId = c.req.param('id');

  const chain = await env.DB.prepare(
    `SELECT c.*, t.county, t.section, t.block, t.survey, t.abstract_number, t.document_count, t.status as tract_status
     FROM chains c JOIN tracts t ON c.tract_id = t.id WHERE c.id = ?`
  ).bind(chainId).first();

  if (!chain) return c.json({ error: 'Chain not found' }, 404);

  const docs = await env.DB.prepare(
    'SELECT * FROM documents WHERE tract_id = ? ORDER BY date_filed ASC, id ASC'
  ).bind(chain.tract_id).all();

  const training = await env.DB.prepare(
    'SELECT id, quality_score, gap_types, exported, created_at FROM training_examples WHERE chain_id = ?'
  ).bind(chainId).all();

  // Parse stored JSON fields
  let parsedDocs: any[] = [];
  let parsedGaps: any[] = [];
  try { parsedDocs = JSON.parse(chain.documents as string); } catch (e) { log("warn", "Failed to parse stored chain documents JSON", { error: (e as Error)?.message || String(e) }); }
  try { parsedGaps = JSON.parse(chain.gaps as string); } catch (e) { log("warn", "Failed to parse stored chain gaps JSON", { error: (e as Error)?.message || String(e) }); }

  return c.json({
    chain: {
      ...chain,
      documents_parsed: parsedDocs,
      gaps_parsed: parsedGaps,
    },
    documents: docs.results,
    training_examples: training.results,
  });
});

// ── POST /training/generate ──
app.post('/training/generate', async (c) => {
  const body = await c.req.json<{ county?: string; limit?: number; min_quality?: number }>();
  const result = await generateTrainingFromChains(c.env, {
    county: body.county,
    limit: body.limit || 100,
    min_quality: body.min_quality ?? 0.3,
  });
  return c.json(result);
});

// ── GET /training/export ──
app.get('/training/export', async (c) => {
  return exportTrainingJSONL(c.env);
});

// ── GET /training/stats ──
app.get('/training/stats', async (c) => {
  const stats = await getTrainingStats(c.env);
  return c.json(stats);
});

// ── GET /status ──
app.get('/status', async (c) => {
  const env = c.env;
  await ensureDB(env.DB);

  const [runs, tractsByStatus, countyCoverage, stats] = await Promise.all([
    env.DB.prepare('SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 10').all(),
    env.DB.prepare('SELECT status, COUNT(*) as count FROM tracts GROUP BY status').all(),
    env.DB.prepare(
      'SELECT county, COUNT(*) as tracts, SUM(document_count) as docs FROM tracts GROUP BY county ORDER BY tracts DESC'
    ).all(),
    getTrainingStats(env),
  ]);

  return c.json({
    recent_runs: runs.results,
    tracts_by_status: tractsByStatus.results,
    county_coverage: countyCoverage.results,
    training_stats: stats,
    priority_counties: PERMIAN_COUNTIES,
  });
});

// ── GET /dashboard ──
app.get('/dashboard', async (c) => {
  const env = c.env;
  await ensureDB(env.DB);

  const [
    totalTracts, totalDocs, totalChains, totalExamples,
    completeChains, interestingChains,
    recentRuns, countyCoverage, qualityDist, topGapTypes,
  ] = await Promise.all([
    env.DB.prepare('SELECT COUNT(*) as c FROM tracts').first('c'),
    env.DB.prepare('SELECT COUNT(*) as c FROM documents').first('c'),
    env.DB.prepare('SELECT COUNT(*) as c FROM chains').first('c'),
    env.DB.prepare('SELECT COUNT(*) as c FROM training_examples').first('c'),
    env.DB.prepare("SELECT COUNT(*) as c FROM chains WHERE chain_type = 'full'").first('c'),
    env.DB.prepare('SELECT COUNT(*) as c FROM chains WHERE score >= 0.7').first('c'),
    env.DB.prepare('SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 5').all(),
    env.DB.prepare(
      `SELECT t.county,
              COUNT(DISTINCT t.id) as tracts,
              COUNT(DISTINCT d.id) as documents,
              COUNT(DISTINCT c.id) as chains,
              COUNT(DISTINCT te.id) as examples,
              AVG(c.score) as avg_quality
       FROM tracts t
       LEFT JOIN documents d ON d.tract_id = t.id
       LEFT JOIN chains c ON c.tract_id = t.id
       LEFT JOIN training_examples te ON te.tract_id = t.id
       GROUP BY t.county
       ORDER BY tracts DESC`
    ).all(),
    env.DB.prepare(
      `SELECT
         SUM(CASE WHEN score >= 0.8 THEN 1 ELSE 0 END) as excellent,
         SUM(CASE WHEN score >= 0.6 AND score < 0.8 THEN 1 ELSE 0 END) as good,
         SUM(CASE WHEN score >= 0.4 AND score < 0.6 THEN 1 ELSE 0 END) as fair,
         SUM(CASE WHEN score < 0.4 THEN 1 ELSE 0 END) as poor
       FROM chains`
    ).first(),
    env.DB.prepare(
      'SELECT gap_types, COUNT(*) as c FROM training_examples GROUP BY gap_types ORDER BY c DESC LIMIT 10'
    ).all(),
  ]);

  const totalEx = (totalExamples as number) || 0;

  return c.json({
    overview: {
      total_tracts: totalTracts || 0,
      total_documents: totalDocs || 0,
      total_chains: totalChains || 0,
      total_training_examples: totalEx,
      complete_chains: completeChains || 0,
      high_quality_chains: interestingChains || 0,
      target_examples: 5000,
      progress_pct: Math.round((totalEx / 5000) * 1000) / 10,
    },
    quality_distribution: qualityDist || { excellent: 0, good: 0, fair: 0, poor: 0 },
    county_breakdown: countyCoverage.results,
    top_gap_types: topGapTypes.results,
    recent_runs: recentRuns.results,
    priority_counties: PERMIAN_COUNTIES,
    timestamp: new Date().toISOString(),
  });
});

// ── 404 fallback ──
app.all('*', (c) => {
  return c.json({
    error: 'Not found',
    endpoints: [
      'GET  /health',
      'GET  /init',
      'POST /scrape/tract    — {county, section, block?, survey?}',
      'POST /scrape/batch    — {tracts: [{county, section, block?, survey?}, ...]}',
      'POST /scrape/county   — {county, limit?}',
      'GET  /chains          — ?county=&min_docs=&chain_type=&limit=',
      'GET  /chains/:id',
      'POST /training/generate — {county?, limit?, min_quality?}',
      'GET  /training/export',
      'GET  /training/stats',
      'GET  /status',
      'GET  /dashboard',
    ],
  }, 404);
});

// ─── Cron Handler ───────────────────────────────────────────────────────────

async function handleScheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
  await ensureDB(env.DB);

  // Determine which county to scrape tonight (rotate through priority list)
  const dayOfYear = Math.floor(
    (Date.now() - new Date(new Date().getFullYear(), 0, 0).getTime()) / 86400000
  );
  const countyIndex = dayOfYear % PERMIAN_COUNTIES.length;
  const county = PERMIAN_COUNTIES[countyIndex];

  const runId = await createScrapeRun(env.DB, 'cron_nightly', county);
  let tractsProcessed = 0;
  let docsFound = 0;
  let chainsBuilt = 0;
  let examplesGenerated = 0;

  try {
    // Phase 1: Scrape up to 50 new tracts in tonight's county
    const scrapeResult = await scrapeCounty(env, county, 50);
    if (scrapeResult.results) {
      tractsProcessed = scrapeResult.total || 0;
      docsFound = scrapeResult.total_documents || 0;
      chainsBuilt = scrapeResult.chains_built || 0;
    }

    // Phase 2: Generate training examples from chains with quality >= 0.3
    const trainingResult = await generateTrainingFromChains(env, {
      county,
      limit: 50,
      min_quality: 0.3,
    });
    examplesGenerated = trainingResult.generated || 0;

    // Phase 3: Export updated JSONL to R2
    if (examplesGenerated > 0) {
      const allExamples = await env.DB.prepare(
        'SELECT system_prompt, user_prompt, assistant_response FROM training_examples ORDER BY quality_score DESC'
      ).all();

      const jsonl = allExamples.results.map(row => JSON.stringify({
        messages: [
          { role: 'system', content: row.system_prompt },
          { role: 'user', content: row.user_prompt },
          { role: 'assistant', content: row.assistant_response },
        ],
      })).join('\n');

      const dateStamp = new Date().toISOString().split('T')[0];
      await env.RECORDS.put(`training/titlehound_scraped_${dateStamp}.jsonl`, jsonl);
      await env.RECORDS.put('training/titlehound_scraped_latest.jsonl', jsonl);
    }

    // Phase 4: Report progress to Shared Brain
    try {
      const totalExamples = await env.DB.prepare(
        'SELECT COUNT(*) as c FROM training_examples'
      ).first('c');

      await env.BRAIN.fetch('https://brain/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          instance_id: 'echo-title-scraper',
          role: 'assistant',
          content: `TITLE SCRAPER NIGHTLY: ${county} County — ${tractsProcessed} tracts, ${docsFound} docs, ${chainsBuilt} chains, ${examplesGenerated} new training examples. Total: ${totalExamples || 0}/5000.`,
          importance: 6,
          tags: ['titlehound', 'training', 'nightly', county.toLowerCase()],
        }),
      });
    } catch (e) {
      log("warn", "Brain ingest unavailable for nightly report (non-critical)", { error: (e as Error)?.message || String(e) });
    }

    await finishScrapeRun(env.DB, runId, tractsProcessed, docsFound, chainsBuilt, examplesGenerated);
  } catch (err: any) {
    await failScrapeRun(env.DB, runId);

    // Report failure to Brain
    try {
      await env.BRAIN.fetch('https://brain/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          instance_id: 'echo-title-scraper',
          role: 'assistant',
          content: `TITLE SCRAPER ERROR: Nightly cron failed for ${county} County — ${err.message}`,
          importance: 8,
          tags: ['titlehound', 'error', 'nightly'],
        }),
      });
    } catch (e) {
      log("warn", "Brain ingest unavailable for error report (best effort)", { error: (e as Error)?.message || String(e) });
    }
  }
}

// ─── Export ─────────────────────────────────────────────────────────────────

export default {
  fetch: app.fetch,
  scheduled: handleScheduled,
};
